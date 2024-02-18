import dataclasses
import pathlib
import re
import typing

from .. import _cast
from .. import _parsing
from . import _markdown

if typing.TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    import polars as pl

    DF_TYPE = typing.TypeVar("DF_TYPE", "pl.DataFrame", "pd.DataFrame")
else:
    _DF_TYPE_VALUES = []
    try:
        import pandas as pd

        _DF_TYPE_VALUES.append("pd.DataFrame")
    except ImportError:  # pragma: no cover
        pd = None  # type: ignore

    try:
        import polars as pl

        _DF_TYPE_VALUES.append("pl.DataFrame")
    except ImportError:  # pragma: no cover
        pl = None  # type: ignore

    DF_TYPE = typing.TypeVar("DF_TYPE", *_DF_TYPE_VALUES)


_DATA_FRAME_SEPARATOR_REGEX = re.compile(
    r"(^|\n)\s*-{3,}\s*(?P<name>[^\s-]*)\s*-{0,}\n+"
)


@dataclasses.dataclass(frozen=True)
class LoadedDataFrame(typing.Generic[DF_TYPE]):
    """A loaded DataFrame within a collection of loaded DataFrames."""

    index: int
    name: str
    sourced_name: typing.Optional[str]
    data_frame: DF_TYPE

    @property
    def frame(self) -> DF_TYPE:
        """Get the DataFrame specified in this item."""
        return self.data_frame


class LoadedDataFrames(typing.Generic[DF_TYPE]):
    """A collection of loaded DataFrames."""

    def __init__(
        self,
        data_frames: typing.Dict[str, DF_TYPE],
        sourced_names: typing.List[typing.Optional[str]],
    ) -> None:
        """Construct a LoadedDataFrames instance from parsed values."""
        self._frames: typing.Dict[str, DF_TYPE] = data_frames
        self._sourced_names = sourced_names

    @property
    def frame_names(self) -> typing.Tuple[str, ...]:
        """Get the ordered names of the loaded DataFrames."""
        return tuple(self._frames.keys())

    @property
    def sourced_frame_names(self) -> typing.Tuple[typing.Optional[str], ...]:
        """Get the names for the frames as specified from the source file."""
        return tuple(self._sourced_names)

    def to_dict(self) -> typing.Dict[str, DF_TYPE]:
        """Convert to a dictionary representation of the loaded DataFrames."""
        return self._frames.copy()

    def to_tuple(self) -> typing.Tuple[DF_TYPE, ...]:
        """Convert to a tuple representation of the loaded DataFrames."""
        return tuple(self._frames.values())

    def __len__(self) -> int:
        """Get the number of loaded DataFrames."""
        return len(self.frame_names)

    def __getattr__(self, name: str) -> DF_TYPE:
        """Get the DataFrame using dot syntax, or other attribute otherwise."""
        if name in self._frames:
            return self._frames[name]
        raise AttributeError(f"No loaded DataFrame named '{name}' was found.")

    def __dir__(self) -> typing.Iterable[str]:
        """Enumerate the available attributes on the object."""
        return tuple(list(super().__dir__()) + list(self.frame_names))

    def __getitem__(self, name_or_index: typing.Union[str, int]) -> DF_TYPE:
        """Get the DataFrame by dictionary-style, key-based access."""
        if isinstance(name_or_index, int):
            return list(self._frames.values())[name_or_index]

        if name_or_index not in self._frames:
            raise KeyError(f"No loaded DataFrame named '{name_or_index}' was found.")
        return self._frames[name_or_index]

    def __iter__(self) -> typing.Iterator[LoadedDataFrame[DF_TYPE]]:
        """Iterate over loaded DataFrames."""
        for index, key in enumerate(self._frames.keys()):
            yield LoadedDataFrame(
                index=index,
                name=key,
                sourced_name=self._sourced_names[index],
                data_frame=self._frames[key],
            )

    def __reversed__(self) -> typing.Iterator[LoadedDataFrame[DF_TYPE]]:
        """Iterate over loaded DataFrames in reverse order."""
        for index, key in reversed(list(enumerate(self._frames.keys()))):
            yield LoadedDataFrame(
                index=index,
                name=key,
                sourced_name=self._sourced_names[index],
                data_frame=self._frames[key],
            )

    def __contains__(self, name: str) -> bool:
        """Whether the frame with the specified name is in the loaded DataFrames."""
        return name in self._frames


def _to_pandas(columns: typing.List["_parsing.RawColumn"]):
    if pd is None:
        raise RuntimeError("No pandas module was found.")

    if not columns:
        return pd.DataFrame([])

    indexes: typing.Union[None, pd.Series, typing.List[pd.Series]]
    indexes = [
        pd.Series(
            (values := c.to_values()),
            name=_cast.cast_to(c.name, c.modifiers.name_data_type or "str"),
            dtype=_cast.to_pandas_dtype(  # type: ignore
                c.data_type or "object",
                values,
            ),
        )
        for c in columns
        if c.modifiers.index
    ]
    if len(indexes) == 1:
        indexes = indexes[0]
    elif len(indexes) == 0:
        indexes = None

    series: typing.Dict[typing.Any, pd.Series] = {}
    for column in columns:
        if column.modifiers.index:
            continue

        name = _cast.cast_to(column.name, column.modifiers.name_data_type or "str")
        values = column.to_values()
        dtype = _cast.to_pandas_dtype(
            column.data_type or "object",
            values,
        )
        series[name] = pd.Series(
            values,
            name=name,
            dtype=dtype,  # type: ignore
            index=indexes,
        )

    return pd.DataFrame(series)


def _to_polars(columns: typing.List["_parsing.RawColumn"]):
    if pl is None:
        raise RuntimeError("No polars module was found.")

    if not columns:
        return pl.DataFrame([])

    series: typing.List[pl.Series] = []
    for column in columns:
        values = column.to_values()
        series.append(
            pl.Series(
                column.name,
                values,
                dtype=_cast.to_polars_dtype(column.data_type, values),
            )
        )

    return pl.DataFrame(series)


@typing.overload
def reads(
    table: str,
    kind: typing.Literal["pandas"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
) -> "pd.DataFrame":
    """Read dftxt string into a Pandas DataFrame."""
    ...


@typing.overload
def reads(
    table: str,
    kind: typing.Literal["polars"],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
) -> "pl.DataFrame":
    """Read dftxt string into a Polars DataFrame."""
    ...


def reads(
    table: str,
    kind: typing.Literal["pandas", "polars"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
):
    """Read dftxt string into a Pandas or Polars DataFrame."""
    if markdown:
        source_text = _markdown.extract(table)
    else:
        source_text = table

    blocks = _parsing.read_blocks(
        source_text.replace("\r", "").replace("\t", "  ").split("\n"),
        modifier_prefix=modifier_prefix,
    )
    distinct_filters = set(filters or [])
    raw_columns = [
        column
        for block in blocks
        for column in block.columns
        if not column.should_skip(distinct_filters)
    ]

    if kind == "pandas":
        return _to_pandas(raw_columns)
    return _to_polars(raw_columns)


@typing.overload
def reads_all(
    tables: str,
    kind: typing.Literal["pandas"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
) -> LoadedDataFrames["pd.DataFrame"]:
    """Read dftxt string into a tuple of Pandas DataFrames."""
    ...


@typing.overload
def reads_all(
    tables: str,
    kind: typing.Literal["polars"],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
) -> LoadedDataFrames["pl.DataFrame"]:
    """Read dftxt string into a tuple of Polars DataFrames."""
    ...


def reads_all(
    tables: str,
    kind: typing.Literal["pandas", "polars"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
):
    """Read dftxt string into a tuple of Pandas or Polars DataFrames."""
    if markdown:
        source_text = _markdown.extract(tables)
    else:
        source_text = tables
    offset = 0
    next_name = ""
    sourced_names: typing.List[typing.Optional[str]] = []
    data_frames: typing.Dict[str, typing.Union["pl.DataFrame", "pd.DataFrame"]] = {}
    while offset < len(source_text):
        match = _DATA_FRAME_SEPARATOR_REGEX.search(source_text, pos=offset)
        start = offset
        end = len(source_text) if not match else match.start()
        offset = len(source_text) if not match else match.end()
        if offset == start:
            continue

        data_frame = reads(
            table=source_text[start:end],
            kind=kind,
            filters=filters,
            modifier_prefix=modifier_prefix,
            markdown=False,
        )
        sourced_name = next_name or None
        frame_name = next_name or f"data_frame_{len(data_frames) + 1}"
        next_name = match.group("name") if match else ""

        if len(data_frame.columns) > 0:
            sourced_names.append(sourced_name)
            data_frames[frame_name] = data_frame

    if kind == "pandas":
        return LoadedDataFrames["pd.DataFrame"](
            typing.cast(typing.Dict[str, "pd.DataFrame"], data_frames), sourced_names
        )
    return LoadedDataFrames["pl.DataFrame"](
        typing.cast(typing.Dict[str, "pl.DataFrame"], data_frames), sourced_names
    )


def reads_to_pandas(
    table: str,
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
) -> "pd.DataFrame":
    """Read the table to a Pandas DataFrame."""
    return typing.cast(
        pd.DataFrame,
        reads(
            table=table,
            kind="pandas",
            filters=filters,
            modifier_prefix=modifier_prefix,
            markdown=markdown,
        ),
    )


def reads_all_to_pandas(
    tables: str,
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
) -> LoadedDataFrames["pd.DataFrame"]:
    """Read the table to Pandas DataFrames."""
    return reads_all(
        tables=tables,
        kind="pandas",
        filters=filters,
        modifier_prefix=modifier_prefix,
        markdown=markdown,
    )


def reads_to_polars(
    table: str,
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
) -> "pl.DataFrame":
    """Read the table to a Polars DataFrame."""
    return typing.cast(
        pl.DataFrame,
        reads(
            table=table,
            kind="polars",
            filters=filters,
            modifier_prefix=modifier_prefix,
            markdown=markdown,
        ),
    )


def reads_all_to_polars(
    tables: str,
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    modifier_prefix: str = "&",
) -> LoadedDataFrames["pl.DataFrame"]:
    """Read the tables to Polars DataFrames."""
    return reads_all(
        tables=tables,
        kind="polars",
        filters=filters,
        modifier_prefix=modifier_prefix,
        markdown=markdown,
    )


@typing.overload
def read(
    path: typing.Union[pathlib.Path, str],
    kind: typing.Literal["pandas"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
) -> "pd.DataFrame":
    """Read dftxt file into a Pandas DataFrame."""
    ...


@typing.overload
def read(
    path: typing.Union[pathlib.Path, str],
    kind: typing.Literal["polars"],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
) -> "pl.DataFrame":
    """Read dftxt file into a Polars DataFrame."""
    ...


def read(
    path: typing.Union[pathlib.Path, str],
    kind: typing.Literal["pandas", "polars"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
):
    """Read dftxt file into a Pandas or Polars DataFrame."""
    source_path = pathlib.Path(path).expanduser().resolve()
    is_markdown = markdown or source_path.name.endswith(".md")
    return reads(
        table=source_path.read_text(encoding),
        kind=kind,
        filters=filters,
        modifier_prefix=modifier_prefix,
        markdown=is_markdown,
    )


@typing.overload
def read_all(
    path: typing.Union[pathlib.Path, str],
    kind: typing.Literal["pandas"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
) -> LoadedDataFrames["pd.DataFrame"]:
    """Read dftxt file into Pandas DataFrames."""
    ...


@typing.overload
def read_all(
    path: typing.Union[pathlib.Path, str],
    kind: typing.Literal["polars"],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
) -> LoadedDataFrames["pl.DataFrame"]:
    """Read dftxt file into Polars DataFrames."""
    ...


def read_all(
    path: typing.Union[pathlib.Path, str],
    kind: typing.Literal["pandas", "polars"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
):
    """Read dftxt file into Pandas or Polars DataFrames."""
    source_path = pathlib.Path(path).expanduser().resolve()
    is_markdown = markdown or source_path.name.endswith(".md")
    return reads_all(
        tables=source_path.read_text(encoding),
        kind=kind,
        filters=filters,
        modifier_prefix=modifier_prefix,
        markdown=is_markdown,
    )


def read_to_pandas(
    path: typing.Union[pathlib.Path, str],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
) -> "pd.DataFrame":
    """Read dftxt file into a Pandas DataFrame."""
    return typing.cast(
        pd.DataFrame,
        read(
            path=path,
            kind="pandas",
            filters=filters,
            modifier_prefix=modifier_prefix,
            encoding=encoding,
            markdown=markdown,
        ),
    )


def read_all_to_pandas(
    path: typing.Union[pathlib.Path, str],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
) -> LoadedDataFrames["pd.DataFrame"]:
    """Read dftxt file into Pandas DataFrames."""
    return read_all(
        path=path,
        kind="pandas",
        filters=filters,
        modifier_prefix=modifier_prefix,
        encoding=encoding,
        markdown=markdown,
    )


def read_to_polars(
    path: typing.Union[pathlib.Path, str],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
) -> "pl.DataFrame":
    """Read dftxt file into a Pandas DataFrame."""
    return typing.cast(
        pl.DataFrame,
        read(
            path=path,
            kind="polars",
            filters=filters,
            modifier_prefix=modifier_prefix,
            encoding=encoding,
            markdown=markdown,
        ),
    )


def read_all_to_polars(
    path: typing.Union[pathlib.Path, str],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    markdown: bool = False,
    encoding: str = "utf-8",
    modifier_prefix: str = "&",
) -> LoadedDataFrames["pl.DataFrame"]:
    """Read dftxt file into a Pandas DataFrame."""
    return read_all(
        path=path,
        kind="polars",
        filters=filters,
        modifier_prefix=modifier_prefix,
        encoding=encoding,
        markdown=markdown,
    )
