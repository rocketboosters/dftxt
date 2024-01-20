import dataclasses
import pathlib
import re
import typing

from . import _cast
from . import _modifiers

if typing.TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    import polars as pl
else:
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        pd = None  # type: ignore

    try:
        import polars as pl
    except ImportError:  # pragma: no cover
        pl = None  # type: ignore


@dataclasses.dataclass()
class ColumnBounds:
    """Data structure that represents fixed-width columns within the raw table."""

    start_index: int
    end_index: int

    def __repr__(self) -> str:
        """Simplified display representation for the ColumnBounds data."""
        return f"({self.start_index}, {self.end_index})"


@dataclasses.dataclass()
class RawColumn:
    """Data structure for source column data."""

    bounds: "ColumnBounds"
    name: str
    modifiers: "_modifiers.ColumnModifiers"
    cells: typing.List[typing.Optional[str]] = dataclasses.field(
        default_factory=lambda: []
    )

    @property
    def data_type(self) -> typing.Optional[str]:
        """Get the dftxt data type associated with the column."""
        return self.modifiers.data_type

    def to_values(self) -> typing.List[typing.Any]:
        """Cast cell data to values."""
        return [_cast.cast_to(v, self.modifiers.data_type or "str") for v in self.cells]

    def should_skip(self, filters: typing.Set[str]) -> bool:
        """Whether the column should be skipped when loaded."""
        return (
            # If the column is marked skip, it should never be included.
            self.modifiers.skip
            # If only filters have been set and none are satisfied.
            or (
                len(self.modifiers.only_filters) > 0
                and len(filters.union(self.modifiers.only_filters)) == 0
            )
            # If never filters have been set and at least one are satisfied.
            or (
                len(self.modifiers.never_filters) > 0
                and len(filters.union(self.modifiers.never_filters)) == 0
            )
        )


@dataclasses.dataclass()
class RawTableBlock:
    """Data structure for raw table blocks."""

    columns: typing.List["RawColumn"] = dataclasses.field(default_factory=lambda: [])


def _read_blocks(
    lines: typing.List[str], modifier_prefix: str = "&"
) -> typing.List["RawTableBlock"]:
    """Read table block data into its raw separated format for parsing."""
    blocks = []

    column_boundaries: typing.List[ColumnBounds] = []
    column_names: typing.List[str] = []
    column_modifiers: typing.List[typing.List[typing.Optional[str]]] = []
    column_data: typing.List[typing.List[typing.Optional[str]]] = []

    remaining_lines = lines.copy()
    while remaining_lines:
        raw = remaining_lines.pop(0)
        stripped = raw.strip()

        if len(stripped) > 2 and set(stripped) == {"-"}:
            # A row of nothing but three or more dashes starts a new block.
            columns = [
                RawColumn(
                    bounds=bounds,
                    name=name,
                    modifiers=_modifiers.parse(modifiers, modifier_prefix),
                    cells=cells,
                )
                for bounds, name, modifiers, cells in zip(
                    column_boundaries, column_names, column_modifiers, column_data
                )
            ]
            blocks.append(RawTableBlock(columns))
            column_boundaries = []
            column_names = []
            column_modifiers = []
            column_data = []
            continue

        if not stripped:
            # Ignore empty lines
            continue

        if stripped.startswith("#"):
            # Ignore comments during read.
            continue

        if not column_boundaries:
            column_boundaries = _find_boundaries(raw)
            column_modifiers = [[] for _ in range(len(column_boundaries))]
            column_data = [[] for _ in range(len(column_boundaries))]

        exploded, continuation = _explode_line(column_boundaries, raw)
        while continuation:
            exploded_continued, continuation = _explode_line(
                column_boundaries, remaining_lines.pop(0)
            )
            exploded = _combine_cells_across_lines(exploded, exploded_continued)

        if not column_names:
            column_names = [n or "" for n in exploded]
        elif stripped.startswith(modifier_prefix):
            column_modifiers = _append_columnwise(column_modifiers, exploded)
        else:
            column_data = _append_columnwise(column_data, exploded)

    columns = [
        RawColumn(
            bounds=bounds,
            name=name,
            modifiers=_modifiers.parse(modifiers, modifier_prefix),
            cells=cells,
        )
        for bounds, name, modifiers, cells in zip(
            column_boundaries, column_names, column_modifiers, column_data
        )
    ]
    blocks.append(RawTableBlock(columns))

    return blocks


def _append_columnwise(
    existing: typing.List[typing.List[typing.Optional[str]]],
    new_cells: typing.List[typing.Optional[str]],
) -> typing.List[typing.List[typing.Optional[str]]]:
    return [[*column, cell] for column, cell in zip(existing, new_cells)]


def _find_boundaries(first_header_line: str) -> typing.List["ColumnBounds"]:
    raw = first_header_line.rstrip()
    if not raw:
        return []

    regex = re.compile(r"\s{2,}")
    columns: typing.List["ColumnBounds"] = []
    position = 0
    while position < len(raw):
        match = regex.search(raw, pos=position)
        if match:
            next_position = match.end()
        else:
            next_position = 100_000

        columns.append(ColumnBounds(start_index=position, end_index=next_position))
        position = next_position

    return columns


def _extract_cell(
    bounds: "ColumnBounds", line: str
) -> typing.Tuple[typing.Optional[str], bool]:
    start = bounds.start_index

    # Row might not have a value specified for a hanging column. In those cases return
    # no value.
    if len(line) <= start:
        return None, False

    # Adjust the start for minor negative alignments.
    while start > 0 and line[start - 1] != " ":
        start -= 1

    out = line[start : bounds.end_index].strip()  # noqa E203

    continues_next_line = out.endswith("\\")
    if continues_next_line:
        out = out[:-1]

    # Ignore whitespace between end of quoted string and a backslash continuation
    # character by rstrip before checking for quotation ends.
    is_quoted = (out.startswith('"') and out.rstrip().endswith('"')) or (
        out.startswith("'") and out.rstrip().endswith("'")
    )
    if is_quoted:
        return out.rstrip()[1:-1], continues_next_line
    return out, continues_next_line


def _explode_line(
    boundaries: typing.List["ColumnBounds"], line: str
) -> typing.Tuple[typing.List[typing.Optional[str]], bool]:
    cells: typing.List[typing.Optional[str]] = []
    continues_on_next_line = False
    for bounds in boundaries:
        cell, continues = _extract_cell(bounds, line)
        cells.append(cell)
        continues_on_next_line = continues_on_next_line or continues
    return cells, continues_on_next_line


def _combine_cells_across_lines(
    existing: typing.List[typing.Optional[str]],
    continuations: typing.List[typing.Optional[str]],
) -> typing.List[typing.Optional[str]]:
    combined: typing.List[typing.Optional[str]] = []
    for prefix, suffix in zip(existing, continuations):
        if prefix is None and suffix is None:
            combined.append(None)
        elif prefix is None:
            combined.append(suffix)
        elif suffix is None:
            combined.append(prefix)
        else:
            combined.append(f"{prefix}{suffix}")
    return combined


def _to_pandas(columns: typing.List["RawColumn"]):
    if pd is None:
        raise RuntimeError("No pandas module was found.")

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


def _to_polars(columns: typing.List["RawColumn"]):
    if pl is None:
        raise RuntimeError("No polars module was found.")

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


def reads(
    table: str,
    kind: typing.Literal["pandas", "polars"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    modifier_prefix: str = "&",
):
    """Read dftxt string into a Pandas DataFrame."""
    blocks = _read_blocks(
        table.replace("\r", "").replace("\t", "  ").split("\n"),
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


def reads_to_pandas(
    table: str,
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    modifier_prefix: str = "&",
) -> "pd.DataFrame":
    """Read the table to a Pandas DataFrame."""
    return typing.cast(
        pd.DataFrame,
        reads(
            table=table, kind="pandas", filters=filters, modifier_prefix=modifier_prefix
        ),
    )


def reads_to_polars(
    table: str,
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    modifier_prefix: str = "&",
) -> "pl.DataFrame":
    """Read the table to a Polars DataFrame."""
    return typing.cast(
        pl.DataFrame,
        reads(
            table=table, kind="polars", filters=filters, modifier_prefix=modifier_prefix
        ),
    )


def read(
    path: typing.Union[pathlib.Path, str],
    kind: typing.Literal["pandas", "polars"] = "pandas",
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    modifier_prefix: str = "&",
    encoding: str = "utf-8",
):
    """Read dftxt file into a Pandas DataFrame."""
    return reads(
        table=pathlib.Path(path).expanduser().resolve().read_text(encoding),
        kind=kind,
        filters=filters,
        modifier_prefix=modifier_prefix,
    )


def read_to_pandas(
    path: typing.Union[pathlib.Path, str],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    modifier_prefix: str = "&",
    encoding: str = "utf-8",
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
        ),
    )


def read_to_polars(
    path: typing.Union[pathlib.Path, str],
    filters: typing.Union[str, typing.Sequence[str], None] = None,
    modifier_prefix: str = "&",
    encoding: str = "utf-8",
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
        ),
    )
