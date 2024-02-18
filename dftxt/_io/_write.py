import dataclasses
import pathlib
import textwrap
import typing

from .. import _cast
from .. import _modifiers

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
class SerializedColumn:
    """Data structure for serialized columns."""

    name: str
    modifiers: "_modifiers.ColumnModifiers"
    values: typing.List[str]


def _from_pandas(
    data_frame: "pd.DataFrame",
    index: typing.Optional[bool] = False,
) -> typing.List["SerializedColumn"]:
    """Convert Pandas DataFrame into a list of Serialized columns for writing."""
    out: typing.List[SerializedColumn] = []
    for name in data_frame.columns:
        name_data_type = _cast.from_value(name)
        stringified_name = _cast.cast_from(name, name_data_type)
        out.append(
            SerializedColumn(
                name=stringified_name,
                modifiers=_modifiers.ColumnModifiers(
                    data_type=(dtype := _cast.from_pandas(data_frame[name])),
                    name_data_type=name_data_type,
                ),
                values=[
                    _cast.cast_from(v, dtype, data_frame[name].dtype)
                    for v in data_frame[name].values
                ],
            )
        )

    if not index:
        return out

    if isinstance(data_frame.index, pd.MultiIndex):
        index_columns = _from_pandas(data_frame.index.to_frame(), index=False)
        for column in index_columns:
            column.modifiers.index = True

        return index_columns + out

    name = data_frame.index.name
    out.insert(
        0,
        SerializedColumn(
            name=str(name or "index"),
            modifiers=_modifiers.ColumnModifiers(
                data_type=_cast.from_pandas(data_frame.index.to_series()),
                index=True,
                name_data_type=_cast.from_value(name or ""),
            ),
            values=[_cast.cast_from(x, "int") for x in data_frame.index.values],
        ),
    )
    return out


def _from_polars(
    data_frame: "pl.DataFrame",
    index: typing.Union[bool, str, typing.Sequence[str], None] = False,
) -> typing.List["SerializedColumn"]:
    """Convert Polars DataFrame into a list of Serialized columns for writing."""
    index_names: typing.List[str] = []
    if index is True:
        index_names.append("index")
    elif isinstance(index, str):
        index_names.append(index)
    elif index and hasattr(index, "__len__"):
        index_names.extend(list(typing.cast(typing.Sequence[str], index)))

    return [
        SerializedColumn(
            name=c,
            modifiers=_modifiers.ColumnModifiers(
                data_type=(dtype := _cast.from_polars(data_frame[c])),
                index=c in index_names,
                name_data_type=_cast.from_value(c),
            ),
            values=[
                _cast.cast_from(v, dtype, data_frame[c].dtype)
                for v in data_frame[c].to_list()
            ],
        )
        for c in data_frame.columns
    ]


def _quote(value: str, ignore_end: bool = False) -> str:
    needs_quote_start = "  " in value or value.startswith(" ")
    needs_quote_end = value.endswith(" ") or value.endswith("\\")
    needs_quoting = needs_quote_start or (needs_quote_end and not ignore_end)
    if needs_quoting:
        return f'"{value}"'
    return value


def _format_cell(value: str, max_width: int = -1) -> typing.List[str]:
    needs_wrapping = max_width > 0 and len(value) > max_width
    if not needs_wrapping:
        return [_quote(value)]

    lines = [
        _quote(line, True)
        for line in textwrap.wrap(value, max_width - 1, drop_whitespace=False)
    ]
    width = max(0, *[len(line) for line in lines])
    if width > max_width:
        delta = width - max_width - 1
        lines = [
            _quote(line, True)
            for line in textwrap.wrap(value, max_width - delta, drop_whitespace=False)
        ]

    return [f"{line}\\" for line in lines[:-1]] + [lines[-1]]


def _serialize(
    column: "SerializedColumn",
    modifier_lines: typing.List[str],
    modifier_prefix: str,
    modifier_count: int,
    is_repeat: bool = False,
    allow_short: bool = False,
    max_width: int = -1,
):
    cells = [_format_cell(column.name, max_width)]

    if is_repeat:
        key = "-" if allow_short else "repeat"
        cells.append(_format_cell(f"{modifier_prefix}{key}", max_width))
    else:
        cells.extend([_format_cell(line, max_width) for line in modifier_lines])

    cells.extend([[""] for _ in range(modifier_count + 1 - len(cells))])
    cells.extend([_format_cell(v, max_width) for v in column.values])
    width = max([len(line) for cell in cells for line in cell]) + 2
    whitespace = width * " "
    for i in range(len(cells)):
        for j in range(len(cells[i])):
            cells[i][j] = f"{cells[i][j]}{whitespace}"[:width]
    return cells, width


def _render_block(
    block_index: int,
    columns: typing.List["SerializedColumn"],
    columns_modifiers: typing.List[typing.List[str]],
    repeats: typing.List["SerializedColumn"],
    repeats_modifiers: typing.List[typing.List[str]],
    line_width: int,
    column_width: typing.Union[int, typing.Dict[str, int]],
    allow_short: bool,
    modifier_prefix: str,
    modifier_count: int,
) -> typing.Tuple[typing.List["SerializedColumn"], typing.List[typing.List[str]], str]:
    cell_grid: typing.List[typing.List[typing.List[str]]] = []
    widths: typing.List[int] = []

    for repeat, modifier_lines in zip(repeats, repeats_modifiers):
        cells, width = _serialize(
            column=repeat,
            modifier_lines=modifier_lines,
            modifier_count=modifier_count,
            modifier_prefix=modifier_prefix,
            is_repeat=block_index > 0,
            allow_short=allow_short,
            max_width=(
                column_width.get(repeat.name, -1)
                if isinstance(column_width, dict)
                else column_width
            ),
        )
        widths.append(width)
        cell_grid.append(cells)

    next_index = 0
    for i, (modifier_lines, column) in enumerate(zip(columns_modifiers, columns)):
        cells, width = _serialize(
            column=column,
            modifier_lines=modifier_lines,
            modifier_prefix=modifier_prefix,
            modifier_count=modifier_count,
            is_repeat=False,
            allow_short=allow_short,
            max_width=(
                column_width.get(column.name, -1)
                if isinstance(column_width, dict)
                else column_width
            ),
        )
        should_start_new_block = (
            # Everything goes in one block if the line width is <= 0.
            line_width > 0
            # Blocks should always have at least one column in them after any repeats.
            and i > 1
            # Content has a 2-space right padding that can be removed if the column is
            # the last column in the block.
            and (sum(widths) + width - 2) > line_width
        )
        if should_start_new_block:
            break

        next_index = i + 1
        widths.append(width)
        cell_grid.append(cells)

    lines: typing.List[str] = []
    for row_index in range(len(cell_grid[0])):
        row_cells = [column[row_index].copy() for column in cell_grid]
        while any(row_cells):
            line = "".join(
                [
                    cell.pop(0) if cell else widths[i] * " "
                    for i, cell in enumerate(row_cells)
                ]
            ).rstrip()
            if line:
                lines.append(line)

    block = "\n".join(lines)

    return columns[next_index:], columns_modifiers[next_index:], block


def _extract_repeat_columns(
    columns: typing.Sequence["SerializedColumn"],
    repeat_columns: typing.Union[None, str, typing.Sequence[str]],
) -> typing.Tuple[typing.List["SerializedColumn"], typing.List["SerializedColumn"]]:
    index_columns = [c for c in columns if c.modifiers.index]
    non_index_columns = [c for c in columns if not c.modifiers.index]
    if not repeat_columns and not index_columns:
        return list(columns).copy(), []

    if not repeat_columns and index_columns:
        return non_index_columns, index_columns

    if isinstance(repeat_columns, str):
        found = next((c for c in non_index_columns if c.name == repeat_columns), None)
        return index_columns + [c for c in non_index_columns if c != found], (
            [] if found is None else [found]
        )

    names = typing.cast(typing.List[str], repeat_columns)
    # Match the order specified by the user for the repeats.
    repeats = {c.name: c for c in non_index_columns if c.name in names}
    return index_columns + [c for c in non_index_columns if c.name not in repeats], [
        repeats[n] for n in names if n in repeats
    ]


def _render_modifiers(
    columns: typing.List["SerializedColumn"],
    modifier_prefix: str,
    allow_short: bool = False,
) -> typing.List[typing.List[str]]:
    return [
        _modifiers.serialize(
            c.modifiers,
            modifier_prefix=modifier_prefix,
            allow_short=allow_short,
        )
        for c in columns
    ]


def _add_filters(
    columns: typing.List["SerializedColumn"],
    only_filters: typing.Mapping[typing.Any, typing.Sequence[str]],
    never_filters: typing.Mapping[typing.Any, typing.Sequence[str]],
):
    """Add only/never filters to the serialized columns."""
    for column in columns:
        column.modifiers.only_filters.update(only_filters.get(column.name) or [])
        column.modifiers.never_filters.update(never_filters.get(column.name) or [])


def writes(
    data_frame: typing.Union["pd.DataFrame", "pl.DataFrame"],
    line_width: int = 88,
    allow_short: bool = False,
    repeat_columns: typing.Union[None, str, typing.Sequence[str]] = None,
    only_filters: typing.Optional[
        typing.Mapping[typing.Any, typing.Sequence[str]]
    ] = None,
    never_filters: typing.Optional[
        typing.Mapping[typing.Any, typing.Sequence[str]]
    ] = None,
    modifier_prefix: str = "&",
    index: typing.Union[bool, str, typing.Sequence[str], None] = False,
    column_width: typing.Union[int, typing.Dict[str, int]] = -1,
) -> str:
    """Serialize the DataFrame to a dftxt string."""
    if pd is not None and isinstance(data_frame, pd.DataFrame):
        columns = _from_pandas(data_frame, bool(index))
    elif pl is not None and isinstance(data_frame, pl.DataFrame):
        columns = _from_polars(data_frame, index)
    else:
        raise ValueError(f"Unknown DataFrame type of '{type(data_frame)}'.")

    _add_filters(columns, only_filters or {}, never_filters or {})

    remaining, repeats = _extract_repeat_columns(columns, repeat_columns)
    remaining_columns_modifiers = _render_modifiers(
        columns=remaining,
        modifier_prefix=modifier_prefix,
        allow_short=allow_short,
    )
    repeat_columns_modifiers = _render_modifiers(
        columns=repeats,
        modifier_prefix=modifier_prefix,
        allow_short=allow_short,
    )

    modifier_count = max(
        1,
        *[len(lines) for lines in remaining_columns_modifiers],
        *[len(lines) for lines in repeat_columns_modifiers],
    )

    blocks: typing.List[str] = []
    while remaining:
        remaining, remaining_columns_modifiers, block = _render_block(
            block_index=len(blocks),
            columns=remaining,
            repeats=repeats,
            line_width=line_width,
            column_width=column_width,
            allow_short=allow_short,
            columns_modifiers=remaining_columns_modifiers,
            repeats_modifiers=repeat_columns_modifiers,
            modifier_prefix=modifier_prefix,
            modifier_count=modifier_count,
        )
        blocks.append(block)
    return "\n\n\n".join(blocks)


def writes_all(
    data_frames: typing.Union[
        typing.Mapping[str, typing.Union["pd.DataFrame", "pl.DataFrame"]],
        typing.Sequence[typing.Union["pd.DataFrame", "pl.DataFrame"]],
    ],
    line_width: int = 88,
    allow_short: bool = False,
    repeat_columns: typing.Union[None, str, typing.Sequence[str]] = None,
    only_filters: typing.Optional[
        typing.Mapping[typing.Any, typing.Sequence[str]]
    ] = None,
    never_filters: typing.Optional[
        typing.Mapping[typing.Any, typing.Sequence[str]]
    ] = None,
    modifier_prefix: str = "&",
    index: typing.Union[bool, str, typing.Sequence[str], None] = False,
    column_width: typing.Union[int, typing.Dict[str, int]] = -1,
):
    """Write the serialized DataFrames to a dftxt string."""
    if isinstance(data_frames, dict):
        frames = list(data_frames.items())
    else:
        frames = [
            (None, typing.cast(typing.Union["pd.DataFrame", "pl.DataFrame"], df))
            for df in data_frames
        ]

    chunks: typing.List[str] = []
    for name, data_frame in frames:
        if name:
            prefix = "\n" if len(chunks) > 0 else ""
            chunks.append(f"{prefix}--- {name} ---\n")
        elif len(chunks) > 0:
            chunks.append("\n---")

        chunks.append(
            writes(
                data_frame=data_frame,
                line_width=line_width,
                allow_short=allow_short,
                repeat_columns=repeat_columns,
                only_filters=only_filters,
                never_filters=never_filters,
                modifier_prefix=modifier_prefix,
                index=index,
                column_width=column_width,
            )
        )

    return "\n".join(chunks)


def write(
    path: typing.Union[str, pathlib.Path],
    data_frame: typing.Union["pd.DataFrame", "pl.DataFrame"],
    line_width: int = 88,
    allow_short: bool = False,
    repeat_columns: typing.Union[None, str, typing.Sequence[str]] = None,
    only_filters: typing.Optional[
        typing.Mapping[typing.Any, typing.Sequence[str]]
    ] = None,
    never_filters: typing.Optional[
        typing.Mapping[typing.Any, typing.Sequence[str]]
    ] = None,
    modifier_prefix: str = "&",
    encoding: str = "utf-8",
    index: typing.Union[bool, str, typing.Sequence[str], None] = False,
    column_width: typing.Union[int, typing.Dict[str, int]] = -1,
):
    """Write the serialized DataFrame to the specified file."""
    pathlib.Path(path).expanduser().resolve().write_text(
        writes(
            data_frame=data_frame,
            line_width=line_width,
            allow_short=allow_short,
            modifier_prefix=modifier_prefix,
            repeat_columns=repeat_columns,
            only_filters=only_filters,
            never_filters=never_filters,
            index=index,
            column_width=column_width,
        )
        + "\n",
        encoding=encoding,
    )


def write_all(
    path: typing.Union[str, pathlib.Path],
    data_frames: typing.Union[
        typing.Mapping[str, typing.Union["pd.DataFrame", "pl.DataFrame"]],
        typing.Sequence[typing.Union["pd.DataFrame", "pl.DataFrame"]],
    ],
    line_width: int = 88,
    allow_short: bool = False,
    repeat_columns: typing.Union[None, str, typing.Sequence[str]] = None,
    only_filters: typing.Optional[
        typing.Mapping[typing.Any, typing.Sequence[str]]
    ] = None,
    never_filters: typing.Optional[
        typing.Mapping[typing.Any, typing.Sequence[str]]
    ] = None,
    modifier_prefix: str = "&",
    encoding: str = "utf-8",
    index: typing.Union[bool, str, typing.Sequence[str], None] = False,
    column_width: typing.Union[int, typing.Dict[str, int]] = -1,
):
    """Write multiple, serialized Pandas/Polars DataFrames to the specified file."""
    pathlib.Path(path).expanduser().resolve().write_text(
        writes_all(
            data_frames=data_frames,
            line_width=line_width,
            allow_short=allow_short,
            modifier_prefix=modifier_prefix,
            repeat_columns=repeat_columns,
            only_filters=only_filters,
            never_filters=never_filters,
            index=index,
            column_width=column_width,
        )
        + "\n",
        encoding=encoding,
    )
