import re
import typing

from .. import _modifiers
from . import _types


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


def _extract_cell(
    bounds: "_types.ColumnBounds", line: str
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
    boundaries: typing.List["_types.ColumnBounds"], line: str
) -> typing.Tuple[typing.List[typing.Optional[str]], bool]:
    cells: typing.List[typing.Optional[str]] = []
    continues_on_next_line = False
    for bounds in boundaries:
        cell, continues = _extract_cell(bounds, line)
        cells.append(cell)
        continues_on_next_line = continues_on_next_line or continues
    return cells, continues_on_next_line


def _append_columnwise(
    existing: typing.List[typing.List[typing.Optional[str]]],
    new_cells: typing.List[typing.Optional[str]],
) -> typing.List[typing.List[typing.Optional[str]]]:
    return [[*column, cell] for column, cell in zip(existing, new_cells)]


def find_boundaries(first_header_line: str) -> typing.List["_types.ColumnBounds"]:
    """Find the boundaries of columns from a header line."""
    raw = first_header_line.rstrip()
    if not raw:
        return []

    regex = re.compile(r"\s{2,}")
    columns: typing.List["_types.ColumnBounds"] = []
    position = 0
    while position < len(raw):
        match = regex.search(raw, pos=position)
        if match:
            next_position = match.end()
        else:
            next_position = 100_000

        columns.append(
            _types.ColumnBounds(start_index=position, end_index=next_position)
        )
        position = next_position

    return columns


def read_blocks(
    lines: typing.List[str],
    modifier_prefix: str = "&",
) -> typing.List["_types.RawTableBlock"]:
    """
    Read table block data into its raw separated format for parsing. This is expected
    to be a single DataFrame entry. Separating DataFrames should already be carried out
    before passing lines to this function.

    :param lines: Exploded lines of a dftxt DataFrame to read into blocks.
    :param modifier_prefix: The prefix modifier character(s) to use when parsing
        modifiers.
    :returns: A list of table blocks from the parsed lines.
    """
    blocks = []

    column_boundaries: typing.List[_types.ColumnBounds] = []
    column_names: typing.List[str] = []
    column_modifiers: typing.List[typing.List[typing.Optional[str]]] = []
    column_data: typing.List[typing.List[typing.Optional[str]]] = []

    remaining_lines = lines.copy()
    contiguous_blank_line_count = 0
    while remaining_lines:
        raw = remaining_lines.pop(0)
        stripped = raw.strip()

        start_new_block = (
            len(stripped) > 0
            and len(column_names) > 0
            and contiguous_blank_line_count > 1
        )
        if start_new_block:
            # A row of multiple blank lines starts a new block.
            contiguous_blank_line_count = 0
            columns = [
                _types.RawColumn(
                    bounds=bounds,
                    name=name,
                    modifiers=_modifiers.parse(modifiers, modifier_prefix),
                    cells=cells,
                )
                for bounds, name, modifiers, cells in zip(
                    column_boundaries, column_names, column_modifiers, column_data
                )
            ]
            blocks.append(_types.RawTableBlock(columns))
            column_boundaries = []
            column_names = []
            column_modifiers = []
            column_data = []

        if not stripped:
            contiguous_blank_line_count += 1
            # Ignore empty lines
            continue

        contiguous_blank_line_count = 0
        if stripped.startswith("#"):
            # Ignore comments during read.
            continue

        if not column_boundaries:
            column_boundaries = find_boundaries(raw)
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

    if column_names:
        columns = [
            _types.RawColumn(
                bounds=bounds,
                name=name,
                modifiers=_modifiers.parse(modifiers, modifier_prefix),
                cells=cells,
            )
            for bounds, name, modifiers, cells in zip(
                column_boundaries, column_names, column_modifiers, column_data
            )
        ]
        blocks.append(_types.RawTableBlock(columns))

    return blocks
