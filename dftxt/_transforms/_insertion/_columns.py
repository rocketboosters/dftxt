import typing

from dftxt import _parsing


def add_column(
    source: str,
    column_name: str,
    before: typing.Optional[str] = None,
    after: typing.Optional[str] = None,
    index: typing.Optional[int] = None,
    appearance_index: typing.Optional[int] = 0,
):
    """Add a column to an existing dftxt file."""
    blocks = _parsing.read_blocks(
        lines=source.replace("\r", "").strip("\n").split("\n"),
    )
    next_column_name = after

    if index is not None:
        columns = [c for b in blocks for c in b.columns if not c.modifiers.skip]
        if index == -1 or index >= len(columns):
            next_column_name = None
        elif index < 0:
            next_column_name = columns[index + 1].name
        else:
            next_column_name = columns[index].name


def append_column(source: str, column_name: str):
    """Append a new column to the end of an existing dftxt file."""
    return add_column(source, column_name, index=-1)


def prepend_column(source: str, column_name: str):
    """Prepend a new column to the start of an existing dftxt file."""
    return add_column(source, column_name, index=0)
