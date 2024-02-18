import typing

from .. import _cast
from . import _types
from . import _utils


def _add_modifier(modifiers: "_types.ColumnModifiers", line: str):
    parts = [part.strip() for part in line.split("=", 1)]
    key = parts[0].lower()
    value = parts[1] if len(parts) == 2 else None

    if key.startswith("+") and len(key) > 1:
        modifiers.only_filters.add(key[1:])
        return

    if key in ("if", "only") and value:
        modifiers.only_filters.add(value)
        return

    if key.startswith("-") and len(key) > 1:
        modifiers.never_filters.add(key[1:])
        return

    if key in ("no", "not", "never", "ifnot", "notif", "if_not", "not_if") and value:
        modifiers.never_filters.add(value)
        return

    if key.startswith("&"):
        modifiers.data_type = line.split("=", 1)[0].strip()[1:]
        return

    if key in ("dtype", "data_type"):
        modifiers.data_type = value or "str"
        return

    if key in ("ntype", "ndtype", "nametype", "name_type", "name_data_type"):
        modifiers.name_data_type = value or "str"
        return

    if key in ("intidx", "int_index", "int_idx"):
        modifiers.index = True
        modifiers.data_type = "int"
        return

    if key in ("index", "idx"):
        modifiers.index = _utils.given(value is not None, _cast.to_boolean(value), True)
        return

    if key in ("never", "exclude", "ignore", "skip", "repeat", "-"):
        modifiers.skip = True
        return

    raise ValueError(f"Unrecognized modifier specification '{line}'.")


def parse(
    modifier_lines: typing.Sequence[typing.Optional[str]], modifier_prefix: str = "&"
) -> "_types.ColumnModifiers":
    """Convert raw dftxt-serialized modifier lines into ColumnModifiers data."""
    modifiers = _types.ColumnModifiers()
    for line in modifier_lines:
        if line:
            _add_modifier(modifiers, line.strip()[len(modifier_prefix) :])  # noqa E203
    return modifiers
