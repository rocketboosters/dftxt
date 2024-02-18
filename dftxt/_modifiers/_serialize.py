import typing

from . import _types
from . import _utils


def serialize(
    modifiers: "_types.ColumnModifiers",
    modifier_prefix: str = "&",
    allow_short: bool = True,
    repeat: bool = False,
) -> typing.List[str]:
    """Convert column modifiers into line representation for serialization."""
    lines = []

    if repeat:
        # No other modifiers are present when the column is being repeated.
        key = "-" if allow_short else "repeat"
        lines.append(f"{modifier_prefix}{key}")
        return lines

    if modifiers.skip:
        # No other modifiers are present when the column is skipped.
        lines.append(f"{modifier_prefix}skip")
        return lines

    # For integer index columns, the datatype can be specified in the index identifier
    # and so the integer type should not be included.
    skip_data_type = False

    if modifiers.index and modifiers.data_type == "int":
        key = _utils.given(allow_short, "intidx", "int_index")
        lines.append(f"{modifier_prefix}{key}")
        skip_data_type = True
    elif modifiers.index:
        key = _utils.given(allow_short, "idx", "index")
        lines.append(f"{modifier_prefix}{key}")

    if not skip_data_type and modifiers.data_type not in (None, "str", "string"):
        key = _utils.given(allow_short, "&", "dtype")
        value = _utils.given(
            allow_short, modifiers.data_type, f"={modifiers.data_type}"
        )
        lines.append(f"{modifier_prefix}{key}{value}")

    if modifiers.name_data_type not in (None, "str"):
        key = _utils.given(allow_short, "ntype", "nametype")
        lines.append(f"{modifier_prefix}{key}={modifiers.name_data_type}")

    lines += [
        "{prefix}{key}{value}".format(
            prefix=modifier_prefix,
            key=_utils.given(allow_short, "+", "if"),
            value=_utils.given(allow_short, alias, f"={alias}"),
        )
        for alias in sorted(modifiers.only_filters)
    ]

    lines += [
        "{prefix}{key}{value}".format(
            prefix=modifier_prefix,
            key=_utils.given(allow_short, "-", "not"),
            value=_utils.given(allow_short, alias, f"={alias}"),
        )
        for alias in sorted(modifiers.never_filters)
    ]

    return lines
