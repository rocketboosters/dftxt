import dataclasses
import typing

from . import _cast


@dataclasses.dataclass()
class ColumnModifiers:
    """Data structure for loaded/parsed column modifiers."""

    data_type: typing.Optional[str] = None
    index: typing.Optional[bool] = False
    name_data_type: typing.Optional[str] = None
    skip: typing.Optional[bool] = False
    only_filters: typing.Set[str] = dataclasses.field(default_factory=lambda: set())
    never_filters: typing.Set[str] = dataclasses.field(default_factory=lambda: set())

    def to_serial_format(self) -> typing.Dict[str, typing.Any]:
        """Convert the modifiers into a dictionary format useful in test comparisons."""
        out = dataclasses.asdict(self)
        out["only_filters"] = list(self.only_filters)
        out["never_filters"] = list(self.never_filters)
        return out


def _add_modifier(modifiers: "ColumnModifiers", line: str):
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
        modifiers.index = _or(value is not None, _cast.to_boolean(value), True)
        return

    if key in ("never", "exclude", "ignore", "skip", "repeat", "-"):
        modifiers.skip = True
        return

    raise ValueError(f"Unrecognized modifier specification '{line}'.")


def parse(
    modifier_lines: typing.Sequence[typing.Optional[str]], modifier_prefix: str = "&"
) -> "ColumnModifiers":
    """Convert raw dftxt-serialized modifier lines into ColumnModifiers data."""
    modifiers = ColumnModifiers()
    for line in modifier_lines:
        if line:
            _add_modifier(modifiers, line.strip()[len(modifier_prefix) :])  # noqa E203
    return modifiers


def _or(condition: bool, yes: typing.Any, no: typing.Any) -> typing.Any:
    return yes if condition else no


def serialize(
    modifiers: "ColumnModifiers",
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
        key = _or(allow_short, "intidx", "int_index")
        lines.append(f"{modifier_prefix}{key}")
        skip_data_type = True
    elif modifiers.index:
        key = _or(allow_short, "idx", "index")
        lines.append(f"{modifier_prefix}{key}")

    if not skip_data_type and modifiers.data_type not in (None, "str", "string"):
        key = _or(allow_short, "&", "dtype")
        value = _or(allow_short, modifiers.data_type, f"={modifiers.data_type}")
        lines.append(f"{modifier_prefix}{key}{value}")

    if modifiers.name_data_type not in (None, "str"):
        key = _or(allow_short, "ntype", "nametype")
        lines.append(f"{modifier_prefix}{key}={modifiers.name_data_type}")

    lines += [
        "{prefix}{key}{value}".format(
            prefix=modifier_prefix,
            key=_or(allow_short, "+", "if"),
            value=_or(allow_short, alias, f"={alias}"),
        )
        for alias in sorted(modifiers.only_filters)
    ]

    lines += [
        "{prefix}{key}{value}".format(
            prefix=modifier_prefix,
            key=_or(allow_short, "-", "not"),
            value=_or(allow_short, alias, f"={alias}"),
        )
        for alias in sorted(modifiers.never_filters)
    ]

    return lines
