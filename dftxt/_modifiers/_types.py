import dataclasses
import typing


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
