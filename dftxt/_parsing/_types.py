import dataclasses
import typing

from .. import _cast
from .. import _modifiers


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
                and len(filters.intersection(self.modifiers.only_filters)) == 0
            )
            # If never filters have been set and at least one are satisfied.
            or (
                len(self.modifiers.never_filters) > 0
                and len(filters.intersection(self.modifiers.never_filters)) > 0
            )
        )


@dataclasses.dataclass()
class RawTableBlock:
    """Data structure for raw table blocks."""

    columns: typing.List["RawColumn"] = dataclasses.field(default_factory=lambda: [])
