import typing

from pytest import mark

from dftxt._io import _read


class Scenario(typing.TypedDict):
    """Interface for scenario tests."""

    headers: str
    expected: typing.List["_read.ColumnBounds"]


_SCENARIOS: typing.Dict[str, "Scenario"] = {
    "basic": {
        "headers": "foo   bar     hello world    spam      ",
        "expected": [
            _read.ColumnBounds(0, 6),
            _read.ColumnBounds(6, 14),
            _read.ColumnBounds(14, 29),
            _read.ColumnBounds(29, 100_000),
        ],
    }
}


@mark.parametrize("scenario", list(_SCENARIOS.keys()))
def test_find_columns(scenario: str):
    """Should find the expected columns for the given scenario."""
    data = _SCENARIOS[scenario]
    observed = _read._find_boundaries(data["headers"])

    expected: typing.List[_read.ColumnBounds] = data["expected"]
    assert observed == expected, "Observed:\n{}".format(
        "\n".join([str(o) for o in observed])
    )
