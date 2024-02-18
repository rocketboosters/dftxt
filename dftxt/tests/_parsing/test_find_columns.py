import typing

from pytest import mark

from dftxt import _parsing


class Scenario(typing.TypedDict):
    """Interface for scenario tests."""

    headers: str
    expected: typing.List["_parsing.ColumnBounds"]


_SCENARIOS: typing.Dict[str, "Scenario"] = {
    "basic": {
        "headers": "foo   bar     hello world    spam      ",
        "expected": [
            _parsing.ColumnBounds(0, 6),
            _parsing.ColumnBounds(6, 14),
            _parsing.ColumnBounds(14, 29),
            _parsing.ColumnBounds(29, 100_000),
        ],
    }
}


@mark.parametrize("scenario", list(_SCENARIOS.keys()))
def test_find_columns(scenario: str):
    """Should find the expected columns for the given scenario."""
    data = _SCENARIOS[scenario]
    observed = _parsing.find_boundaries(data["headers"])

    expected: typing.List[_parsing.ColumnBounds] = data["expected"]
    assert observed == expected, "Observed:\n{}".format(
        "\n".join([str(o) for o in observed])
    )
