import json
import pathlib

from pytest import mark

from dftxt._io import _modifiers

_DIRECTORY = pathlib.Path(__file__).resolve().parent / "scenarios"
_SCENARIOS = [p.name[:-5] for p in _DIRECTORY.iterdir() if p.name.endswith(".json")]


@mark.parametrize("name", _SCENARIOS)
def test_modifier_scenario(name: str):
    """Should execute the scenario as expected."""
    scenario = json.loads(_DIRECTORY.joinpath(f"{name}.json").read_text("utf-8"))
    observed = _modifiers.parse(scenario["input"], **scenario.get("parse_args", {}))
    assert observed.to_serial_format() == scenario["modifiers"]
    serialized = _modifiers.serialize(observed, **scenario.get("serialize_args", {}))
    assert not set(serialized).difference(set(scenario["serialized"])), serialized
    observed_backwards = _modifiers.parse(serialized, **scenario.get("parse_args", {}))
    assert observed_backwards.to_serial_format() == scenario["modifiers"]
