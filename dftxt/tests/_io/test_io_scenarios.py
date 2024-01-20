import difflib
import json
import os
import pathlib
import tempfile
import typing

import pandas as pd
import pandas.testing as pd_test
import polars as pl
import polars.testing as pl_test
from pytest import mark

import dftxt

_DIRECTORY = pathlib.Path(__file__).resolve().parent / "scenarios"
_SCENARIOS = [d.name for d in _DIRECTORY.iterdir() if d.is_dir()]


def _assert_frame_equal(
    observed: typing.Union[pd.DataFrame, pl.DataFrame],
    expected: typing.Union[pd.DataFrame, pl.DataFrame],
):
    """Compare the DataFrames for equality."""
    if isinstance(observed, pd.DataFrame):
        try:
            pd_test.assert_frame_equal(
                typing.cast(pd.DataFrame, observed), typing.cast(pd.DataFrame, expected)
            )
            return
        except Exception:  # pragma: no cover
            print(f"OBSERVED:\n{observed}\n\nEXPECTED:\n{expected}")
            raise

    try:
        # Polars assert_frame_equal has limitations that currently lead to panic errors
        # for certain dtypes. For now, string comparison is more reliable to determine
        # consistency.
        assert str(observed) == str(expected)
        return
    except AssertionError:  # pragma: no cover
        pass

    try:  # pragma: no cover
        pl_test.assert_frame_equal(
            typing.cast(pl.DataFrame, observed), typing.cast(pl.DataFrame, expected)
        )
    except AssertionError:  # pragma: no cover
        print(f"OBSERVED:\n{observed}\n\nEXPECTED:\n{expected}")
        raise


def _run(
    directory: pathlib.Path,
    scenario: typing.Dict[str, typing.Any],
    locals: typing.Dict[str, typing.Any],
    kind: typing.Literal["pandas", "polars"],
):
    read_args: typing.Dict[str, typing.Any] = scenario.get("read", {}).get("args", {})
    expected_read_path = directory / "source.dftxt"
    expected_write_path = next(
        (
            p
            for filename in [
                f"expected_{kind}.dftxt",
                "expected.dftxt",
                "source.dftxt",
            ]
            if (p := directory / filename).exists()
        )
    )

    observed = dftxt.read(expected_read_path, kind=kind, **read_args)
    try:
        _assert_frame_equal(observed, locals[f"expected_{kind}"])
    except Exception:  # pragma: no cover
        print("FAILED TO COMPARE READ DATA FRAME TO EXPECTED.PY")
        raise

    fid, path = tempfile.mkstemp(suffix=".dftxt")
    observed_path = pathlib.Path(path).resolve()
    os.close(fid)

    try:
        write_args: typing.Dict[str, typing.Any] = scenario.get("write", {}).get(
            "args", {}
        )
        dftxt.write(
            data_frame=observed,
            path=observed_path,
            **write_args,
        )
        observed_write = observed_path.read_text("utf-8")
        observed_read_again = dftxt.read(observed_path, kind=kind, **read_args)
    finally:
        os.remove(observed_path)

    expected_write = expected_write_path.read_text("utf-8")
    diff_lines = list(
        difflib.unified_diff(
            [
                "{}\n".format(line)
                for line in expected_write.replace("\r", "").split("\n")
            ],
            [
                "{}\n".format(line)
                for line in observed_write.replace("\r", "").split("\n")
            ],
            "expected",
            "observed",
        )
    )

    matches_expected_write = len(diff_lines) == 0
    assert (
        matches_expected_write
    ), "Writes do not match:\n{}\n\nfrom observed\n\n{}".format(
        "".join(diff_lines), observed_write
    )

    try:
        _assert_frame_equal(observed_read_again, observed)
    except Exception:  # pragma: no cover
        print("FAILED TO COMPARE RE-READ DATA FRAME TO ORIGINAL READ")
        raise


@mark.parametrize("folder", _SCENARIOS)
def test_io_scenario(folder: str):
    """Should read according to the expected scenario."""
    directory = _DIRECTORY / folder
    scenario_path = directory / "scenario.json"
    scenario = (
        json.loads(scenario_path.read_text("utf-8")) if scenario_path.exists() else {}
    )
    locals: typing.Dict[str, typing.Any] = {}

    exec(
        directory.joinpath("expected.py").read_text("utf-8"),
        {
            **globals(),
            "__file__": str(directory / "expected.py"),
            "__name__": "io_scenario_test",
        },
        locals,
    )

    _run(directory, scenario, locals, "pandas")
    _run(directory, scenario, locals, "polars")
