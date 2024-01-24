import difflib
import pathlib

from pytest import mark

from dftxt._io import _markdown

_DIRECTORY = pathlib.Path(__file__).resolve().parent / "scenarios"
_SCENARIOS = [d.name for d in _DIRECTORY.iterdir() if d.is_dir()]


@mark.parametrize("name", _SCENARIOS)
def test_markdown(name: str):
    """Should operate on a markdown file as expected."""
    directory = _DIRECTORY / name
    observed_extracted = _markdown.extract(
        directory.joinpath("source.md").read_text("utf-8")
    )

    expected_extracted = directory.joinpath("extracted.dftxt").read_text("utf-8")
    diff_lines = list(
        difflib.unified_diff(
            [
                "{}\n".format(line)
                for line in expected_extracted.replace("\r", "").split("\n")
            ],
            [
                "{}\n".format(line)
                for line in observed_extracted.replace("\r", "").split("\n")
            ],
            "expected",
            "observed",
        )
    )

    extraction_matches = observed_extracted == expected_extracted
    assert (
        extraction_matches
    ), "Unexpected extraction results:\n{}\n\nfrom observed\n\n{}".format(
        "".join(diff_lines), observed_extracted
    )
