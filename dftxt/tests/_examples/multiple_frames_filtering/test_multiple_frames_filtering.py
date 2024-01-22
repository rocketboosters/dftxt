import pathlib

import pandas.testing
import polars.testing

import dftxt

_DIRECTORY = pathlib.Path(__file__).resolve().parent


def test_multiple_frames_unnamed_pandas():
    """Should load the frames file so that the test behavior passes."""
    frames = dftxt.read_all_to_pandas(_DIRECTORY / "frames.dftxt")
    expected_frames = dftxt.read_all_to_pandas(
        _DIRECTORY / "frames.dftxt", filters=["expected"]
    )
    combined = frames.people.merge(
        frames.movies,
        how="left",
        left_on="Favorite Movie",
        right_on="Movie",
    ).drop(columns=["Movie", "Birth Month"])
    pandas.testing.assert_frame_equal(combined, expected_frames.people)


def test_multiple_frames_unnamed_polars():
    """Should load the frames file so that the test behavior passes."""
    frames = dftxt.read_all_to_polars(_DIRECTORY / "frames.dftxt")
    expected_frames = dftxt.read_all_to_polars(
        _DIRECTORY / "frames.dftxt", filters=["expected"]
    )
    combined = frames.people.join(
        frames.movies,
        how="left",
        left_on="Favorite Movie",
        right_on="Movie",
    ).drop(["Birth Month"])
    polars.testing.assert_frame_equal(combined, expected_frames.people)
