import pathlib

import pandas.testing
import polars.testing

import dftxt

_DIRECTORY = pathlib.Path(__file__).resolve().parent


def test_multiple_frames_unnamed_pandas():
    """Should load the frames file so that the test behavior passes."""
    frames = dftxt.read_all_to_pandas(_DIRECTORY / "frames.dftxt")
    combined = (
        frames[0]
        .merge(
            frames[1],
            how="left",
            left_on="Favorite Movie",
            right_on="Movie",
        )
        .drop(columns=["Movie"])
    )
    pandas.testing.assert_frame_equal(combined, frames[2])


def test_multiple_frames_unnamed_polars():
    """Should load the frames file so that the test behavior passes."""
    frames = dftxt.read_all_to_polars(_DIRECTORY / "frames.dftxt")
    combined = frames[0].join(
        frames[1],
        how="left",
        left_on="Favorite Movie",
        right_on="Movie",
    )
    polars.testing.assert_frame_equal(combined, frames[2])
