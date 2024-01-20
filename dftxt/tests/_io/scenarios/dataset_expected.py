"""Source for the expected script copied into externally-loaded dataset scenarios."""
import json
import pathlib

if __name__ == "io_scenario_test":
    import polars as pl
    import seaborn

    _DIRECTORY = pathlib.Path(__file__).resolve().parent
    _NAME = _DIRECTORY.name
    _SCENARIO = json.loads(_DIRECTORY.joinpath("scenario.json").read_text("utf-8"))

    expected_pandas = (
        seaborn.load_dataset(_NAME).reindex(_SCENARIO["index_reference"]).copy()
    )
    expected_pandas.index.name = "index"

    datetime_columns = []
    for column in expected_pandas.columns:
        if str(expected_pandas[column].dtype).lower().startswith("datetime64"):
            datetime_columns.append(column)

    expected_polars = pl.from_pandas(
        expected_pandas.reset_index(drop=False, names="index")
    ).cast({c: pl.Datetime() for c in datetime_columns})
