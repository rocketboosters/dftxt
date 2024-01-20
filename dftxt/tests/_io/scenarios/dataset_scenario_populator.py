"""Supporting population function for externally-loaded dataset scenarios."""
import json
import os
import pathlib
import random
import sys

import seaborn

_DIRECTORY = pathlib.Path(__file__).resolve().parent
_ROOT_DIRECTORY = _DIRECTORY.joinpath("..", "..", "..", "..").resolve()


def populate():
    """Populate the test scenarios with all available externally-loaded datasets."""
    sys.path.append(str(_ROOT_DIRECTORY))

    import dftxt

    expected_script = _DIRECTORY.joinpath("dataset_expected.py").read_text("utf-8")

    for name in seaborn.get_dataset_names():
        print(f"Writing: {name} dataset")
        allow_short = random.choice([True, False])
        line_width = random.choice([88, 79, 0])

        df = seaborn.load_dataset(name)
        row_count = min(random.randint(2, 10), len(df.index))
        directory = _DIRECTORY / name
        directory.mkdir(parents=True, exist_ok=True)
        sampled = df.sample(row_count)

        source_path = directory / "source.dftxt"
        dftxt.write(
            source_path,
            sampled,
            index=True,
            allow_short=allow_short,
            line_width=line_width,
        )

        expected_polars_path = directory / "expected_polars.dftxt"
        dftxt.write(
            expected_polars_path,
            dftxt.read_to_polars(source_path),
            index=True,
            allow_short=allow_short,
            line_width=line_width,
        )
        source_contents = source_path.read_text("utf-8")
        expected_polars_contents = expected_polars_path.read_text("utf-8")
        if source_contents == expected_polars_contents:
            os.remove(expected_polars_path)

        scenario = {
            "read": {"args": {}},
            "write": {
                "args": {
                    "index": True,
                    "allow_short": allow_short,
                    "line_width": line_width,
                }
            },
            "index_reference": sampled.index.tolist(),
        }
        directory.joinpath("scenario.json").write_text(json.dumps(scenario, indent=2))

        directory.joinpath("expected.py").write_text(
            data=expected_script,
            encoding="utf-8",
        )


if __name__ == "__main__":
    populate()
