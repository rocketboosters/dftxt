if __name__ == "io_scenario_test":
    import pandas as pd
    import polars as pl

    expected_pandas = pd.DataFrame(
        {
            "Name": [
                "Moon",
                "Phobos",
                "Deimos",
                "Io",
                "Europa",
                "Ganymede",
                "Callisto",
                "Amalthea",
                "Himalia",
                "Mimas",
            ],
            "Planet": pd.Series(
                [
                    "Earth",
                    "Mars",
                    "Mars",
                    "Jupiter",
                    "Jupiter",
                    "Jupiter",
                    "Jupiter",
                    "Jupiter",
                    "Jupiter",
                    "Saturn",
                ],
                dtype="category",
            ),
            "Numeral": [
                "I",
                "I",
                "II",
                "I",
                "II",
                "III",
                "IV",
                "V",
                "VI",
                "I",
            ],
            "Mean Radius (km)": [
                1738,
                11.267,
                6.2,
                1821,
                1560,
                2634,
                2410,
                83.5,
                69.8,
                198.2,
            ],
            "Discovery Year": pd.Series(
                [
                    None,
                    1877,
                    1877,
                    1610,
                    1610,
                    1610,
                    1610,
                    1892,
                    1904,
                    1789,
                ],
                dtype="Int64",
            ),
            "Discoverer": [
                None,
                "Hall",
                "Hall",
                "Galileo",
                "Galileo",
                "Galileo",
                "Galileo",
                "Barnard",
                "Perrine",
                "Herschel",
            ],
        }
    )
    expected_pandas.info()

    expected_polars = pl.from_pandas(expected_pandas)
