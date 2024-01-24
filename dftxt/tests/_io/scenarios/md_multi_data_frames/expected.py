if __name__ == "io_scenario_test":
    import pandas as pd
    import polars as pl

    expected_pandas_people = pd.DataFrame(
        {
            "Name": ["Jane Doe", "John Doe", "Anna Johnson", "Steve Simpson"],
            "Birth Month": ["February", "April", "November", "June"],
            "Favorite Movie": [
                "Back to the Future",
                "Harry Potter & the Goblet of Fire",
                "Frozen",
                "Avengers: Infinity War",
            ],
            "Favorite Book": [
                "Emma",
                "Harry Potter & the Goblet of Fire",
                "The Girl with the Dragon Tattoo",
                "The Three Musketeers",
            ],
        }
    )
    expected_polars_people = pl.from_pandas(expected_pandas_people)

    expected_pandas_books = pd.DataFrame(
        {
            "Name": [
                "The Three Musketeers",
                "The Girl with the Dragon Tattoo",
                "Harry Potter & the Goblet of Fire",
                "Emma",
            ],
            "Published": [1844, 2005, 2000, 1816],
            "Page Count": [700, 544, 636, 1036],
        }
    )
    expected_polars_books = pl.from_pandas(expected_pandas_books)
