if __name__ == "io_scenario_test":
    import pandas as pd
    import polars as pl

    rows = [
        {
            "Play": "Hamlet",
            "Quotation": "To be, or not to be: that is the question",
            "Act": 3,
            "Scene": 1,
        },
        {
            "Play": "As You Like It",
            "Quotation": "All the world's a stage, and all the men "
            + "and women merely players. They have their "
            + "exits and their entrances; And one man in "
            + "his time plays many parts.",
            "Act": 2,
            "Scene": 7,
        },
        {
            "Play": "Romeo & Juliet",
            "Quotation": "Romeo, Romeo! Wherefore art thou Romeo?",
            "Act": 2,
            "Scene": 2,
        },
        {
            "Play": "Richard III",
            "Quotation": "Now is the winter of our discontent",
            "Act": 1,
            "Scene": 1,
        },
        {
            "Play": "Macbeth",
            "Quotation": "Is this a dagger which I see before me, "
            + "the handle toward my hand?",
            "Act": 2,
            "Scene": 1,
        },
    ]

    expected_pandas = pd.DataFrame(rows)
    expected_polars = pl.DataFrame(rows)
