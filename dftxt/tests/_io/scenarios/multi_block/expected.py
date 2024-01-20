import datetime
import decimal

if __name__ == "io_scenario_test":
    import pandas as pd
    import polars as pl

    expected_pandas = pd.DataFrame(
        {
            "account_id": ["1", "1", "2", "3", "4"],
            "created_at": [
                pd.Timestamp(2023, 4, 23, 13, 1, 59, tzinfo=datetime.UTC),
                pd.Timestamp(2023, 4, 23, 13, 1, 59, tzinfo=datetime.UTC),
                pd.Timestamp(2023, 10, 1, 1, 21, 0, tzinfo=datetime.UTC),
                pd.Timestamp(2023, 11, 10, 0, 0, 0, tzinfo=datetime.UTC),
                pd.Timestamp(2023, 12, 9, 6, 42, 2, tzinfo=datetime.UTC),
            ],
            "plan_id": ["foo bar", "foo bar", "spam ham", "lorem ipsum", "free"],
            "country": [
                "United States",
                "United States",
                "England",
                "South Korea",
                "Mexico",
            ],
            "converted_on": [
                pd.Timestamp(2023, 4, 23).date(),
                pd.Timestamp(2023, 4, 23).date(),
                pd.Timestamp(2023, 10, 1).date(),
                pd.Timestamp(2023, 11, 10).date(),
                None,
            ],
            "Monthly Revenue": [
                decimal.Decimal("25.490"),
                decimal.Decimal("25.490"),
                decimal.Decimal("99.990"),
                decimal.Decimal("149.990"),
                None,
            ],
            "active": pd.Series([True, True, False, True, None], dtype="boolean"),
        }
    )

    expected_polars = pl.DataFrame(
        {
            "account_id": ["1", "1", "2", "3", "4"],
            "created_at": pl.Series(
                name="created_at",
                values=[
                    datetime.datetime(2023, 4, 23, 13, 1, 59, tzinfo=datetime.UTC),
                    datetime.datetime(2023, 4, 23, 13, 1, 59, tzinfo=datetime.UTC),
                    datetime.datetime(2023, 10, 1, 1, 21, 0, tzinfo=datetime.UTC),
                    datetime.datetime(2023, 11, 10, 0, 0, 0, tzinfo=datetime.UTC),
                    datetime.datetime(2023, 12, 9, 6, 42, 2, tzinfo=datetime.UTC),
                ],
                dtype=pl.Datetime("us", "UTC"),
            ),
            "plan_id": ["foo bar", "foo bar", "spam ham", "lorem ipsum", "free"],
            "country": [
                "United States",
                "United States",
                "England",
                "South Korea",
                "Mexico",
            ],
            "converted_on": [
                pd.Timestamp(2023, 4, 23).date(),
                pd.Timestamp(2023, 4, 23).date(),
                pd.Timestamp(2023, 10, 1).date(),
                pd.Timestamp(2023, 11, 10).date(),
                None,
            ],
            "Monthly Revenue": [
                decimal.Decimal("25.490"),
                decimal.Decimal("25.490"),
                decimal.Decimal("99.990"),
                decimal.Decimal("149.990"),
                None,
            ],
            "active": [True, True, False, True, None],
        }
    )
