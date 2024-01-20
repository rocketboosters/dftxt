import decimal

if __name__ == "io_scenario_test":
    import pandas as pd
    import polars as pl

    expected_pandas = pd.DataFrame(
        {
            "Country": [
                "Argentina",
                "Brazil",
                "Canada",
                "Cayman Islands",
                "Australia",
                "China",
                "Euro Zone",
            ],
            "Currency": [
                "Peso",
                "Real",
                "Dollar",
                "Dollar",
                "Dollar",
                "Yuan",
                "Euro",
            ],
            pd.Timestamp(2023, 1, 1).date(): [
                decimal.Decimal("296.154"),
                decimal.Decimal("4.994"),
                decimal.Decimal("1.350"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.506"),
                decimal.Decimal("7.075"),
                decimal.Decimal("0.924"),
            ],
            pd.Timestamp(2022, 1, 1).date(): [
                decimal.Decimal("130.792"),
                decimal.Decimal("5.165"),
                decimal.Decimal("1.301"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.442"),
                decimal.Decimal("6.730"),
                decimal.Decimal("0.951"),
            ],
            pd.Timestamp(2021, 1, 1).date(): [
                decimal.Decimal("95.098"),
                decimal.Decimal("5.395"),
                decimal.Decimal("1.254"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.332"),
                decimal.Decimal("6.452"),
                decimal.Decimal("0.846"),
            ],
            pd.Timestamp(2020, 1, 1).date(): [
                decimal.Decimal("70.635"),
                decimal.Decimal("5.151"),
                decimal.Decimal("1.341"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.452"),
                decimal.Decimal("6.900"),
                decimal.Decimal("0.877"),
            ],
            pd.Timestamp(2019, 1, 1).date(): [
                decimal.Decimal("48.192"),
                decimal.Decimal("3.946"),
                decimal.Decimal("1.327"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.439"),
                decimal.Decimal("6.910"),
                decimal.Decimal("0.893"),
            ],
        }
    )

    expected_polars = pl.DataFrame(
        {
            "Country": [
                "Argentina",
                "Brazil",
                "Canada",
                "Cayman Islands",
                "Australia",
                "China",
                "Euro Zone",
            ],
            "Currency": [
                "Peso",
                "Real",
                "Dollar",
                "Dollar",
                "Dollar",
                "Yuan",
                "Euro",
            ],
            "2023-01-01": [
                decimal.Decimal("296.154"),
                decimal.Decimal("4.994"),
                decimal.Decimal("1.350"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.506"),
                decimal.Decimal("7.075"),
                decimal.Decimal("0.924"),
            ],
            "2022-01-01": [
                decimal.Decimal("130.792"),
                decimal.Decimal("5.165"),
                decimal.Decimal("1.301"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.442"),
                decimal.Decimal("6.730"),
                decimal.Decimal("0.951"),
            ],
            "2021-01-01": [
                decimal.Decimal("95.098"),
                decimal.Decimal("5.395"),
                decimal.Decimal("1.254"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.332"),
                decimal.Decimal("6.452"),
                decimal.Decimal("0.846"),
            ],
            "2020-01-01": [
                decimal.Decimal("70.635"),
                decimal.Decimal("5.151"),
                decimal.Decimal("1.341"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.452"),
                decimal.Decimal("6.900"),
                decimal.Decimal("0.877"),
            ],
            "2019-01-01": [
                decimal.Decimal("48.192"),
                decimal.Decimal("3.946"),
                decimal.Decimal("1.327"),
                decimal.Decimal("0.833"),
                decimal.Decimal("1.439"),
                decimal.Decimal("6.910"),
                decimal.Decimal("0.893"),
            ],
        }
    )
