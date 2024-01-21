import decimal

if __name__ == "io_scenario_test":
    import pandas as pd
    import polars as pl

    handbags = {
        "sku": [123, 124, 125],
        "price": [
            decimal.Decimal("1689.99"),
            decimal.Decimal("1490.00"),
            decimal.Decimal("3200.00"),
        ],
        "sale_price": [
            decimal.Decimal("1497.00"),
            decimal.Decimal("1366.97"),
            decimal.Decimal("3079.97"),
        ],
        "brand": ["Saint Below", "Cobo Tau", "Saint Below"],
        "name": [
            "Lala Leather Camera Bag",
            "Cali Calfskin Leather Tote",
            "Envelope Calfskin Shoulder Bag",
        ],
    }
    expected_pandas_handbags = pd.DataFrame(handbags)
    expected_polars_handbags = pl.DataFrame(handbags)

    shoes = {
        "sku": [210, 211],
        "price": [
            decimal.Decimal("220.00"),
            decimal.Decimal("190.00"),
        ],
        "sale_price": [
            decimal.Decimal("140.00"),
            decimal.Decimal("159.99"),
        ],
        "brand": ["AM Sedel", "Fad by Zelmy"],
        "name": [
            "Bianka Slingback Pump",
            "Diva Kitten Heel Pointed Toe Pump",
        ],
    }
    expected_pandas_shoes = pd.DataFrame(shoes)
    expected_polars_shoes = pl.DataFrame(shoes)

    skirts = {
        "sku": [365, 366, 367, 368, 369],
        "price": [
            decimal.Decimal("155.00"),
            decimal.Decimal("99.00"),
            decimal.Decimal("43.00"),
            decimal.Decimal("188.00"),
            decimal.Decimal("295.00"),
        ],
        "sale_price": [
            decimal.Decimal("135.00"),
            decimal.Decimal("59.00"),
            decimal.Decimal("30.10"),
            decimal.Decimal("94.00"),
            decimal.Decimal("177.00"),
        ],
        "brand": [
            "Wing Fall",
            "Fedam",
            "Gong Lab",
            "Reform",
            "Reform",
        ],
        "name": [
            "Rib Pencil Skirt",
            "Suited Midi Column Skirt",
            "Faux Leather Miniskirt",
            "Tazz Denim Max Skirt (Galaway)",
            "Side Slit Stretch Velvet Pencil Skirt",
        ],
    }
    expected_pandas_skirts = pd.DataFrame(skirts)
    expected_polars_skirts = pl.DataFrame(skirts)
