import typing

from . import _categorical

if typing.TYPE_CHECKING:  # pragma: no cover
    import polars as pl
else:
    try:
        import polars as pl
    except ImportError:  # pragma: no cover
        pl = None  # type: ignore


def to_polars_dtype(
    data_type: typing.Optional[str],
    values: typing.List[typing.Any],
) -> typing.Optional["pl.PolarsDataType"]:
    """Convert a dftxt data type into the associated Polars data type."""
    dtype = (data_type or "").lower()

    if dtype in ("str", ""):
        return pl.Utf8()

    if dtype in ("float", "float64"):
        return pl.Float64()

    if dtype in ("int", "int64"):
        return pl.Int64()

    prefix = dtype.split(":", 1)[0]
    is_categorical = prefix in _categorical.CATEGORICAL_DTYPES
    is_enum = is_categorical and _categorical.CATEGORICAL_DTYPES[prefix]
    if is_categorical and not is_enum:
        return pl.Categorical(
            "lexical" if dtype.endswith((":az", ":abc")) else "physical"
        )

    if is_enum:
        return pl.Enum(_categorical.get_categorical_ordering(data_type or "", values))

    return None


def from_polars(series: "pl.Series") -> typing.Optional[str]:
    """Determine dftxt dtype from a Polars Series column."""
    if pl is None:
        return "str"

    dtype = str(series.dtype).lower()
    if dtype == "utf8":
        return "str"

    if isinstance(series.dtype, pl.Categorical):
        return "category"

    if isinstance(series.dtype, pl.Enum):
        ordering = _categorical.encode_categorical_ordering(
            series.dtype.categories.to_list(), series.to_list()
        )
        separator = ":" if ordering else ""
        return f"enum{separator}{ordering}"

    if dtype == "categorical":
        return "category"

    if dtype == "int64":
        return "int"

    if dtype == "float64":
        return "float"

    if dtype.startswith("datetime"):
        return "datetime"

    if dtype.startswith("decimal"):
        return "decimal"

    return dtype
