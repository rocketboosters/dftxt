import typing

from . import _categorical
from . import _common

if typing.TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
else:
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        pd = None  # type: ignore

_PANDAS_DTYPES = {"date": "object"}


def to_pandas_dtype(
    data_type: typing.Optional[str], values: typing.List[typing.Any]
) -> typing.Union[None, str, "pd.CategoricalDtype"]:
    """Convert a dftxt data type into the associated Pandas data type."""
    dtype = data_type or ""
    dt = dtype.lower()
    if dt.startswith("decimal"):
        return "object"
    if dt in ("timestamp", "datetime", "datetime64"):
        return None

    if dt in ("int", "int64"):
        return "Int64" if dtype.startswith("Int") else "int64"

    if dt.startswith("str") or dt == "":
        return "object"

    categorical_dtypes = list(_categorical.CATEGORICAL_DTYPES.keys())
    if dt in categorical_dtypes:
        return "category"

    if dt.startswith(tuple([f"{v}:" for v in categorical_dtypes])):
        return pd.CategoricalDtype(
            categories=_categorical.get_categorical_ordering(data_type or "", values),
            ordered=dt.startswith(("enum", "ord")),
        )

    return _PANDAS_DTYPES.get(data_type or "", data_type)


def from_pandas(series: "pd.Series") -> typing.Optional[str]:
    """Determine dftxt dtype from a Pandas Series column."""
    dtype = str(series.dtype)
    index = series.first_valid_index()
    if index is None and dtype == "object":
        return None

    if index is None:
        return dtype

    if isinstance(series.dtype, pd.CategoricalDtype):
        prefix = "ordinal" if series.dtype.ordered else "category"
        ordering = _categorical.encode_categorical_ordering(
            series.dtype.categories.tolist(), series.tolist()
        )
        separator = ":" if ordering else ""
        return f"{prefix}{separator}{ordering}"

    if dtype == "category":
        return "category"

    value = series.loc[index]
    if dtype in ("int64", "int", "Int", "Int64"):
        return "Int" if dtype[0] == "I" else "int"

    if dtype.lower() == "float64":
        return "float"

    return _common.from_value(value) or dtype
