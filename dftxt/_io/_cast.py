import datetime
import decimal
import re
import typing

import pytz

if typing.TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    import polars as pl
else:
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        pd = None  # type: ignore

    try:
        import polars as pl
    except ImportError:  # pragma: no cover
        pl = None  # type: ignore

_PANDAS_DTYPES = {"date": "object"}

_CATEGORICAL_DTYPES = {
    "cat": False,
    "category": False,
    "categorical": False,
    "nom": False,
    "nominal": False,
    "enum": True,
    "enumerated": True,
    "ord": True,
    "ordinal": True,
    "ordered": True,
    "order": True,
}


def _get_categorical_ordering(dftxt_data_type: str, values: typing.List[typing.Any]):
    """Convert dftxt categorical dtype into stored category ordering."""
    distinct = set(values)
    available_indexed = [(values.index(v), v) for v in distinct]
    raw = dftxt_data_type.split(":", 1)[-1]
    if raw in ("az", "abc"):
        return [x[1] for x in sorted(available_indexed, key=lambda v: v[1])]

    if raw in ("za", "cba"):
        return [
            x[1] for x in sorted(available_indexed, key=lambda v: v[1], reverse=True)
        ]

    raw_ordering = list(raw) if "," not in raw else raw.split(",")
    indexes = [
        int(v.strip()) if v.strip().isdigit() else v.strip() for v in raw_ordering
    ]
    appearance_ordered = list(sorted(available_indexed, key=lambda v: v[0]))
    return [appearance_ordered[i][1] if isinstance(i, int) else i for i in indexes]


def _encode_categorical_ordering(
    order: typing.List[typing.Any], values: typing.List[typing.Any]
) -> str:
    """Serialize categorical ordering for preservation in dftxt outputs."""
    distinct = set(values)
    defined = set(order)

    has_all = distinct == defined

    if has_all and order == list(sorted(order)):
        return "az"
    if has_all and order == list(sorted(order, reverse=True)):
        return "za"

    delimiter = "" if has_all and len(distinct) < 10 else ","

    available_indexed = [(values.index(v), v) for v in distinct]
    physical_ordered = [v[1] for v in sorted(available_indexed, key=lambda v: v[0])]
    if order == physical_ordered:
        return ""
    return delimiter.join(
        [str(physical_ordered.index(v)) if v in distinct else v for v in order]
    )


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

    categorical_dtypes = list(_CATEGORICAL_DTYPES.keys())
    if dt in categorical_dtypes:
        return "category"

    if dt.startswith(tuple([f"{v}:" for v in categorical_dtypes])):
        return pd.CategoricalDtype(
            categories=_get_categorical_ordering(data_type or "", values),
            ordered=dt.startswith(("enum", "ord")),
        )

    return _PANDAS_DTYPES.get(data_type or "", data_type)


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
    is_categorical = prefix in _CATEGORICAL_DTYPES
    is_enum = is_categorical and _CATEGORICAL_DTYPES[prefix]
    if is_categorical and not is_enum:
        return pl.Categorical(
            "lexical" if dtype.endswith((":az", ":abc")) else "physical"
        )

    if is_enum:
        return pl.Enum(_get_categorical_ordering(data_type or "", values))

    return None


def to_decimal(value: typing.Any, data_type: str) -> decimal.Decimal:
    """Convert dftxt-serialized value into the decimal data type."""
    return decimal.Decimal(value)


def to_boolean(value: typing.Optional[str]) -> bool:
    """Convert dftxt-serialized value into the boolean data type."""
    if value is None:
        return False
    return value.strip().lower() in ["true", "yes", "1", "on", "y", "t"]


def cast_to(value: typing.Any, data_type: str) -> typing.Any:
    """Cast dftxt-serialized value to the specified data type."""
    if value is None or value in ["None", "NA", "NAN", "NAT", "null"]:
        return None

    dtype = data_type or ""
    dt = dtype.lower()

    if dt.startswith("str"):
        return str(value)

    if dt.startswith("int"):
        return int(value)

    if dt.startswith("float"):
        return float(value)

    if dt.startswith("decimal"):
        return to_decimal(value, data_type)

    if dt in ("bool", "boolean"):
        return to_boolean(value)

    if dt == "date":
        return datetime.date.fromisoformat(value)

    if dt in ("timestamp", "datetime", "datetime64"):
        return datetime.datetime.fromisoformat(value)

    if dt.startswith(("timestamp[", "datetime[", "datetime64[")):
        time_zone = pytz.timezone(
            data_type.split("[")[-1].strip().split(",")[-1].strip().split("]")[0]
        )
        return datetime.datetime.fromisoformat(value).replace(tzinfo=time_zone)

    return value


def cast_from(
    value: typing.Any,
    data_type: typing.Optional[str],
    series_data_type: typing.Any = None,
) -> str:
    """Serialize to the dftxt string representation."""
    if (pd is not None and pd.isna(value)) or value is None:
        return "None"

    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")

    if pd is not None and "datetime64" in str(type(value)):
        match = re.compile(r",\s*(?P<tz>[^\]]+)]$").search(str(series_data_type) or "")
        time_zone = match.group("tz") if match else None
        return pd.Timestamp(value, tz=time_zone).isoformat().replace("+00:00", "Z")

    return str(value)


def from_value(value: typing.Any) -> typing.Optional[str]:
    """Identify the dftxt data type from the specified value."""
    if isinstance(value, datetime.date) and not hasattr(value, "hour"):
        return "date"

    if isinstance(value, datetime.date):
        return "datetime"

    if isinstance(value, decimal.Decimal):
        return "decimal"

    if isinstance(value, int):
        return "int"

    if isinstance(value, float):
        return "float"

    if isinstance(value, str):
        return "str"

    return None


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
        ordering = _encode_categorical_ordering(
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

    return from_value(value) or dtype


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
        ordering = _encode_categorical_ordering(
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
