import datetime
import decimal
import re
import typing

import pytz

if typing.TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
else:
    try:
        import pandas as pd
    except ImportError:  # pragma: no cover
        pd = None  # type: ignore


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
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))

    if dt.startswith(("timestamp[", "datetime[", "datetime64[")):
        time_zone = pytz.timezone(
            data_type.split("[")[-1].strip().split(",")[-1].strip().split("]")[0]
        )
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).replace(
            tzinfo=time_zone
        )

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
