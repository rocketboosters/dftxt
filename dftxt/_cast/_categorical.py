import typing

CATEGORICAL_DTYPES = {
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


def get_categorical_ordering(dftxt_data_type: str, values: typing.List[typing.Any]):
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


def encode_categorical_ordering(
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
