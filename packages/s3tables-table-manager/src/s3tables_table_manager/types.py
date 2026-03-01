"""
Iceberg type and transform parsing utilities.

Converts string representations (e.g. ``"string"``, ``"decimal(10,2)"``,
``"bucket[16]"``) into their pyiceberg equivalents.
"""

from __future__ import annotations

import re
from typing import Annotated, Any

from pydantic import AfterValidator
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    VoidTransform,
    YearTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    StringType,
    TimestampType,
    TimestamptzType,
    TimeType,
    UUIDType,
)

# ---------------------------------------------------------------------------
# Iceberg type parsing
# ---------------------------------------------------------------------------

_PRIMITIVE_TYPES: dict[str, type[IcebergType]] = {
    "boolean": BooleanType,
    "int": IntegerType,
    "integer": IntegerType,
    "long": LongType,
    "float": FloatType,
    "double": DoubleType,
    "date": DateType,
    "time": TimeType,
    "timestamp": TimestampType,
    "timestamptz": TimestamptzType,
    "string": StringType,
    "uuid": UUIDType,
    "binary": BinaryType,
}


def parse_iceberg_type(type_str: str) -> IcebergType:
    """
    Parse an Iceberg type string into a pyiceberg :class:`IcebergType`.

    Supports all Iceberg primitive types, ``decimal(p,s)``, ``fixed(l)``,
    ``list<element>``, and ``map<key,value>``.

    Raises:
        ValueError: If the type string is not recognised.

    """
    type_str = type_str.strip()
    type_lower = type_str.lower()

    # --- Primitives ---
    if type_lower in _PRIMITIVE_TYPES:
        return _PRIMITIVE_TYPES[type_lower]()

    # --- decimal(precision, scale) ---
    m = re.match(r"decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)", type_lower)
    if m:
        return DecimalType(precision=int(m.group(1)), scale=int(m.group(2)))

    # --- fixed(length) ---
    m = re.match(r"fixed\(\s*(\d+)\s*\)", type_lower)
    if m:
        return FixedType(length=int(m.group(1)))

    # --- list<element_type> ---
    m = re.match(r"list<(.+)>$", type_lower)
    if m:
        element_type = parse_iceberg_type(m.group(1))
        return ListType(element_id=1, element=element_type, element_required=True)

    # --- map<key_type, value_type> ---
    m = re.match(r"map<(.+)>$", type_lower)
    if m:
        key_str, value_str = _split_type_pair(m.group(1))
        return MapType(
            key_id=1,
            key=parse_iceberg_type(key_str),
            key_required=True,
            value_id=2,
            value=parse_iceberg_type(value_str),
            value_required=False,
        )

    raise ValueError(f"Unsupported Iceberg type: {type_str}")


def _split_type_pair(pair: str) -> tuple[str, str]:
    """Split ``'K, V'`` respecting nested angle brackets."""
    depth = 0
    for i, ch in enumerate(pair):
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        elif ch == "," and depth == 0:
            return pair[:i].strip(), pair[i + 1 :].strip()
    raise ValueError(f"Cannot split type pair: {pair}")


# ---------------------------------------------------------------------------
# Transform parsing
# ---------------------------------------------------------------------------

_SIMPLE_TRANSFORMS: dict[str, type[Transform[Any, Any]]] = {
    "identity": IdentityTransform,
    "year": YearTransform,
    "month": MonthTransform,
    "day": DayTransform,
    "hour": HourTransform,
    "void": VoidTransform,
}


def parse_transform(transform_str: str) -> Transform[Any, Any]:
    """Parse strings like ``'day'``, ``'bucket[16]'``, or ``'truncate[10]'``."""
    t = transform_str.strip().lower()

    if t in _SIMPLE_TRANSFORMS:
        return _SIMPLE_TRANSFORMS[t]()  # pyright: ignore[reportCallIssue]  # root has a default in pydantic

    m = re.match(r"bucket\[(\d+)]", t)
    if m:
        return BucketTransform(num_buckets=int(m.group(1)))

    m = re.match(r"truncate\[(\d+)]", t)
    if m:
        return TruncateTransform(width=int(m.group(1)))

    raise ValueError(f"Unsupported transform: {transform_str}")


# ---------------------------------------------------------------------------
# Reusable annotated types for Pydantic models
# ---------------------------------------------------------------------------


def _check_iceberg_type(v: str) -> str:
    """Validate that *v* is a recognised Iceberg type string."""
    parse_iceberg_type(v)
    return v


def _check_transform(v: str) -> str:
    """Validate that *v* is a recognised Iceberg transform string."""
    parse_transform(v)
    return v


IcebergTypeStr = Annotated[str, AfterValidator(_check_iceberg_type)]
"""A ``str`` that must be a valid Iceberg type expression."""

TransformStr = Annotated[str, AfterValidator(_check_transform)]
"""A ``str`` that must be a valid Iceberg transform expression."""
