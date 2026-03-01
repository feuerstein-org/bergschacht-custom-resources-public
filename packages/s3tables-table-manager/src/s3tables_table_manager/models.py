"""
Pydantic models for the Iceberg schema definition.

These models represent the ``Schema`` JSON payload passed via CloudFormation
``ResourceProperties`` and provide validation + convenience accessors.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pyiceberg.table.sorting import NullOrder, SortDirection
from pyiceberg.transforms import IdentityTransform, Transform
from pyiceberg.types import IcebergType

from .types import (
    IcebergTypeStr,
    TransformStr,
    parse_iceberg_type,
    parse_transform,
)


class ColumnDef(BaseModel, frozen=True):
    """A single column in the desired Iceberg schema."""

    name: str
    type: IcebergTypeStr
    required: bool = False
    doc: str | None = None

    @property
    def iceberg_type(self) -> IcebergType:
        """Return the parsed :class:`IcebergType`."""
        return parse_iceberg_type(self.type)


class PartitionFieldDef(BaseModel, frozen=True):
    """A single partition field in the desired partition spec."""

    model_config = ConfigDict(validate_by_name=True)

    source_column: str = Field(validation_alias="sourceColumn")
    transform: TransformStr
    name: str | None = None

    @property
    def resolved_name(self) -> str:
        """Return the explicit name or generate one from source + transform."""
        if self.name:
            return self.name
        # Sanitise transform text so names like ``bucket[16]`` become
        # ``bucket_16`` - brackets can cause issues with some tooling.
        sanitised = self.transform.replace("[", "_").replace("]", "")
        return f"{self.source_column}_{sanitised}"

    @property
    def iceberg_transform(self) -> Transform[Any, Any]:
        """Return the parsed :class:`Transform`."""
        return parse_transform(self.transform)


class SortFieldDef(BaseModel, frozen=True):
    """A single field in the desired sort order."""

    model_config = ConfigDict(validate_by_name=True)

    source_column: str = Field(validation_alias="sourceColumn")
    direction: Literal["asc", "desc"] = "asc"
    transform: TransformStr | None = None
    null_order: Literal["nulls-first", "nulls-last"] = Field("nulls-last", validation_alias="nullOrder")

    @property
    def iceberg_transform(self) -> Transform[Any, Any]:
        """Return the parsed :class:`Transform` (defaults to identity)."""
        return parse_transform(self.transform) if self.transform else IdentityTransform()

    @property
    def sort_direction(self) -> SortDirection:
        """Map the string literal to :class:`SortDirection`."""
        return SortDirection.ASC if self.direction == "asc" else SortDirection.DESC

    @property
    def iceberg_null_order(self) -> NullOrder:
        """Map the string literal to :class:`NullOrder`."""
        return NullOrder.NULLS_FIRST if self.null_order == "nulls-first" else NullOrder.NULLS_LAST


class IcebergSchemaDefinition(BaseModel, frozen=True):
    """
    Top-level schema definition passed via the ``Schema`` resource property.

    Cross-field validation ensures partition spec and sort order only reference
    columns that exist in the schema.
    """

    model_config = ConfigDict(validate_by_name=True)

    columns: list[ColumnDef]
    partition_spec: Annotated[
        list[PartitionFieldDef],
        Field(default_factory=list, validation_alias="partitionSpec"),
    ]
    sort_order: Annotated[
        list[SortFieldDef],
        Field(default_factory=list, validation_alias="sortOrder"),
    ]

    @model_validator(mode="after")
    def _validate_references(self) -> Self:
        column_names = {col.name for col in self.columns}
        errors = [
            f"Partition field references unknown column '{part.source_column}'"
            for part in self.partition_spec
            if part.source_column not in column_names
        ] + [
            f"Sort field references unknown column '{sf.source_column}'"
            for sf in self.sort_order
            if sf.source_column not in column_names
        ]
        if errors:
            raise ValueError("; ".join(errors))
        return self
