"""
Iceberg schema, partition-spec, and sort-order evolution helpers.

Each ``_apply_*`` function reconciles the **current** table state with the
desired definition and applies the minimum set of changes needed.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import VoidTransform
from pyiceberg.types import IcebergType, ListType, MapType, NestedField

if TYPE_CHECKING:
    from .models import ColumnDef, PartitionFieldDef, SortFieldDef

logger = logging.getLogger(__package__)

# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------


def _types_equal_ignoring_ids(a: IcebergType, b: IcebergType) -> bool:
    """
    Compare two :class:`IcebergType` instances structurally.

    Ignores element / key / value IDs that Iceberg assigns internally.
    ``parse_iceberg_type`` creates ``ListType`` and ``MapType`` with
    placeholder IDs (1, 2) whereas the actual table schema uses unique
    IDs.  A plain ``==`` would therefore always report a difference for
    nested types.
    """
    if type(a) is not type(b):
        return False
    if isinstance(a, ListType) and isinstance(b, ListType):
        return (
            a.element_required == b.element_required and _types_equal_ignoring_ids(a.element_type, b.element_type)  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
        )
    if isinstance(a, MapType) and isinstance(b, MapType):
        return (
            a.value_required == b.value_required
            and _types_equal_ignoring_ids(a.key_type, b.key_type)  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
            and _types_equal_ignoring_ids(a.value_type, b.value_type)  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
        )
    return a == b


def apply_schema(table: Table, columns: list[ColumnDef]) -> None:
    """
    Reconcile the table's current Iceberg schema with the desired column list.

    The desired column list is the **source of truth**:

    * New columns are **added**.
    * Type changes are **applied** (the Iceberg spec allows certain promotions;
      pyiceberg will raise if a change is invalid).
    * ``required`` / ``optional`` transitions are applied.
    * Documentation is updated.
    * Columns present in the table but **absent** from *columns* are **deleted**.
    """
    current_fields = {field.name: field for field in table.schema().fields}
    desired_names: set[str] = set()

    with table.update_schema() as update:
        for col in columns:
            desired_names.add(col.name)
            col_type = col.iceberg_type

            if col.name not in current_fields:
                logger.info("Adding column %s (%s)", col.name, col.type)
                update.add_column(col.name, col_type, doc=col.doc, required=col.required)
            else:
                existing = current_fields[col.name]

                # Adjust nullability
                if not col.required and existing.required:
                    logger.info("Making column %s optional", col.name)
                    update.make_column_optional(col.name)
                elif col.required and not existing.required:
                    raise ValueError(
                        f"Cannot make existing optional column '{col.name}' required: "
                        "pyiceberg does not support promoting an optional column to "
                        "required. Remove the column and re-add it as required, or "
                        "change the schema definition to keep it optional."
                    )

                update_kwargs: dict[str, Any] = {}

                if col.doc != existing.doc:
                    logger.info("Updating doc for column %s", col.name)
                    # pyiceberg treats doc=None as "no change", so to
                    # clear an existing doc we pass an empty string.
                    update_kwargs["doc"] = col.doc if col.doc is not None else ""

                if not _types_equal_ignoring_ids(col_type, existing.field_type):
                    logger.info(
                        "Updating type of column %s: %s → %s",
                        col.name,
                        existing.field_type,
                        col_type,
                    )
                    update_kwargs["field_type"] = col_type

                if update_kwargs:
                    update.update_column(col.name, **update_kwargs)

        # Remove columns not in the desired schema
        for name in current_fields:
            if name not in desired_names:
                logger.info("Deleting column %s (absent from desired schema)", name)
                update.delete_column(name)


def cleanup_stale_references(table: Table, desired_column_names: set[str]) -> None:
    """
    Remove partition and sort order references to columns about to be deleted.

    Must run **before** :func:`apply_schema` so that column deletions do not
    conflict with existing partition-spec or sort-order references.
    """
    current_column_names = {f.name for f in table.schema().fields}
    columns_being_removed = current_column_names - desired_column_names

    if not columns_being_removed:
        return

    # Build source-id -> column-name lookup from the *current* schema.
    id_to_name: dict[int, str] = {f.field_id: f.name for f in table.schema().fields}

    # --- Partition fields referencing removed columns ---
    stale_partition_names = [
        f.name
        for f in table.spec().fields
        if not isinstance(f.transform, VoidTransform)  # pyright: ignore[reportUnknownMemberType]
        and id_to_name.get(f.source_id) in columns_being_removed
    ]
    if stale_partition_names:
        with table.update_spec() as spec_update:
            for name in stale_partition_names:
                logger.info(
                    "Pre-cleanup: removing partition field %s (source column being deleted)",
                    name,
                )
                spec_update.remove_field(name)

    # --- Sort order referencing removed columns ---
    current_sort = table.sort_order()
    has_stale_sort = any(id_to_name.get(sf.source_id) in columns_being_removed for sf in current_sort.fields)
    if has_stale_sort:
        logger.info("Pre-cleanup: clearing sort order (references columns being deleted)")
        with table.update_sort_order() as _builder:
            pass  # Empty builder -> unsorted


# ---------------------------------------------------------------------------
# Partition spec evolution
# ---------------------------------------------------------------------------


def _partition_spec_matches(table: Table, partition_spec: list[PartitionFieldDef]) -> bool:
    """Return ``True`` if the table's current partition spec already matches *partition_spec*."""
    current_fields = {
        f.name: f
        for f in table.spec().fields
        if not isinstance(f.transform, VoidTransform)  # pyright: ignore[reportUnknownMemberType]
    }

    desired_names = {part.resolved_name for part in partition_spec}

    if set(current_fields.keys()) != desired_names:
        return False

    if not partition_spec:
        return True

    name_to_id = {field.name: field.field_id for field in table.schema().fields}

    for part in partition_spec:
        name = part.resolved_name
        existing = current_fields[name]
        desired_source_id = name_to_id.get(part.source_column)
        if (
            desired_source_id is None
            or existing.source_id != desired_source_id
            or existing.transform != part.iceberg_transform  # pyright: ignore[reportUnknownMemberType]
        ):
            return False

    return True


def apply_partition_spec(table: Table, partition_spec: list[PartitionFieldDef]) -> None:
    """
    Reconcile the table's partition spec with the desired definition.

    * Missing partition fields are **added**.
    * Partition fields present in the table but absent from *partition_spec*
      are **removed** (voided).
    """
    if _partition_spec_matches(table, partition_spec):
        logger.info("Partition spec already matches - skipping update")
        return

    current_fields = {
        f.name: f
        for f in table.spec().fields
        if not isinstance(f.transform, VoidTransform)  # pyright: ignore[reportUnknownMemberType]
    }
    desired_names: set[str] = set()

    with table.update_spec() as spec_update:
        # Add new partition fields / update transform if changed
        for part in partition_spec:
            transform = part.iceberg_transform
            name = part.resolved_name
            desired_names.add(name)

            if name not in current_fields:
                logger.info(
                    "Adding partition field %s (%s) on column %s",
                    name,
                    part.transform,
                    part.source_column,
                )
                spec_update.add_field(
                    source_column_name=part.source_column,
                    transform=transform,
                    partition_field_name=name,
                )
            else:
                existing = current_fields[name]
                new_source_id = table.schema().find_field(part.source_column).field_id

                # Check if either the source column or the transform has changed.
                # If so, we have to remove and re-add the field
                source_changed = existing.source_id != new_source_id
                transform_changed = existing.transform != transform  # pyright: ignore[reportUnknownMemberType]

                if source_changed or transform_changed:
                    logger.info(
                        "Transform changed for partition field %s: %s -> %s; removing and re-adding",
                        name,
                        str(existing.transform),  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
                        transform,
                    )
                    spec_update.remove_field(name)
                    spec_update.add_field(
                        source_column_name=part.source_column,
                        transform=transform,
                        partition_field_name=name,
                    )

        # Remove partition fields not in the desired spec
        for name in current_fields:
            if name not in desired_names:
                logger.info("Removing partition field %s", name)
                spec_update.remove_field(name)


# ---------------------------------------------------------------------------
# Sort order evolution
# ---------------------------------------------------------------------------


def _sort_order_matches(table: Table, sort_order: list[SortFieldDef]) -> bool:
    """Return ``True`` if the table's current sort order already matches *sort_order*."""
    current = table.sort_order()

    if len(current.fields) != len(sort_order):
        return False

    if not sort_order:
        # Both are empty / unsorted.
        return True

    # Build a name -> field-id lookup from the current schema.
    name_to_id = {field.name: field.field_id for field in table.schema().fields}

    for current_field, desired in zip(current.fields, sort_order, strict=True):
        desired_id = name_to_id.get(desired.source_column)
        if (
            desired_id is None
            or current_field.source_id != desired_id
            or current_field.direction != desired.sort_direction
            or current_field.transform != desired.iceberg_transform  # pyright: ignore[reportUnknownMemberType]
            or current_field.null_order != desired.iceberg_null_order
        ):
            return False

    return True


def apply_sort_order(table: Table, sort_order: list[SortFieldDef]) -> None:
    """
    Replace the table's sort order with the desired definition.

    If *sort_order* is empty the table is set to unsorted — Iceberg will record
    a new empty sort order entry since sort orders are immutable and append-only.
    The sort order is only replaced when it differs from the current state.
    """
    if _sort_order_matches(table, sort_order):
        logger.info("Sort order already matches - skipping update")
        return

    with table.update_sort_order() as builder:
        for sf in sort_order:
            if sf.direction == "asc":
                builder.asc(
                    sf.source_column,
                    transform=sf.iceberg_transform,
                    null_order=sf.iceberg_null_order,
                )
            else:
                builder.desc(
                    sf.source_column,
                    transform=sf.iceberg_transform,
                    null_order=sf.iceberg_null_order,
                )

    logger.info("Sort order applied")


# ---------------------------------------------------------------------------
# Initial schema builder
# ---------------------------------------------------------------------------


def build_initial_schema(columns: list[ColumnDef]) -> Schema:
    """
    Build a pyiceberg :class:`Schema` from column definitions.

    Used when creating a brand-new table via ``catalog.create_table()``.
    Field IDs are placeholders — ``create_table`` calls
    ``assign_fresh_schema_ids`` internally before sending to the server.
    """
    return Schema(
        *(
            NestedField(
                field_id=i,
                name=col.name,
                field_type=col.iceberg_type,
                required=col.required,
                doc=col.doc,
            )
            for i, col in enumerate(columns, 1)
        )
    )
