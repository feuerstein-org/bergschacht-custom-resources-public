"""
Iceberg schema, partition-spec, and sort-order evolution helpers.

Each ``_apply_*`` function reconciles the **current** table state with the
desired definition, handling no-op cases is done by py-iceberg.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyiceberg.schema import Schema
from pyiceberg.table import Transaction
from pyiceberg.transforms import VoidTransform
from pyiceberg.types import NestedField

if TYPE_CHECKING:
    from .models import ColumnDef, PartitionFieldDef, SortFieldDef

logger = logging.getLogger(__package__)

# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------


def apply_schema(txn: Transaction, columns: list[ColumnDef]) -> None:
    """
    Reconcile the table's current Iceberg schema with the desired column list.

    Pyiceberg will internally handle no-op changes and will not commit them.
    This means we can simply attempt to apply the desired schema.

    The desired column list is the **source of truth**:

    * New columns are **added**.
    * Type changes are **applied** (the Iceberg spec allows certain promotions;
      pyiceberg will raise if a change is invalid).
    * ``required`` / ``optional`` transitions are applied.
    * Documentation is updated.
    * Columns present in the table but **absent** from *columns* are **deleted**.
    """
    current_fields = {field.name: field for field in txn.table_metadata.schema().fields}
    desired_names: set[str] = set()

    with txn.update_schema() as update:
        for col in columns:
            desired_names.add(col.name)
            col_type = col.iceberg_type

            if col.name not in current_fields:
                if col.required:
                    raise ValueError(
                        f"Cannot add new required column '{col.name}' without a default value: "
                        "pyiceberg does not support this operation, and s3tables-table-manager "
                        "does not currently support setting default values."
                    )

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
                        "required without setting a default value, "
                        "this is currently not supported by s3tables-table-manager"
                    )

                # Setting doc to None doesn't clear it, need to explicitly set to empty string
                doc = col.doc if col.doc is not None else ""
                update.update_column(col.name, field_type=col_type, doc=doc)

        # Remove columns not in the desired schema
        for name in current_fields:
            if name not in desired_names:
                logger.info("Deleting column %s (absent from desired schema)", name)
                update.delete_column(name)

    logger.info("Schema staged")


def cleanup_stale_references(txn: Transaction, desired_column_names: set[str]) -> None:
    """
    Remove partition and sort order references to columns about to be deleted.

    Must run **before** :func:`apply_schema` so that column deletions do not
    conflict with existing partition-spec or sort-order references.
    """
    meta = txn.table_metadata
    current_column_names = {f.name for f in meta.schema().fields}
    columns_being_removed = current_column_names - desired_column_names

    if not columns_being_removed:
        return

    # Build source-id -> column-name lookup from the *current* schema.
    id_to_name: dict[int, str] = {f.field_id: f.name for f in meta.schema().fields}

    # Find all partition fields referencing columns being removed
    stale_partition_names = [
        f.name
        for f in meta.spec().fields
        if not isinstance(f.transform, VoidTransform)  # pyright: ignore[reportUnknownMemberType]
        and id_to_name.get(f.source_id) in columns_being_removed
    ]
    if stale_partition_names:
        with txn.update_spec() as spec_update:
            for name in stale_partition_names:
                logger.info(
                    "Pre-cleanup: removing partition field %s (source column being deleted)",
                    name,
                )
                spec_update.remove_field(name)

    # Get the currently sorted field by id
    current_sort = next(so for so in meta.sort_orders if so.order_id == meta.default_sort_order_id)
    has_stale_sort = any(id_to_name.get(sf.source_id) in columns_being_removed for sf in current_sort.fields)
    if has_stale_sort:
        logger.info("Pre-cleanup: clearing sort order (source columns being deleted)")
        with txn.update_sort_order() as _:
            pass  # Empty builder -> unsorted


# ---------------------------------------------------------------------------
# Partition spec evolution
# ---------------------------------------------------------------------------


def apply_partition_spec(txn: Transaction, partition_spec: list[PartitionFieldDef]) -> None:
    """
    Reconcile the table's partition spec with the desired definition.

    * Missing partition fields are **added**.
    * Partition fields present in the table but absent from *partition_spec*
      are **removed** (voided).
    """
    current_fields = {
        f.name: f
        for f in txn.table_metadata.spec().fields
        if not isinstance(f.transform, VoidTransform)  # pyright: ignore[reportUnknownMemberType]
    }
    desired_names: set[str] = set()

    with txn.update_spec() as spec_update:
        for part in partition_spec:
            transform = part.iceberg_transform
            name = part.resolved_name
            desired_names.add(name)

            # Add new partition fields / update transform if changed
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
            else:  # If not...
                existing = current_fields[name]
                new_source_id = txn.table_metadata.schema().find_field(part.source_column).field_id

                # Check if either the source column or the transform has changed.
                # If so, we have to remove and re-add the field, this is because pyiceberg
                # does not support updating the source column or transform of an existing partition field.
                if existing.source_id != new_source_id or existing.transform != transform:  # pyright: ignore[reportUnknownMemberType]
                    logger.info(
                        "Updating partition field %s: removing and re-adding with new source id/transform "
                        "(id=%s, transform=%s)",
                        name,
                        new_source_id,
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

    logger.info("Partition spec staged")


# ---------------------------------------------------------------------------
# Sort order evolution
# ---------------------------------------------------------------------------


def apply_sort_order(txn: Transaction, sort_order: list[SortFieldDef]) -> None:
    """
    Replace the table's sort order with the desired definition.

    If *sort_order* is empty the table is set to unsorted - Iceberg will record
    a new empty sort order entry since sort orders are immutable and append-only.
    The sort order is only replaced when it differs from the current state.
    """
    with txn.update_sort_order() as builder:
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

    logger.info("Sort order staged")


# ---------------------------------------------------------------------------
# Initial schema builder
# ---------------------------------------------------------------------------


def build_initial_schema(columns: list[ColumnDef]) -> Schema:
    """
    Build a pyiceberg :class:`Schema` from column definitions.

    Used when creating a brand-new table via ``catalog.create_table()``.
    Field IDs are placeholders - ``create_table`` calls
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
