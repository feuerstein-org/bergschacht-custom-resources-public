"""
CloudFormation Custom Resource handler for managing Iceberg tables on S3 Tables.

This custom resource **owns the full table lifecycle**: it creates tables on
``Create``, evolves schema / partition spec / sort order on ``Update``, and
drops the table on ``Delete``.

The CDK stack should manage the **table bucket** and **namespace** via native
CloudFormation resources (``AWS::S3Tables::TableBucket`` and
``AWS::S3Tables::Namespace``).  The table itself must **not** be declared as
an ``AWS::S3Tables::Table`` resource - this custom resource replaces it.

The CDK schema definition is the **source of truth**.  The handler applies
whatever changes are needed to make the table match the desired state:
new columns are added, types are updated, documentation is set, required /
optional is adjusted, and columns absent from the definition are **removed**.
Partition specs and sort orders are likewise fully reconciled.

Expected ``ResourceProperties``::

    {
        "TableBucketArn": "arn:aws:s3tables:...:bucket/...",
        "Namespace": "my_namespace",
        "TableName": "my_table",
        "Schema": "<JSON string of IcebergSchemaDefinition>"
    }

Returned ``Data``::

    {
        "SchemaId": "0",
        "TableArn": "arn:aws:s3tables:...:bucket/.../table/<uuid>",
        "TableName": "my_table",
        "Namespace": "my_namespace"
    }

The ``Schema`` JSON follows the same structure defined in the CDK companion
module ``lib/infrastructure/iceberg-schema.ts``.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import boto3
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

from .evolution import (
    apply_partition_spec,
    apply_schema,
    apply_sort_order,
    build_initial_schema,
    cleanup_stale_references,
)
from .models import IcebergSchemaDefinition

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__package__)


# ---------------------------------------------------------------------------
# Table ARN helper
# ---------------------------------------------------------------------------


def _get_table_arn(
    table_bucket_arn: str,
    namespace: str,
    table_name: str,
    region: str,
) -> str:
    """Fetch the real table ARN (contains a UUID) from the S3 Tables control plane."""
    client = boto3.client("s3tables", region_name=region)  # pyright: ignore[reportUnknownMemberType]
    resp = client.get_table(
        tableBucketARN=table_bucket_arn,
        namespace=namespace,
        name=table_name,
    )
    return resp["tableARN"]


# ---------------------------------------------------------------------------
# Catalog helper
# ---------------------------------------------------------------------------


def _get_catalog(table_bucket_arn: str, region: str) -> Catalog:
    """Return a pyiceberg REST catalog pointed at S3 Tables."""
    return load_catalog(
        "s3tables",
        **{
            "type": "rest",
            "uri": f"https://s3tables.{region}.amazonaws.com/iceberg",
            "warehouse": table_bucket_arn,
            "rest.sigv4-enabled": "true",
            "rest.signing-name": "s3tables",
            "rest.signing-region": region,
        },
    )


# ---------------------------------------------------------------------------
# CloudFormation Custom Resource entry point
# ---------------------------------------------------------------------------


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    """
    CDK Provider-framework ``onEvent`` handler.

    Returns a dict consumed by the CDK custom-resource provider framework:
    ``PhysicalResourceId`` and optional ``Data``.
    """
    request_type: str = event["RequestType"]
    props: dict[str, Any] = event["ResourceProperties"]

    table_bucket_arn: str = props["TableBucketArn"]
    namespace: str = props["Namespace"]
    table_name: str = props["TableName"]

    physical_id = f"{table_bucket_arn}|{namespace}|{table_name}"

    logger.info(
        "TableManager %s for %s.%s (bucket=%s)",
        request_type,
        namespace,
        table_name,
        table_bucket_arn,
    )

    if request_type == "Delete":
        # During a DELETE (including rollback of a failed CREATE), CloudFormation passes the physical ID.
        existing_physical_id: str = event.get("PhysicalResourceId", physical_id)
        region = os.environ["AWS_REGION"]
        catalog = _get_catalog(table_bucket_arn, region)
        table_identifier = f"{namespace}.{table_name}"
        try:
            catalog.purge_table(table_identifier)
            logger.info("Dropped & purged table %s", table_identifier)
        except NoSuchTableError:
            logger.warning(
                "Table %s not found (may already be gone)",
                table_identifier,
            )
        return {"PhysicalResourceId": existing_physical_id}

    # --- Create / Update -------------------------------------------------
    region = os.environ["AWS_REGION"]

    schema = IcebergSchemaDefinition.model_validate_json(props["Schema"])

    catalog = _get_catalog(table_bucket_arn, region)
    table_identifier = f"{namespace}.{table_name}"

    if request_type == "Create":
        initial_schema = build_initial_schema(schema.columns)
        logger.info("Creating table %s with %d columns", table_identifier, len(schema.columns))
        table = catalog.create_table(table_identifier, schema=initial_schema)  # pyright: ignore[reportUnknownMemberType]

        with table.transaction() as txn:
            apply_partition_spec(txn, schema.partition_spec)
            apply_sort_order(txn, schema.sort_order)

    else:
        table = catalog.load_table(table_identifier)
        with table.transaction() as txn:
            cleanup_stale_references(txn, {col.name for col in schema.columns})
            apply_schema(txn, schema.columns)
            apply_partition_spec(txn, schema.partition_spec)
            apply_sort_order(txn, schema.sort_order)

    schema_id = table.schema().schema_id
    table_arn = _get_table_arn(table_bucket_arn, namespace, table_name, region)

    logger.info("Schema applied successfully (schema_id=%s, arn=%s)", schema_id, table_arn)

    return {
        "PhysicalResourceId": physical_id,
        "Data": {
            "SchemaId": str(schema_id),
            "TableArn": table_arn,
            "TableName": table_name,
            "Namespace": namespace,
        },
    }
