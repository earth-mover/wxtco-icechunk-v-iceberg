"""Wide table schema: six key columns, one float column per diagnostic."""

from __future__ import annotations

import os
from collections.abc import Sequence

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import FloatType, IntegerType, NestedField, TimestampType

NAMESPACE = "mogreps"
# WXTCO_TABLE_ID names a scratch table for clean ETL timing runs. Namespace stays `mogreps`.
TABLE_ID = os.environ.get("WXTCO_TABLE_ID", f"{NAMESPACE}.surface")
# WXTCO_TABLE_SORT: `member` writes rows member, lat, lon; `hilbert` writes cell-by-cell along a
# Hilbert curve, member innermost, so row-group lat/lon statistics prune regional queries.
SORT = os.environ.get("WXTCO_TABLE_SORT", "member")
if SORT not in ("member", "hilbert"):
    raise ValueError(f"WXTCO_TABLE_SORT={SORT!r}; expected member or hilbert")
KEY_COLUMNS = ("init_time", "lead_hours", "valid_time", "member", "lat", "lon")
TABLE_PROPERTIES = {
    "write.parquet.compression-codec": "zstd",
    # pyiceberg 0.12 ignores row-group-size-bytes. 380000 rows x ~356 B (6 keys + 81 floats) is ~128 MiB.
    "write.parquet.row-group-limit": "380000",
    "write.target-file-size-bytes": str(2 * 1024 * 1024 * 1024),
    "wxtco.sort": SORT,
    # 10 concurrent one-lead committers give HTTP 409 conflicts; pyiceberg's default of 4 retries lost
    # leads on 2026-09-20. Retry for up to an hour with 0.5-15 s backoff.
    "commit.retry.num-retries": "30",
    "commit.retry.min-wait-ms": "500",
    "commit.retry.max-wait-ms": "15000",
    "commit.retry.total-timeout-ms": "3600000",
}


def progress_method() -> str:
    """Give the `_progress/<method>/` name: `table` for the study table, `table_<name>` for a scratch table."""
    name = TABLE_ID.split(".")[-1]
    return "table" if TABLE_ID == f"{NAMESPACE}.surface" else f"table_{name}"


def column_name(slug: str) -> str:
    """Make a column name from a diagnostic slug."""
    return slug.replace("-", "_")


def iceberg_schema(slugs: Sequence[str]) -> Schema:
    """Build the Iceberg schema: the key columns, then one column per slug.

    Field ids follow slug order. Use one slug list for the life of a table. Timestamps are naive UTC.
    """
    keys = [
        NestedField(1, "init_time", TimestampType(), required=True),
        NestedField(2, "lead_hours", IntegerType(), required=True),
        NestedField(3, "valid_time", TimestampType(), required=True),
        NestedField(4, "member", IntegerType(), required=True),
        NestedField(5, "lat", FloatType(), required=True),
        NestedField(6, "lon", FloatType(), required=True),
    ]
    values = [NestedField(7 + i, column_name(s), FloatType(), required=False) for i, s in enumerate(slugs)]
    return Schema(*keys, *values)


def partition_spec() -> PartitionSpec:
    """Give the identity partition spec on init_time and lead_hours."""
    return PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="init_time"),
        PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="lead_hours"),
    )


def arrow_schema(slugs: Sequence[str]) -> pa.Schema:
    """Give the Arrow schema of the Iceberg schema, with the same field ids."""
    return iceberg_schema(slugs).as_arrow()
