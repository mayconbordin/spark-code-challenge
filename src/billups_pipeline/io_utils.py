"""Small Iceberg helpers shared by the layer modules."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .config import CATALOG


def ensure_namespaces(spark: SparkSession) -> None:
    """Create the bronze/silver/gold namespaces if they do not exist."""
    for ns in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")


def overwrite_table(df: DataFrame, table: str) -> None:
    """(Re)create an Iceberg table from ``df`` — full-refresh semantics.

    Using create-or-replace keeps the pipeline idempotent: re-running any layer
    yields the same table regardless of prior state.
    """
    (df.writeTo(table).using("iceberg").tableProperty("format-version", "2").createOrReplace())
