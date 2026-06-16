"""SparkSession construction wired for Apache Iceberg (hadoop catalog).

The hadoop catalog keeps the whole stack dependency-light: the Iceberg
warehouse is a plain directory that every container shares through a named
Docker volume, so no external metastore / REST catalog is required.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from .config import CATALOG, Settings, load_settings


def build_spark(settings: Settings | None = None) -> SparkSession:
    """Create (or fetch) a SparkSession configured for the Iceberg catalog."""
    settings = settings or load_settings()

    builder = (
        SparkSession.builder.appName(settings.app_name)
        .master(settings.master)
        # Pull the Iceberg runtime at session start (cached after first run).
        .config("spark.jars.packages", settings.iceberg_packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Iceberg "billups" catalog backed by a hadoop (filesystem) warehouse.
        .config(
            f"spark.sql.catalog.{CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", settings.warehouse)
        # Make every file/dir Iceberg writes world-writable. On the Docker stack
        # the driver (Airflow, uid 50000) and executors (worker, uid 185) share
        # one warehouse volume; without this they cannot write into each other's
        # directories. Harmless for single-user local runs.
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        # Sensible defaults for skewed retail-style aggregations.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
