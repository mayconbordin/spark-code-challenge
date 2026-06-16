"""Central configuration for the medallion pipeline.

All environment-specific values are read from environment variables so the
exact same code runs locally (``local[*]``) and on the Docker Spark cluster.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Iceberg catalog name used for every table reference (catalog.namespace.table).
CATALOG = "billups"

# Medallion namespaces (databases) inside the catalog.
BRONZE_NS = "bronze"
SILVER_NS = "silver"
GOLD_NS = "gold"

# Fully-qualified table identifiers.
T_BRONZE_MERCHANTS = f"{CATALOG}.{BRONZE_NS}.merchants"
T_BRONZE_TRANSACTIONS = f"{CATALOG}.{BRONZE_NS}.transactions"
T_SILVER_MERCHANTS = f"{CATALOG}.{SILVER_NS}.merchants"
T_SILVER_TRANSACTIONS = f"{CATALOG}.{SILVER_NS}.transactions"
T_GOLD_TRANSACTIONS = f"{CATALOG}.{GOLD_NS}.transactions_enriched"

# Default Iceberg runtime jar matched to Spark 3.5.x / Scala 2.12.
DEFAULT_ICEBERG_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1"


@dataclass(frozen=True)
class Settings:
    """Resolved runtime settings."""

    data_dir: str
    warehouse: str
    master: str
    iceberg_packages: str
    app_name: str = "billups-medallion-pipeline"

    @property
    def merchants_csv(self) -> str:
        return os.path.join(self.data_dir, "merchants-subset.csv")

    @property
    def transactions_path(self) -> str:
        return os.path.join(self.data_dir, "historical_transactions")


def load_settings() -> Settings:
    """Build :class:`Settings` from the environment with sensible local defaults."""
    return Settings(
        data_dir=os.environ.get("BILLUPS_DATA_DIR", "./data"),
        warehouse=os.path.abspath(os.environ.get("BILLUPS_WAREHOUSE", "./warehouse")),
        master=os.environ.get("SPARK_MASTER", "local[*]"),
        iceberg_packages=os.environ.get("BILLUPS_ICEBERG_PACKAGES", DEFAULT_ICEBERG_PACKAGES),
    )
