"""Bronze layer: land the raw sources into Iceberg with no business logic."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from .config import T_BRONZE_MERCHANTS, T_BRONZE_TRANSACTIONS, Settings
from .io_utils import ensure_namespaces, overwrite_table
from .schemas import MERCHANTS_RAW_SCHEMA

log = logging.getLogger(__name__)


def run_bronze(spark: SparkSession, settings: Settings) -> None:
    """Read merchants.csv and the transactions parquet; persist verbatim."""
    ensure_namespaces(spark)

    log.info("Bronze: reading merchants from %s", settings.merchants_csv)
    merchants = (
        spark.read.option("header", True).schema(MERCHANTS_RAW_SCHEMA).csv(settings.merchants_csv)
    )
    overwrite_table(merchants, T_BRONZE_MERCHANTS)

    log.info("Bronze: reading transactions from %s", settings.transactions_path)
    transactions = spark.read.parquet(settings.transactions_path)
    overwrite_table(transactions, T_BRONZE_TRANSACTIONS)

    log.info("Bronze complete.")
