"""Silver layer: typed, cleaned, conformed tables."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from .config import (
    T_BRONZE_MERCHANTS,
    T_BRONZE_TRANSACTIONS,
    T_SILVER_MERCHANTS,
    T_SILVER_TRANSACTIONS,
    Settings,
)
from .io_utils import overwrite_table
from .transforms import clean_merchants, clean_transactions

log = logging.getLogger(__name__)


def run_silver(spark: SparkSession, settings: Settings) -> None:
    """Cast types, derive time dimensions, clean categories, dedupe merchants."""
    log.info("Silver: cleaning merchants")
    merchants = clean_merchants(spark.read.table(T_BRONZE_MERCHANTS))
    overwrite_table(merchants, T_SILVER_MERCHANTS)

    log.info("Silver: cleaning transactions")
    transactions = clean_transactions(spark.read.table(T_BRONZE_TRANSACTIONS))
    overwrite_table(transactions, T_SILVER_TRANSACTIONS)

    log.info("Silver complete.")
