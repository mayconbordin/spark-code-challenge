"""Gold layer: the merchants x transactions analytical table."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from .config import (
    T_GOLD_TRANSACTIONS,
    T_SILVER_MERCHANTS,
    T_SILVER_TRANSACTIONS,
    Settings,
)
from .io_utils import overwrite_table
from .transforms import build_gold

log = logging.getLogger(__name__)


def run_gold(spark: SparkSession, settings: Settings) -> None:
    """Join cleaned transactions to merchant attributes into the gold table."""
    log.info("Gold: building transactions_enriched")
    transactions = spark.read.table(T_SILVER_TRANSACTIONS)
    merchants = spark.read.table(T_SILVER_MERCHANTS)
    gold = build_gold(transactions, merchants)
    overwrite_table(gold, T_GOLD_TRANSACTIONS)
    log.info("Gold complete.")
