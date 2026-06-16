"""Shared pytest fixtures: a session-scoped local SparkSession.

The tests exercise the pure transforms in ``billups_pipeline.transforms`` against
tiny in-memory DataFrames, so they need a SparkSession but neither Iceberg nor
the real data — they run fast and offline.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("billups-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
