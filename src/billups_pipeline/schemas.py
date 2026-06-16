"""Explicit read schemas for the raw source files.

Bronze lands the data faithfully (CSV read as strings), so type coercion is a
deliberate, auditable step that happens in Silver rather than via fragile
schema inference over values such as ``inf`` in the merchant lag columns.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

# Column order of merchants-subset.csv (see docs/Data Dictionary - merchants.csv).
MERCHANT_COLUMNS = [
    "merchant_name",
    "merchant_id",
    "merchant_group_id",
    "merchant_category_id",
    "subsector_id",
    "numerical_1",
    "numerical_2",
    "most_recent_sales_range",
    "most_recent_purchases_range",
    "avg_sales_lag3",
    "avg_purchases_lag3",
    "active_months_lag3",
    "avg_sales_lag6",
    "avg_purchases_lag6",
    "active_months_lag6",
    "avg_sales_lag12",
    "avg_purchases_lag12",
    "active_months_lag12",
    "city_id",
    "state_id",
]

# Read every CSV column as string in Bronze; cast only what we need in Silver.
MERCHANTS_RAW_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in MERCHANT_COLUMNS]
)
