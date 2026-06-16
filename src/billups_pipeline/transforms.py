"""Pure, unit-testable transformations (DataFrame -> DataFrame).

Every function here is free of I/O so the same logic can be exercised against a
tiny in-memory DataFrame in the test suite and against the full dataset in the
pipeline. All transformations use the PySpark DataFrame API (no Spark SQL).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Text used to replace null/blank product categories (per the cleaning rules).
UNKNOWN_CATEGORY = "Unknown category"

# Format Spark uses for the historical_transactions purchase_date strings.
_PURCHASE_DATE_FMT = "yyyy-MM-dd HH:mm:ss"


# --------------------------------------------------------------------------- #
# Silver
# --------------------------------------------------------------------------- #
def clean_merchants(raw: DataFrame) -> DataFrame:
    """Type-cast merchant attributes and deduplicate to one row per merchant.

    The commercial extract contains duplicate ``merchant_id`` rows; we keep a
    single deterministic row per id so the downstream join cannot fan out.
    """
    typed = raw.select(
        F.col("merchant_id").cast("string").alias("merchant_id"),
        F.trim(F.col("merchant_name")).alias("merchant_name"),
        F.col("merchant_group_id").cast("int").alias("merchant_group_id"),
        F.col("merchant_category_id").cast("int").alias("merchant_category_id"),
        F.col("subsector_id").cast("int").alias("subsector_id"),
        F.col("most_recent_sales_range").alias("most_recent_sales_range"),
        F.col("most_recent_purchases_range").alias("most_recent_purchases_range"),
        F.col("city_id").cast("int").alias("city_id"),
        F.col("state_id").cast("int").alias("state_id"),
    )

    # Deterministic dedupe: lowest (merchant_group_id, city_id) wins ties.
    w = Window.partitionBy("merchant_id").orderBy(
        F.col("merchant_group_id").asc_nulls_last(),
        F.col("city_id").asc_nulls_last(),
        F.col("merchant_category_id").asc_nulls_last(),
    )
    return typed.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _clean_category(col: Column) -> Column:
    """Null or blank category -> 'Unknown category' (records are NOT dropped)."""
    trimmed = F.trim(col)
    return F.when(col.isNull() | (trimmed == F.lit("")), F.lit(UNKNOWN_CATEGORY)).otherwise(trimmed)


def clean_transactions(raw: DataFrame) -> DataFrame:
    """Parse timestamps, derive time dimensions, and clean categories.

    Keeps every record (the cleaning rules forbid dropping null-category rows).
    """
    ts = F.to_timestamp(F.col("purchase_date"), _PURCHASE_DATE_FMT)
    hour = F.hour(ts)

    return raw.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("merchant_id").cast("string").alias("merchant_id"),
        F.col("merchant_category_id").cast("int").alias("merchant_category_id"),
        F.col("subsector_id").cast("int").alias("subsector_id"),
        F.col("authorized_flag").cast("string").alias("authorized_flag"),
        F.col("installments").cast("int").alias("installments"),
        F.col("month_lag").cast("int").alias("month_lag"),
        F.col("purchase_amount").cast("double").alias("purchase_amount"),
        # Transaction-level geography (where the sale happened).
        F.col("city_id").cast("int").alias("city_id"),
        F.col("state_id").cast("int").alias("state_id"),
        _clean_category(F.col("category")).alias("category"),
        ts.alias("purchase_ts"),
        F.year(ts).alias("txn_year"),
        F.month(ts).alias("txn_month_num"),
        # Sortable yyyymm key plus the "Oct 2017" presentation label.
        (F.year(ts) * F.lit(100) + F.month(ts)).alias("txn_month_key"),
        F.date_format(ts, "MMM yyyy").alias("txn_month_label"),
        hour.alias("txn_hour"),
        # 24h clock label as in the spec example: 1300, 0800, 1900.
        F.lpad((hour * F.lit(100)).cast("string"), 4, "0").alias("txn_hour_label"),
    )


# --------------------------------------------------------------------------- #
# Gold
# --------------------------------------------------------------------------- #
def build_gold(transactions: DataFrame, merchants: DataFrame) -> DataFrame:
    """Left-join transactions to merchant attributes.

    Where a transaction has no matching merchant, the ``merchant_id`` is used as
    the merchant name (per the cleaning rules). Transaction-level city/state are
    the analytic geography; merchant-level location is carried as ``merchant_*``.
    """
    m = merchants.select(
        F.col("merchant_id"),
        F.col("merchant_name"),
        F.col("merchant_group_id"),
        F.col("most_recent_sales_range"),
        F.col("most_recent_purchases_range"),
        F.col("city_id").alias("merchant_city_id"),
        F.col("state_id").alias("merchant_state_id"),
    )

    joined = transactions.join(m, on="merchant_id", how="left")

    return joined.select(
        "customer_id",
        "merchant_id",
        # merchant_id stands in for the name when the merchant is unknown.
        F.coalesce(F.col("merchant_name"), F.col("merchant_id")).alias("merchant_name"),
        "category",
        "purchase_amount",
        "installments",
        "authorized_flag",
        "city_id",
        "state_id",
        "merchant_category_id",
        "subsector_id",
        "month_lag",
        "purchase_ts",
        "txn_year",
        "txn_month_num",
        "txn_month_key",
        "txn_month_label",
        "txn_hour",
        "txn_hour_label",
        "merchant_group_id",
        "most_recent_sales_range",
        "most_recent_purchases_range",
        "merchant_city_id",
        "merchant_state_id",
    )


# --------------------------------------------------------------------------- #
# Analytics (Questions 1-5) — also reused by the notebook and tests.
# --------------------------------------------------------------------------- #
def q1_top_merchants_by_city_month(gold: DataFrame, n: int = 5) -> DataFrame:
    """Top ``n`` merchants by purchase total, per month, per city."""
    agg = gold.groupBy("txn_month_key", "txn_month_label", "city_id", "merchant_name").agg(
        F.sum("purchase_amount").alias("purchase_total"),
        F.count(F.lit(1)).alias("no_of_sales"),
    )
    w = Window.partitionBy("txn_month_key", "city_id").orderBy(
        F.col("purchase_total").desc(), F.col("merchant_name").asc()
    )
    return (
        agg.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= n)
        .orderBy("txn_month_key", "city_id", "rank")
        .select(
            F.col("txn_month_label").alias("month"),
            F.col("city_id").alias("city"),
            F.col("merchant_name").alias("merchant"),
            "purchase_total",
            "no_of_sales",
            "rank",
        )
    )


def q2_avg_sale_by_merchant_state(gold: DataFrame) -> DataFrame:
    """Average purchase_amount per merchant per state, largest sales first."""
    return (
        gold.groupBy("merchant_name", "state_id")
        .agg(
            F.avg("purchase_amount").alias("average_amount"),
            F.count(F.lit(1)).alias("no_of_sales"),
        )
        .orderBy(F.col("average_amount").desc())
        .select(
            F.col("merchant_name").alias("merchant"),
            F.col("state_id").alias("state_id"),
            "average_amount",
            "no_of_sales",
        )
    )


def q3_top_hours_by_category(gold: DataFrame, n: int = 3) -> DataFrame:
    """Top ``n`` hours by total purchase_amount for each product category."""
    agg = gold.groupBy("category", "txn_hour", "txn_hour_label").agg(
        F.sum("purchase_amount").alias("purchase_total")
    )
    w = Window.partitionBy("category").orderBy(
        F.col("purchase_total").desc(), F.col("txn_hour").asc()
    )
    return (
        agg.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= n)
        .orderBy("category", "rank")
        .select(
            F.col("category").alias("product_category"),
            F.col("txn_hour_label").alias("hour"),
            "purchase_total",
            "rank",
        )
    )


def q4_popular_merchants_by_city(gold: DataFrame, n: int = 20) -> DataFrame:
    """Most popular merchants (by transaction count) and the city they sell in.

    Popularity = number of sales transactions. For each merchant we surface the
    city where it records the most transactions (its primary city).
    """
    by_merchant_city = gold.groupBy("merchant_name", "city_id").agg(
        F.count(F.lit(1)).alias("city_sales")
    )
    w_city = Window.partitionBy("merchant_name").orderBy(
        F.col("city_sales").desc(), F.col("city_id").asc()
    )
    primary_city = (
        by_merchant_city.withColumn("rn", F.row_number().over(w_city))
        .filter(F.col("rn") == 1)
        .select("merchant_name", F.col("city_id").alias("primary_city_id"), "city_sales")
    )

    totals = gold.groupBy("merchant_name").agg(F.count(F.lit(1)).alias("total_sales"))

    return (
        totals.join(primary_city, on="merchant_name", how="inner")
        .orderBy(F.col("total_sales").desc())
        .limit(n)
        .select(
            F.col("merchant_name").alias("merchant"),
            "total_sales",
            "primary_city_id",
            F.col("city_sales").alias("sales_in_primary_city"),
        )
    )
