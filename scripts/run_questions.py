"""Execute the five case-study questions against the gold table and print
results. Used to generate the figures embedded in the report.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from billups_pipeline.config import (
    T_BRONZE_TRANSACTIONS,
    T_GOLD_TRANSACTIONS,
    T_SILVER_MERCHANTS,
    T_SILVER_TRANSACTIONS,
)
from billups_pipeline.session import build_spark
from billups_pipeline.transforms import (
    q1_top_merchants_by_city_month,
    q2_avg_sale_by_merchant_state,
    q3_top_hours_by_category,
    q4_popular_merchants_by_city,
)

spark = build_spark()

print("\n==== ROW COUNTS / SANITY ====")
bronze_tx = spark.read.table(T_BRONZE_TRANSACTIONS).count()
silver_tx = spark.read.table(T_SILVER_TRANSACTIONS).count()
gold = spark.read.table(T_GOLD_TRANSACTIONS)
gold_ct = gold.count()
merch = spark.read.table(T_SILVER_MERCHANTS)
print(f"bronze transactions : {bronze_tx:,}")
print(f"silver transactions : {silver_tx:,}")
print(f"gold rows           : {gold_ct:,}")
print(
    f"silver merchants     : {merch.count():,}  distinct ids: "
    f"{merch.select('merchant_id').distinct().count():,}"
)
print("named vs id-fallback merchants in gold:")
gold.withColumn("is_named", F.col("merchant_name") != F.col("merchant_id")).groupBy(
    "is_named"
).count().show()
print(
    "distinct cities:",
    gold.select("city_id").distinct().count(),
    " distinct states:",
    gold.select("state_id").distinct().count(),
    " distinct categories:",
    gold.select("category").distinct().count(),
)
print("month range:")
gold.groupBy("txn_month_key", "txn_month_label").count().orderBy("txn_month_key").show(50, False)
print("category distribution:")
gold.groupBy("category").agg(
    F.count(F.lit(1)).alias("n"), F.sum("purchase_amount").alias("total")
).orderBy(F.col("n").desc()).show(50, False)

print("\n==== Q1: top 5 merchants by purchase_total per city per month (sample) ====")
q1 = q1_top_merchants_by_city_month(gold)
print("q1 total rows:", q1.count())
q1.show(40, False)

print("\n==== Q2: avg sale per merchant per state (largest first) ====")
q2 = q2_avg_sale_by_merchant_state(gold)
q2.show(25, False)

print("\n==== Q3: top 3 hours by purchase_amount per category ====")
q3 = q3_top_hours_by_category(gold)
q3.show(100, False)

print("\n==== Q4: most popular merchants (by txn count) & their city ====")
q4 = q4_popular_merchants_by_city(gold, n=20)
q4.show(25, False)

print("\n==== Q4: correlation city_id vs category (top cities by txn) ====")
gold.groupBy("city_id").agg(F.count(F.lit(1)).alias("n")).orderBy(F.col("n").desc()).show(15, False)

spark.stop()
