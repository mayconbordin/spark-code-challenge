"""Unit tests for the PySpark pipeline transforms.

These cover the cleaning rules and each analytical question on small, fully
controlled inputs where the expected answer can be reasoned by hand.
"""

from __future__ import annotations

from pyspark.sql import Row

from billups_pipeline.transforms import (
    UNKNOWN_CATEGORY,
    build_gold,
    clean_merchants,
    clean_transactions,
    q1_top_merchants_by_city_month,
    q2_avg_sale_by_merchant_state,
    q3_top_hours_by_category,
    q4_popular_merchants_by_city,
)


# --------------------------------------------------------------------------- #
# Helpers to build raw-shaped DataFrames
# --------------------------------------------------------------------------- #
def _raw_merchants(spark, rows):
    cols = [
        "merchant_name",
        "merchant_id",
        "merchant_group_id",
        "merchant_category_id",
        "subsector_id",
        "most_recent_sales_range",
        "most_recent_purchases_range",
        "city_id",
        "state_id",
    ]
    return spark.createDataFrame([Row(**dict(zip(cols, r))) for r in rows])


def _raw_txns(spark, rows):
    cols = [
        "customer_id",
        "merchant_id",
        "merchant_category_id",
        "subsector_id",
        "authorized_flag",
        "installments",
        "month_lag",
        "purchase_amount",
        "city_id",
        "state_id",
        "category",
        "purchase_date",
    ]
    return spark.createDataFrame([Row(**dict(zip(cols, r))) for r in rows])


# --------------------------------------------------------------------------- #
# Silver: merchants
# --------------------------------------------------------------------------- #
def test_clean_merchants_dedupes_by_id(spark):
    raw = _raw_merchants(
        spark,
        [
            ("Alpha inc", "M_1", "10", "100", "5", "A", "A", "2", "9"),
            # duplicate id with a higher group_id -> should be dropped
            ("Alpha inc DUP", "M_1", "99", "100", "5", "A", "A", "2", "9"),
            ("Beta inc", "M_2", "20", "200", "6", "B", "B", "3", "9"),
        ],
    )
    out = clean_merchants(raw)
    assert out.count() == 2  # one row per merchant_id
    alpha = out.filter(out.merchant_id == "M_1").first()
    assert alpha.merchant_group_id == 10  # lowest group_id won the tie-break
    assert alpha.city_id == 2 and isinstance(alpha.city_id, int)


# --------------------------------------------------------------------------- #
# Silver: transactions
# --------------------------------------------------------------------------- #
def test_clean_transactions_null_category_becomes_unknown(spark):
    raw = _raw_txns(
        spark,
        [
            ("C1", "M_1", 1, 1, "Y", 1, -1, 100.0, 5, 9, None, "2017-10-01 13:30:00"),
            ("C2", "M_1", 1, 1, "Y", 1, -1, 100.0, 5, 9, "  ", "2017-10-01 13:30:00"),
            ("C3", "M_1", 1, 1, "Y", 1, -1, 100.0, 5, 9, "A", "2017-10-01 13:30:00"),
        ],
    )
    out = clean_transactions(raw)
    cats = {r.category for r in out.collect()}
    assert UNKNOWN_CATEGORY in cats and "A" in cats
    # No records dropped by the cleaning step.
    assert out.count() == 3


def test_clean_transactions_derives_time_dimensions(spark):
    raw = _raw_txns(
        spark,
        [("C1", "M_1", 1, 1, "Y", 1, -1, 100.0, 5, 9, "A", "2017-10-05 08:07:00")],
    )
    row = clean_transactions(raw).first()
    assert row.txn_year == 2017
    assert row.txn_month_num == 10
    assert row.txn_month_key == 201710
    assert row.txn_month_label == "Oct 2017"
    assert row.txn_hour == 8
    assert row.txn_hour_label == "0800"  # zero-padded 24h clock label


# --------------------------------------------------------------------------- #
# Gold: join + name fallback
# --------------------------------------------------------------------------- #
def _gold(spark, txn_rows, merch_rows):
    return build_gold(
        clean_transactions(_raw_txns(spark, txn_rows)),
        clean_merchants(_raw_merchants(spark, merch_rows)),
    )


def test_build_gold_uses_merchant_id_when_no_match(spark):
    txns = [
        ("C1", "M_known", 1, 1, "Y", 1, -1, 100.0, 5, 9, "A", "2017-10-01 13:00:00"),
        ("C2", "M_orphan", 1, 1, "Y", 1, -1, 100.0, 5, 9, "A", "2017-10-01 13:00:00"),
    ]
    merch = [("Known inc", "M_known", "10", "100", "5", "A", "A", "5", "9")]
    out = {r.merchant_id: r.merchant_name for r in _gold(spark, txns, merch).collect()}
    assert out["M_known"] == "Known inc"
    assert out["M_orphan"] == "M_orphan"  # id stands in for the missing name


def test_build_gold_does_not_fan_out_on_duplicate_merchants(spark):
    # Two merchant rows share an id; the join must still yield one row per txn.
    txns = [("C1", "M_1", 1, 1, "Y", 1, -1, 100.0, 5, 9, "A", "2017-10-01 13:00:00")]
    merch = [
        ("Alpha inc", "M_1", "10", "100", "5", "A", "A", "2", "9"),
        ("Alpha DUP", "M_1", "99", "100", "5", "A", "A", "2", "9"),
    ]
    assert _gold(spark, txns, merch).count() == 1


# --------------------------------------------------------------------------- #
# Questions
# --------------------------------------------------------------------------- #
def test_q1_top_merchants_by_city_month(spark):
    # City 2, Oct 2017: A=300, B=100, C=50 -> A then B then C; limit 5 keeps all.
    txns = [
        ("C", "A", 1, 1, "Y", 1, -1, 300.0, 2, 9, "X", "2017-10-01 13:00:00"),
        ("C", "B", 1, 1, "Y", 1, -1, 100.0, 2, 9, "X", "2017-10-01 13:00:00"),
        ("C", "C", 1, 1, "Y", 1, -1, 50.0, 2, 9, "X", "2017-10-01 13:00:00"),
        # different city same month -> separate ranking
        ("C", "A", 1, 1, "Y", 1, -1, 10.0, 3, 9, "X", "2017-10-01 13:00:00"),
    ]
    merch = [
        ("Merch A", "A", "1", "1", "1", "A", "A", "2", "9"),
        ("Merch B", "B", "1", "1", "1", "A", "A", "2", "9"),
        ("Merch C", "C", "1", "1", "1", "A", "A", "2", "9"),
    ]
    res = q1_top_merchants_by_city_month(_gold(spark, txns, merch), n=5)
    city2 = [r for r in res.collect() if r.city == 2]
    assert [r.merchant for r in city2] == ["Merch A", "Merch B", "Merch C"]
    assert city2[0].purchase_total == 300.0 and city2[0].no_of_sales == 1
    assert city2[0]["rank"] == 1


def test_q1_respects_top_n(spark):
    txns = [
        ("C", f"M{i}", 1, 1, "Y", 1, -1, float(100 - i), 2, 9, "X", "2017-10-01 13:00:00")
        for i in range(7)
    ]
    merch = [(f"Merch {i}", f"M{i}", "1", "1", "1", "A", "A", "2", "9") for i in range(7)]
    res = q1_top_merchants_by_city_month(_gold(spark, txns, merch), n=5)
    assert res.filter(res.city == 2).count() == 5  # only top 5 of 7 kept


def test_q2_avg_sale_by_merchant_state_ordering(spark):
    txns = [
        ("C", "A", 1, 1, "Y", 1, -1, 100.0, 5, 2, "X", "2017-10-01 13:00:00"),
        ("C", "A", 1, 1, "Y", 1, -1, 200.0, 5, 2, "X", "2017-10-01 13:00:00"),  # A/state2 avg=150
        ("C", "A", 1, 1, "Y", 1, -1, 10.0, 5, 10, "X", "2017-10-01 13:00:00"),  # A/state10 avg=10
        ("C", "B", 1, 1, "Y", 1, -1, 90.0, 5, 2, "X", "2017-10-01 13:00:00"),  # B/state2 avg=90
    ]
    merch = [
        ("Merch A", "A", "1", "1", "1", "A", "A", "5", "9"),
        ("Merch B", "B", "1", "1", "1", "A", "A", "5", "9"),
    ]
    res = q2_avg_sale_by_merchant_state(_gold(spark, txns, merch)).collect()
    # Largest average first.
    assert (res[0].merchant, res[0].state_id, res[0].average_amount) == ("Merch A", 2, 150.0)
    assert res[1].average_amount == 90.0  # Merch B / state 2
    assert res[-1].average_amount == 10.0  # Merch A / state 10


def test_q3_top_hours_by_category(spark):
    # Category A: hour 13 total 300 > hour 09 total 100 > hour 20 total 50.
    txns = [
        ("C", "M", 1, 1, "Y", 1, -1, 300.0, 5, 9, "A", "2017-10-01 13:00:00"),
        ("C", "M", 1, 1, "Y", 1, -1, 100.0, 5, 9, "A", "2017-10-01 09:00:00"),
        ("C", "M", 1, 1, "Y", 1, -1, 50.0, 5, 9, "A", "2017-10-01 20:00:00"),
        ("C", "M", 1, 1, "Y", 1, -1, 5.0, 5, 9, "A", "2017-10-01 01:00:00"),  # 4th, dropped
    ]
    merch = [("Merch M", "M", "1", "1", "1", "A", "A", "5", "9")]
    res = q3_top_hours_by_category(_gold(spark, txns, merch), n=3).collect()
    assert [r.hour for r in res] == ["1300", "0900", "2000"]
    assert all(r.product_category == "A" for r in res)


def test_q4_popular_merchant_and_primary_city(spark):
    # Merchant M1 has 3 txns (2 in city 5, 1 in city 7); M2 has 1 txn.
    txns = [
        ("C", "M1", 1, 1, "Y", 1, -1, 10.0, 5, 9, "A", "2017-10-01 13:00:00"),
        ("C", "M1", 1, 1, "Y", 1, -1, 10.0, 5, 9, "A", "2017-10-01 13:00:00"),
        ("C", "M1", 1, 1, "Y", 1, -1, 10.0, 7, 9, "A", "2017-10-01 13:00:00"),
        ("C", "M2", 1, 1, "Y", 1, -1, 10.0, 8, 9, "A", "2017-10-01 13:00:00"),
    ]
    merch = [
        ("Merch 1", "M1", "1", "1", "1", "A", "A", "5", "9"),
        ("Merch 2", "M2", "1", "1", "1", "A", "A", "8", "9"),
    ]
    res = q4_popular_merchants_by_city(_gold(spark, txns, merch), n=10).collect()
    top = res[0]
    assert top.merchant == "Merch 1"
    assert top.total_sales == 3
    assert top.primary_city_id == 5  # city with the most of its transactions
    assert top.sales_in_primary_city == 2
