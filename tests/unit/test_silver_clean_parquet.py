from __future__ import annotations

import pytest


pytest.importorskip("pyspark")
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-clean-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_purchase_silver_recomputes_canonical_dates_and_warns_on_source_mismatch(spark):
    from spark_log_lab.pipelines.silver_clean_parquet import clean_purchase_behavior_to_silver

    bronze_df = spark.createDataFrame(
        [
            {
                "event_time": "2019-11-01 08:00:00",
                "event_type": "purchase",
                "product_id": "1000000",
                "category_id": "2053013555631882655",
                "category_code": "furniture.bedroom.bed",
                "brand": "",
                "price": "19.99",
                "user_id": "520000000",
                "user_session": "sample-session-000",
                "event_timestamp": "2019-11-01 08:00:00",
                "event_date": "2019-11-02",
                "first_event_date": "2019-10-25",
                "start_of_week": "2019-11-04",
                "week_number": "45",
                "end_of_week": "2019-11-10",
                "week_text": "W45",
                "cohort_index_week": "W45 (2019-11-04 -> 2019-11-10)",
                "week_after": "1",
                "source_file": "purchase.csv",
                "ingest_time": "2026-06-16 03:02:19",
                "batch_id": "batch-001",
            }
        ]
    )

    silver_df, quarantine_df = clean_purchase_behavior_to_silver(bronze_df)
    row = silver_df.collect()[0].asDict()

    assert quarantine_df.count() == 0
    assert str(row["event_date"]) == "2019-11-01"
    assert str(row["cohort_week_start"]) == "2019-10-28"
    assert str(row["cohort_week_end"]) == "2019-11-03"
    assert row["cohort_week_number"] == 44
    assert row["cohort_week_label"] == "W44"
    assert row["category_l1"] == "furniture"
    assert row["category_l2"] == "bedroom"
    assert row["category_l3"] == "bed"
    assert row["is_brand_missing"] is True
    assert row["is_cohort_week_mismatch"] is True
    assert "BRAND_MISSING" in row["dq_warnings"]
    assert "SOURCE_COHORT_WEEK_MISMATCH" in row["dq_warnings"]
    assert "EVENT_DATE_MISMATCH_WITH_TIMESTAMP" in row["dq_warnings"]


def test_log_silver_quarantines_hard_rule_failures_and_keeps_warnings_separate(spark):
    from spark_log_lab.pipelines.silver_clean_parquet import clean_log_tracking_to_silver

    bronze_df = spark.createDataFrame(
        [
            {
                "event_time": "not-a-timestamp",
                "event_type": "download",
                "product_id": "bad-product",
                "category_id": "bad-category",
                "category_code": "",
                "brand": "",
                "price": "-1.00",
                "user_id": "bad-user",
                "user_session": "session-1",
                "event_timestamp": "",
                "event_date": "",
                "source_file": "log.csv",
                "ingest_time": "2026-06-16 03:02:19",
                "batch_id": "batch-001",
            }
        ]
    )

    silver_df, quarantine_df = clean_log_tracking_to_silver(bronze_df)
    quarantined = quarantine_df.collect()[0].asDict()

    assert silver_df.count() == 0
    assert quarantined["rule_id"] == "EVENT_TIMESTAMP_PARSE_FAILED"
    assert quarantined["dataset"] == "log_tracking"
    assert quarantined["product_id"] == "bad-product"
    assert quarantined["price"] == "-1.00"


def test_log_silver_quarantines_duplicate_records_but_keeps_one_clean_row(spark):
    from spark_log_lab.pipelines.silver_clean_parquet import clean_log_tracking_to_silver

    row = {
        "event_time": "2019-11-01 08:00:00",
        "event_type": "view",
        "product_id": "1000000",
        "category_id": "2053013555631882655",
        "category_code": "electronics.smartphone",
        "brand": "samsung",
        "price": "19.99",
        "user_id": "520000000",
        "user_session": "sample-session-000",
        "event_timestamp": "2019-11-01 08:00:00",
        "event_date": "2019-11-01",
        "source_file": "log.csv",
        "ingest_time": "2026-06-16 03:02:19",
        "batch_id": "batch-001",
    }
    bronze_df = spark.createDataFrame([row, row])

    silver_df, quarantine_df = clean_log_tracking_to_silver(bronze_df)
    silver_row = silver_df.collect()[0].asDict()

    assert silver_df.count() == 1
    assert silver_row["category_l1"] == "electronics"
    assert silver_row["category_l2"] == "smartphone"
    assert silver_row["category_l3"] is None
    assert quarantine_df.count() == 1
    assert quarantine_df.collect()[0]["rule_id"] == "DUPLICATE_RECORD"
