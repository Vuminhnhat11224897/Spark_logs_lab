"""Bronze CSV-to-Parquet pipeline."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_log_lab.common.paths import bronze_dir, raw_dir
from spark_log_lab.io.readers import read_csv
from spark_log_lab.io.writers import write_parquet
from spark_log_lab.schemas.raw import LOG_TRACKING_SCHEMA, PURCHASE_BEHAVIOR_SCHEMA


def parse_log_tracking_to_bronze(log_df: DataFrame, batch_id: str) -> DataFrame:
    """Parse log tracking data to bronze."""
    parsed_df = (
        log_df.withColumn(
            "event_timestamp",
            F.coalesce(
                F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss z"),
                F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss"),
            ),
        )
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
    )
    return parsed_df.select(
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session",
        "event_timestamp",
        "event_date",
        "source_file",
        "ingest_time",
        "batch_id",
    )


def parse_purchase_behavior_to_bronze(purchase_df: DataFrame, batch_id: str) -> DataFrame:
    """Parse purchase behavior data to bronze."""
    parsed_df = (
        purchase_df.withColumn(
            "event_timestamp",
            F.coalesce(
                F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss z"),
                F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss"),
            ),
        )
        .withColumn("event_date", F.to_date(F.col("event_date")))
        .withColumn("first_event_date", F.to_date(F.col("first_event_date")))
        .withColumn("start_of_week", F.to_date(F.col("start_of_week")))
        .withColumn("end_of_week", F.to_date(F.col("end_of_week")))
        .withColumn("source_file", F.input_file_name())
        .withColumn("ingest_time", F.current_timestamp())
        .withColumn("batch_id", F.lit(batch_id))
    )
    return parsed_df.select(
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session",
        "event_timestamp",
        "event_date",
        "first_event_date",
        "start_of_week",
        "week_number",
        "end_of_week",
        "week_text",
        "cohort_index_week",
        "week_after",
        "source_file",
        "ingest_time",
        "batch_id",
    )


def build_bronze_pipeline(spark, batch_id: str) -> None:
    """Build the bronze pipeline."""
    log_path = raw_dir() / "01-log-tracking.csv"
    purchase_path = raw_dir() / "02-purchase-behavior.csv"
    log_df = read_csv(spark, log_path, LOG_TRACKING_SCHEMA)
    purchase_df = read_csv(spark, purchase_path, PURCHASE_BEHAVIOR_SCHEMA)
    log_bronze = parse_log_tracking_to_bronze(log_df, batch_id)
    purchase_bronze = parse_purchase_behavior_to_bronze(purchase_df, batch_id)
    write_parquet(log_bronze, bronze_dir() / "log_tracking", mode="overwrite")
    write_parquet(purchase_bronze, bronze_dir() / "purchase_behavior", mode="overwrite")
