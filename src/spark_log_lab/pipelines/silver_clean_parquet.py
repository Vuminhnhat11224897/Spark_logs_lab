"""Silver cleaning pipeline."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_log_lab.common.paths import bronze_dir, warehouse_dir
from spark_log_lab.io.readers import read_parquet
from spark_log_lab.io.writers import write_parquet
from spark_log_lab.schemas.silver import (
    LOG_TRACKING_SILVER_SCHEMA,
    PURCHASE_BEHAVIOR_SILVER_SCHEMA,
    SILVER_DEDUP_KEYS,
    SILVER_QUARANTINE_SCHEMA,
)


VALID_EVENT_TYPES = ("view", "cart", "purchase")
PRICE_TYPE_SQL = "decimal(18,2)"


def _clean_string(column_name: str) -> F.Column:
    column = F.trim(F.col(column_name).cast("string"))
    return F.when(column == "", F.lit(None)).otherwise(column)


def _try_cast(column_name: str, data_type: str) -> F.Column:
    return F.expr(f"try_cast({column_name} as {data_type})")


def _normalize_common_columns(df: DataFrame, dataset: str) -> DataFrame:
    normalized = df
    for column_name in [
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session",
        "source_file",
        "batch_id",
    ]:
        if column_name in normalized.columns:
            normalized = normalized.withColumn(f"{column_name}_clean", _clean_string(column_name))

    return (
        normalized.withColumn(
            "event_timestamp_typed",
            F.coalesce(
                F.expr("try_cast(event_timestamp as timestamp)"),
                F.expr("try_to_timestamp(event_time_clean, 'yyyy-MM-dd HH:mm:ss z')"),
                F.expr("try_to_timestamp(event_time_clean, 'yyyy-MM-dd HH:mm:ss')"),
            ),
        )
        .withColumn("event_date_canonical", F.to_date(F.col("event_timestamp_typed")))
        .withColumn("event_type_clean", F.lower(F.col("event_type_clean")))
        .withColumn("product_id_typed", _try_cast("product_id_clean", "bigint"))
        .withColumn("category_id_typed", _try_cast("category_id_clean", "bigint"))
        .withColumn("user_id_typed", _try_cast("user_id_clean", "bigint"))
        .withColumn("price_typed", _try_cast("price_clean", PRICE_TYPE_SQL))
        .withColumn("dataset", F.lit(dataset))
    )


def _with_category_fields(df: DataFrame) -> DataFrame:
    parts = F.split(F.col("category_code_clean"), "\\.")
    return (
        df.withColumn("category_l1", F.get(parts, F.lit(0)))
        .withColumn("category_l2", F.get(parts, F.lit(1)))
        .withColumn("category_l3", F.get(parts, F.lit(2)))
    )


def _with_common_hard_rule(df: DataFrame, require_price: bool, require_source_event_date: bool) -> DataFrame:
    missing_required = (
        F.col("event_time_clean").isNull()
        | F.col("event_type_clean").isNull()
        | F.col("product_id_clean").isNull()
        | F.col("user_id_clean").isNull()
        | F.col("user_session_clean").isNull()
        | (F.lit(require_price) & F.col("price_clean").isNull())
        | (F.lit(require_source_event_date) & F.col("event_date").isNull())
    )
    timestamp_failed = F.col("event_time_clean").isNotNull() & F.col("event_timestamp_typed").isNull()
    invalid_event_type = ~F.col("event_type_clean").isin(*VALID_EVENT_TYPES)
    invalid_id = (
        (F.col("product_id_clean").isNotNull() & F.col("product_id_typed").isNull())
        | (F.col("user_id_clean").isNotNull() & F.col("user_id_typed").isNull())
    )
    invalid_purchase_price = (
        F.lit(require_price) & F.col("price_clean").isNotNull() & F.col("price_typed").isNull()
    )

    return (
        df.withColumn(
            "hard_rule_id",
            F.when(missing_required, F.lit("MISSING_REQUIRED_FIELD"))
            .when(timestamp_failed, F.lit("EVENT_TIMESTAMP_PARSE_FAILED"))
            .when(invalid_event_type, F.lit("INVALID_EVENT_TYPE"))
            .when(invalid_id, F.lit("INVALID_REQUIRED_ID_CAST"))
            .when(invalid_purchase_price, F.lit("INVALID_PURCHASE_PRICE")),
        )
        .withColumn(
            "hard_failed_columns",
            F.when(missing_required, F.array(F.lit("required_fields")))
            .when(timestamp_failed, F.array(F.lit("event_time"), F.lit("event_timestamp")))
            .when(invalid_event_type, F.array(F.lit("event_type")))
            .when(invalid_id, F.array(F.lit("product_id"), F.lit("user_id")))
            .when(invalid_purchase_price, F.array(F.lit("price"))),
        )
        .withColumn(
            "hard_failed_reason",
            F.when(missing_required, F.lit("A required Silver field is missing."))
            .when(timestamp_failed, F.lit("Event timestamp could not be parsed."))
            .when(invalid_event_type, F.lit("Event type is outside the allowed event enum."))
            .when(invalid_id, F.lit("A required identifier could not be cast to long."))
            .when(invalid_purchase_price, F.lit("Purchase price could not be cast to decimal."))
        )
    )


def _warning_array(*rules: tuple[str, F.Column]) -> F.Column:
    entries = [
        F.when(condition, F.array(F.lit(rule_id))).otherwise(F.array().cast("array<string>"))
        for rule_id, condition in rules
    ]
    result = entries[0]
    for entry in entries[1:]:
        result = F.concat(result, entry)
    return result


def _with_common_warnings(df: DataFrame) -> DataFrame:
    warnings = _warning_array(
        ("CATEGORY_CODE_MISSING", F.col("category_code_clean").isNull()),
        ("BRAND_MISSING", F.col("brand_clean").isNull()),
        (
            "CATEGORY_ID_CAST_FAILED",
            F.col("category_id_clean").isNotNull() & F.col("category_id_typed").isNull(),
        ),
        ("PRICE_ZERO_OR_NEGATIVE", F.col("price_typed") <= F.lit(0)),
    )
    return (
        df.withColumn("is_category_code_missing", F.col("category_code_clean").isNull())
        .withColumn("is_brand_missing", F.col("brand_clean").isNull())
        .withColumn("dq_warnings", warnings)
        .withColumn("dq_warning_count", F.size(F.col("dq_warnings")))
    )


def _with_purchase_dates_and_warnings(df: DataFrame) -> DataFrame:
    with_source_dates = (
        df.withColumn("source_event_date_typed", F.expr("try_cast(event_date as date)"))
        .withColumn("first_event_date_typed", F.expr("try_cast(first_event_date as date)"))
        .withColumn("source_week_start_typed", F.expr("try_cast(start_of_week as date)"))
        .withColumn("source_week_end_typed", F.expr("try_cast(end_of_week as date)"))
        .withColumn("source_week_number_typed", F.expr("try_cast(week_number as int)"))
        .withColumn("week_after_typed", F.expr("try_cast(week_after as int)"))
    )
    with_canonical_week = (
        with_source_dates.withColumn(
            "cohort_week_start", F.date_sub(F.next_day(F.col("event_date_canonical"), "Mon"), 7)
        )
        .withColumn("cohort_week_end", F.date_add(F.col("cohort_week_start"), 6))
        .withColumn("cohort_week_number", F.weekofyear(F.col("event_date_canonical")))
        .withColumn("cohort_week_label", F.format_string("W%02d", F.col("cohort_week_number")))
        .withColumn(
            "week_after_canonical",
            F.floor(F.datediff(F.col("event_date_canonical"), F.col("first_event_date_typed")) / 7),
        )
    )
    source_week_mismatch = (
        (F.col("source_week_start_typed").isNotNull())
        & (F.col("source_week_end_typed").isNotNull())
        & (
            (F.col("source_week_start_typed") != F.col("cohort_week_start"))
            | (F.col("source_week_end_typed") != F.col("cohort_week_end"))
            | (
                F.col("source_week_number_typed").isNotNull()
                & (F.col("source_week_number_typed") != F.col("cohort_week_number"))
            )
        )
    )
    event_date_mismatch = (
        F.col("source_event_date_typed").isNotNull()
        & (F.col("source_event_date_typed") != F.col("event_date_canonical"))
    )
    event_date_failed = F.col("event_date").isNotNull() & F.col("source_event_date_typed").isNull()

    return (
        with_canonical_week.withColumn("is_cohort_week_mismatch", source_week_mismatch)
        .withColumn(
            "hard_rule_id",
            F.when(
                F.col("hard_rule_id").isNull() & event_date_failed,
                F.lit("EVENT_DATE_PARSE_FAILED"),
            ).otherwise(F.col("hard_rule_id")),
        )
        .withColumn(
            "hard_failed_columns",
            F.when(
                F.col("hard_rule_id").isNull() & event_date_failed,
                F.array(F.lit("event_date")),
            ).otherwise(F.col("hard_failed_columns")),
        )
        .withColumn(
            "hard_failed_reason",
            F.when(
                F.col("hard_rule_id").isNull() & event_date_failed,
                F.lit("Source event_date could not be parsed."),
            ).otherwise(F.col("hard_failed_reason")),
        )
        .withColumn(
            "dq_warnings",
            F.concat(
                F.col("dq_warnings"),
                _warning_array(
                    ("SOURCE_COHORT_WEEK_MISMATCH", source_week_mismatch),
                    ("EVENT_DATE_MISMATCH_WITH_TIMESTAMP", event_date_mismatch),
                ),
            ),
        )
        .withColumn("dq_warning_count", F.size(F.col("dq_warnings")))
    )


def _quarantine_from(df: DataFrame, rule_id_column: str = "hard_rule_id") -> DataFrame:
    selected = df.select(
        F.col("dataset"),
        F.col(rule_id_column).alias("rule_id"),
        F.col(rule_id_column).alias("rule_name"),
        F.lit("error").alias("severity"),
        F.col("hard_failed_columns").alias("failed_columns"),
        F.col("hard_failed_reason").alias("failed_reason"),
        F.current_timestamp().alias("quarantined_at"),
        F.col("event_time").cast("string"),
        F.col("event_type").cast("string"),
        F.col("product_id").cast("string"),
        F.col("category_id").cast("string"),
        F.col("category_code").cast("string"),
        F.col("brand").cast("string"),
        F.col("price").cast("string"),
        F.col("user_id").cast("string"),
        F.col("user_session").cast("string"),
        _optional_string(df, "event_date"),
        _optional_string(df, "first_event_date"),
        _optional_string(df, "start_of_week"),
        _optional_string(df, "week_number"),
        _optional_string(df, "end_of_week"),
        _optional_string(df, "week_text"),
        _optional_string(df, "cohort_index_week"),
        _optional_string(df, "week_after"),
        F.col("source_file").cast("string"),
        F.expr("try_cast(ingest_time as timestamp)").alias("ingest_time"),
        F.col("batch_id").cast("string"),
    )
    return selected.select([field.name for field in SILVER_QUARANTINE_SCHEMA.fields])


def _optional_string(df: DataFrame, column_name: str) -> F.Column:
    if column_name in df.columns:
        return F.col(column_name).cast("string").alias(column_name)
    return F.lit(None).cast("string").alias(column_name)


def _deduplicate(valid_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    dedup_columns = [
        F.col("user_id_typed"),
        F.col("user_session_clean"),
        F.col("event_timestamp_typed"),
        F.col("event_type_clean"),
        F.col("product_id_typed"),
    ]
    window = Window.partitionBy(*dedup_columns).orderBy(
        F.col("source_file_clean").asc_nulls_last(),
        F.col("batch_id_clean").asc_nulls_last(),
        F.expr("try_cast(ingest_time as timestamp)").asc_nulls_last(),
    )
    ranked = valid_df.withColumn("_dedup_rank", F.row_number().over(window))
    clean = ranked.filter(F.col("_dedup_rank") == 1).drop("_dedup_rank")
    duplicates = (
        ranked.filter(F.col("_dedup_rank") > 1)
        .drop("_dedup_rank")
        .withColumn("hard_rule_id", F.lit("DUPLICATE_RECORD"))
        .withColumn("hard_failed_columns", F.array(*[F.lit(key) for key in SILVER_DEDUP_KEYS]))
        .withColumn("hard_failed_reason", F.lit("Record duplicates an earlier Silver event key."))
    )
    return clean, duplicates


def _select_log_silver(df: DataFrame) -> DataFrame:
    selected = df.select(
        F.col("event_time_clean").alias("event_time"),
        F.col("event_timestamp_typed").alias("event_timestamp"),
        F.col("event_date_canonical").alias("event_date"),
        F.col("event_type_clean").alias("event_type"),
        F.col("product_id_typed").alias("product_id"),
        F.col("category_id_typed").alias("category_id"),
        F.col("category_code_clean").alias("category_code"),
        F.col("category_l1"),
        F.col("category_l2"),
        F.col("category_l3"),
        F.col("brand_clean").alias("brand"),
        F.col("price_typed").alias("price"),
        F.col("user_id_typed").alias("user_id"),
        F.col("user_session_clean").alias("user_session"),
        F.col("is_category_code_missing"),
        F.col("is_brand_missing"),
        F.col("dq_warning_count"),
        F.col("dq_warnings"),
        F.col("source_file_clean").alias("source_file"),
        F.expr("try_cast(ingest_time as timestamp)").alias("ingest_time"),
        F.col("batch_id_clean").alias("batch_id"),
        F.current_timestamp().alias("silver_processed_time"),
    )
    return selected.select([field.name for field in LOG_TRACKING_SILVER_SCHEMA.fields])


def _select_purchase_silver(df: DataFrame) -> DataFrame:
    selected = df.select(
        F.col("event_time_clean").alias("event_time"),
        F.col("event_timestamp_typed").alias("event_timestamp"),
        F.col("event_date_canonical").alias("event_date"),
        F.col("event_type_clean").alias("event_type"),
        F.col("product_id_typed").alias("product_id"),
        F.col("category_id_typed").alias("category_id"),
        F.col("category_code_clean").alias("category_code"),
        F.col("category_l1"),
        F.col("category_l2"),
        F.col("category_l3"),
        F.col("brand_clean").alias("brand"),
        F.col("price_typed").alias("price"),
        F.col("user_id_typed").alias("user_id"),
        F.col("user_session_clean").alias("user_session"),
        F.col("first_event_date_typed").alias("first_event_date"),
        F.col("cohort_week_start"),
        F.col("cohort_week_end"),
        F.col("cohort_week_number"),
        F.col("cohort_week_label"),
        F.coalesce(F.col("week_after_canonical"), F.col("week_after_typed")).cast("int").alias(
            "week_after"
        ),
        F.col("is_cohort_week_mismatch"),
        F.col("is_category_code_missing"),
        F.col("is_brand_missing"),
        F.col("dq_warning_count"),
        F.col("dq_warnings"),
        F.col("source_file_clean").alias("source_file"),
        F.expr("try_cast(ingest_time as timestamp)").alias("ingest_time"),
        F.col("batch_id_clean").alias("batch_id"),
        F.current_timestamp().alias("silver_processed_time"),
    )
    return selected.select([field.name for field in PURCHASE_BEHAVIOR_SILVER_SCHEMA.fields])


def clean_log_tracking_to_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Clean log tracking Bronze rows into Silver rows plus quarantine rows."""
    prepared = (
        _normalize_common_columns(bronze_df, "log_tracking")
        .transform(_with_category_fields)
        .transform(
            lambda df: _with_common_hard_rule(
                df, require_price=False, require_source_event_date=False
            )
        )
        .transform(_with_common_warnings)
    )
    hard_quarantine = _quarantine_from(prepared.filter(F.col("hard_rule_id").isNotNull()))
    valid = prepared.filter(F.col("hard_rule_id").isNull())
    deduped, duplicates = _deduplicate(valid)
    duplicate_quarantine = _quarantine_from(duplicates)
    return _select_log_silver(deduped), hard_quarantine.unionByName(duplicate_quarantine)


def clean_purchase_behavior_to_silver(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Clean purchase behavior Bronze rows into Silver rows plus quarantine rows."""
    prepared = (
        _normalize_common_columns(bronze_df, "purchase_behavior")
        .transform(_with_category_fields)
        .transform(
            lambda df: _with_common_hard_rule(
                df, require_price=True, require_source_event_date=True
            )
        )
        .transform(_with_common_warnings)
        .transform(_with_purchase_dates_and_warnings)
    )
    hard_quarantine = _quarantine_from(prepared.filter(F.col("hard_rule_id").isNotNull()))
    valid = prepared.filter(F.col("hard_rule_id").isNull())
    deduped, duplicates = _deduplicate(valid)
    duplicate_quarantine = _quarantine_from(duplicates)
    return _select_purchase_silver(deduped), hard_quarantine.unionByName(duplicate_quarantine)


def build_silver_pipeline(spark: SparkSession, output_root: Path | None = None) -> None:
    """Build Silver Parquet datasets from Bronze Parquet inputs."""
    root = output_root or warehouse_dir() / "silver"
    log_bronze = read_parquet(spark, bronze_dir() / "log_tracking")
    purchase_bronze = read_parquet(spark, bronze_dir() / "purchase_behavior")

    log_silver, log_quarantine = clean_log_tracking_to_silver(log_bronze)
    purchase_silver, purchase_quarantine = clean_purchase_behavior_to_silver(purchase_bronze)
    quarantine = log_quarantine.unionByName(purchase_quarantine)

    write_parquet(log_silver, root / "log_tracking", mode="overwrite", partition_by=["event_date"])
    write_parquet(
        purchase_silver, root / "purchase_behavior", mode="overwrite", partition_by=["event_date"]
    )
    write_parquet(quarantine, root / "quarantine", mode="overwrite")
