"""Silver output schema definitions."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PRICE_TYPE = DecimalType(18, 2)


LOG_TRACKING_SILVER_SCHEMA = StructType(
    [
        # Event identity and time
        StructField("event_time", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("event_date", DateType(), True),
        StructField("event_type", StringType(), True),

        # Product dimensions
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("category_l1", StringType(), True),
        StructField("category_l2", StringType(), True),
        StructField("category_l3", StringType(), True),
        StructField("brand", StringType(), True),

        # Measures
        StructField("price", PRICE_TYPE, True),

        # User/session identity
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),

        # Data quality flags
        StructField("is_category_code_missing", BooleanType(), True),
        StructField("is_brand_missing", BooleanType(), True),
        StructField("dq_warning_count", IntegerType(), True),
        StructField("dq_warnings", ArrayType(StringType()), True),

        # Lineage and processing metadata
        StructField("source_file", StringType(), True),
        StructField("ingest_time", TimestampType(), True),
        StructField("batch_id", StringType(), True),
        StructField("silver_processed_time", TimestampType(), True),
    ]
)


PURCHASE_BEHAVIOR_SILVER_SCHEMA = StructType(
    [
        # Event identity and time
        StructField("event_time", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("event_date", DateType(), True),
        StructField("event_type", StringType(), True),

        # Product dimensions
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("category_l1", StringType(), True),
        StructField("category_l2", StringType(), True),
        StructField("category_l3", StringType(), True),
        StructField("brand", StringType(), True),

        # Measures
        StructField("price", PRICE_TYPE, True),

        # User/session identity
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),

        # Clean cohort fields recomputed in Silver
        StructField("first_event_date", DateType(), True),
        StructField("cohort_week_start", DateType(), True),
        StructField("cohort_week_end", DateType(), True),
        StructField("cohort_week_number", IntegerType(), True),
        StructField("cohort_week_label", StringType(), True),
        StructField("week_after", IntegerType(), True),
        StructField("is_cohort_week_mismatch", BooleanType(), True),

        # Data quality flags
        StructField("is_category_code_missing", BooleanType(), True),
        StructField("is_brand_missing", BooleanType(), True),
        StructField("dq_warning_count", IntegerType(), True),
        StructField("dq_warnings", ArrayType(StringType()), True),

        # Lineage and processing metadata
        StructField("source_file", StringType(), True),
        StructField("ingest_time", TimestampType(), True),
        StructField("batch_id", StringType(), True),
        StructField("silver_processed_time", TimestampType(), True),
    ]
)


SILVER_QUARANTINE_SCHEMA = StructType(
    [
        # Quarantine metadata
        StructField("dataset", StringType(), True),
        StructField("rule_id", StringType(), True),
        StructField("rule_name", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("failed_columns", ArrayType(StringType()), True),
        StructField("failed_reason", StringType(), True),
        StructField("quarantined_at", TimestampType(), True),

        # Original event fields, kept as strings for inspection
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_session", StringType(), True),

        # Purchase/cohort raw fields, nullable for log_tracking
        StructField("event_date", StringType(), True),
        StructField("first_event_date", StringType(), True),
        StructField("start_of_week", StringType(), True),
        StructField("week_number", StringType(), True),
        StructField("end_of_week", StringType(), True),
        StructField("week_text", StringType(), True),
        StructField("cohort_index_week", StringType(), True),
        StructField("week_after", StringType(), True),

        # Lineage
        StructField("source_file", StringType(), True),
        StructField("ingest_time", TimestampType(), True),
        StructField("batch_id", StringType(), True),
    ]
)


LOG_TRACKING_SILVER_REQUIRED_COLUMNS = [
    "event_timestamp",
    "event_type",
    "product_id",
    "user_id",
    "user_session",
]


PURCHASE_BEHAVIOR_SILVER_REQUIRED_COLUMNS = [
    "event_timestamp",
    "event_date",
    "event_type",
    "product_id",
    "user_id",
    "user_session",
    "price",
]


SILVER_DEDUP_KEYS = [
    "user_id",
    "user_session",
    "event_timestamp",
    "event_type",
    "product_id",
]