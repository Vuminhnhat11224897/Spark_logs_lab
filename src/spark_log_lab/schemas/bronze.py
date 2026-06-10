"""Bronze output schema definitions."""
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
LOG_TRACKING_BRONZE_SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_session", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("ingest_time", TimestampType(), True),
    StructField("event_date", DateType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("batch_id", StringType(), True)
])

PURCHASE_BEHAVIOR_BRONZE_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", StringType(), True),
    StructField("user_session", StringType(), True),
    StructField("event_date", DateType(), True),
    StructField("first_event_date", DateType(), True),
    StructField("start_of_week", DateType(), True),
    StructField("week_number", StringType(), True),
    StructField("end_of_week", DateType(), True),
    StructField("week_text", StringType(), True),
    StructField("cohort_index_week", StringType(), True),
    StructField("week_after", StringType(), True),
    StructField("source_file", StringType(), True),
    StructField("ingest_time", TimestampType(), True),
    StructField("batch_id", StringType(), True)  
])