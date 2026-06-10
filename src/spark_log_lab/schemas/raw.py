"""Raw input schema definitions."""
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

LOG_TRACKING_SCHEMA = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_session", StringType(), True)
])

PURCHASE_BEHAVIOR_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", StringType(), True),
    StructField("user_session", StringType(), True),
    StructField("event_date", TimestampType(), True),
    StructField("first_event_date", TimestampType(), True),
    StructField("start_of_week", TimestampType(), True),
    StructField("week_number", StringType(), True),
    StructField("end_of_week", TimestampType(), True),
    StructField("week_text", StringType(), True),
    StructField("cohort_index_week", StringType(), True),
    StructField("week_after", StringType(), True)
])
