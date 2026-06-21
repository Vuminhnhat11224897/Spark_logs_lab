# Data Contracts

## Raw Layer Contract

Raw files are source inputs. They must be kept unchanged under `data/raw/`.

Raw schemas intentionally use `StringType` for every source column. Parsing and casting belong in
Bronze/Silver transformations so malformed source values can be inspected instead of silently
becoming null during CSV read.

### log_tracking

- Path: `data/raw/01-log-tracking.csv`
- Schema: `src/spark_log_lab/schemas/raw.py::LOG_TRACKING_SCHEMA`
- Field order must match the CSV header exactly.

Required fields:

- `event_time`
- `event_type`
- `product_id`
- `user_id`
- `user_session`

### purchase_behavior

- Path: `data/raw/02-purchase-behavior.csv`
- Schema: `src/spark_log_lab/schemas/raw.py::PURCHASE_BEHAVIOR_SCHEMA`
- Field order must match the CSV header exactly.
- The first physical CSV field is `user_id`; do not reorder the Raw schema into a logical event-first layout.

Required fields:

- `user_id`
- `event_time`
- `event_type`
- `product_id`
- `user_session`
- `event_date`

## Bronze Layer Contract

Bronze is the first parsed ingestion output. It should preserve source columns, add parse-friendly
date/time columns, and append ingestion metadata.

### Current Bronze Fields

`LOG_TRACKING_BRONZE_SCHEMA`:

- Source fields: `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`
- Derived fields: `event_timestamp`, `event_date`
- Metadata fields: `source_file`, `ingest_time`, `batch_id`

`PURCHASE_BEHAVIOR_BRONZE_SCHEMA`:

- Source fields: `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session`
- Derived fields: `event_timestamp`, `event_date`, `first_event_date`, `start_of_week`, `week_number`, `end_of_week`, `week_text`, `cohort_index_week`, `week_after`
- Metadata fields: `source_file`, `ingest_time`, `batch_id`

### Bronze Quality Rules

- Row count must be greater than zero.
- Required Raw fields must exist before transformation.
- `event_time` should parse into `event_timestamp` when valid.
- `event_date` should be available as `DateType`.
- `price` should remain inspectable even if later numeric parsing fails.
- `source_file`, `ingest_time`, and `batch_id` must be populated.

## Silver Layer Contract

Silver is the cleaned analytical layer built from Bronze Parquet. It normalizes analytical fields,
routes hard failures to quarantine, and keeps softer quality issues as warning flags on retained
rows.

### Current Silver Fields

`LOG_TRACKING_SILVER_SCHEMA`:

- Event identity and time: `event_time`, `event_timestamp`, `event_date`, `event_type`
- Product dimensions: `product_id`, `category_id`, `category_code`, `category_l1`, `category_l2`, `category_l3`, `brand`
- Measures: `price`
- User/session identity: `user_id`, `user_session`
- Data quality flags: `is_category_code_missing`, `is_brand_missing`, `dq_warning_count`, `dq_warnings`
- Metadata fields: `source_file`, `ingest_time`, `batch_id`, `silver_processed_time`

`PURCHASE_BEHAVIOR_SILVER_SCHEMA`:

- Event identity and time: `event_time`, `event_timestamp`, `event_date`, `event_type`
- Product dimensions: `product_id`, `category_id`, `category_code`, `category_l1`, `category_l2`, `category_l3`, `brand`
- Measures: `price`
- User/session identity: `user_id`, `user_session`
- Clean cohort fields: `first_event_date`, `cohort_week_start`, `cohort_week_end`, `cohort_week_number`, `cohort_week_label`, `week_after`, `is_cohort_week_mismatch`
- Data quality flags: `is_category_code_missing`, `is_brand_missing`, `dq_warning_count`, `dq_warnings`
- Metadata fields: `source_file`, `ingest_time`, `batch_id`, `silver_processed_time`

`SILVER_QUARANTINE_SCHEMA`:

- Rule metadata: `dataset`, `rule_id`, `rule_name`, `severity`, `failed_columns`, `failed_reason`, `quarantined_at`
- Original event fields retained as strings for inspection
- Purchase/cohort raw fields retained as strings where present
- Lineage fields: `source_file`, `ingest_time`, `batch_id`

### Silver Required Columns

`LOG_TRACKING_SILVER_REQUIRED_COLUMNS`:

- `event_timestamp`
- `event_type`
- `product_id`
- `user_id`
- `user_session`

`PURCHASE_BEHAVIOR_SILVER_REQUIRED_COLUMNS`:

- `event_timestamp`
- `event_date`
- `event_type`
- `product_id`
- `user_id`
- `user_session`
- `price`

### Silver Deduplication Contract

Shared deduplication keys are currently:

- `user_id`
- `user_session`
- `event_timestamp`
- `event_type`
- `product_id`

Duplicate records are routed to Silver quarantine with `rule_id = DUPLICATE_RECORD`. One
deterministic row per deduplication key is retained in the main Silver dataset.

### Silver Date and Cohort Contract

Silver `event_date` is canonical and is always derived as `to_date(event_timestamp)`. Source
`event_date` from `purchase_behavior` is used only for validation.

For `purchase_behavior`, Silver recomputes cohort week fields from canonical `event_date`:

- `cohort_week_start`: Monday of the week containing `event_date`
- `cohort_week_end`: `cohort_week_start + 6 days`
- `cohort_week_number`: ISO-style Spark `weekofyear(event_date)`
- `cohort_week_label`: `WNN`

Source week fields from Bronze are compared against these canonical values. Mismatches are retained
as warnings, not hard failures.

### Silver Hard Quarantine Rules

- `MISSING_REQUIRED_FIELD`
- `EVENT_TIMESTAMP_PARSE_FAILED`
- `EVENT_DATE_PARSE_FAILED`
- `INVALID_EVENT_TYPE`
- `INVALID_REQUIRED_ID_CAST`
- `INVALID_PURCHASE_PRICE`
- `DUPLICATE_RECORD`

### Silver Warning Rules

- `CATEGORY_CODE_MISSING`
- `BRAND_MISSING`
- `CATEGORY_ID_CAST_FAILED`
- `PRICE_ZERO_OR_NEGATIVE`
- `SOURCE_COHORT_WEEK_MISMATCH`
- `EVENT_DATE_MISMATCH_WITH_TIMESTAMP`

### Silver Quality Expectations

- `price` is normalized to `decimal(18,2)` for analytical use.
- Category hierarchy fields `category_l1`, `category_l2`, and `category_l3` are materialized from `category_code`.
- `event_date` is derived from `event_timestamp`; source `event_date` is not canonical.
- Purchase cohort week start/end fields are recomputed in Silver using Monday-to-Sunday weeks.
- Missing category code and brand values are tracked explicitly through boolean flags and warning arrays.
- `silver_processed_time` records when the Silver row was produced.
- Quarantined records preserve source values as strings so rule failures stay inspectable.

## Profile Output Contract

Profiling is observational only. It reads existing layer datasets and writes column-level metrics
without rewriting input files.

Current profile snapshots:

- Raw current snapshot: `results/data_profiles/raw_<dataset>_profile.csv`
- Bronze current snapshot: `results/data_profiles/bronze_<dataset>_profile.csv`
- Versioned snapshot: `results/data_profiles/<layer>_<dataset>_profile_<profile-version>.csv`

Current profiler fields:

- Run identity: `run_id`, `layer`, `dataset`, `column_name`, `profiled_at`
- Type and volume: `data_type`, `row_count`
- Completeness: `null_count`, `null_rate`, `empty_count`, `empty_rate`
- Cardinality and distribution: `approx_distinct_count`, `mode_value`, `mode_count`
- Range and numeric stats: `min_value`, `max_value`, `avg_value`, `stddev_value`

Current profile snapshots are overwritten on each run for the same layer/dataset. Use
`--profile-version <version>` to create a separate snapshot instead of mixing repeated rows into
the current profile file.

## Breaking Changes

- Removing or renaming required Raw columns.
- Reordering Raw schema fields so they no longer match the physical CSV header.
- Changing timestamp format without updating parser logic.
- Changing `event_type` semantics.
- Changing `price` from numeric-compatible text to non-numeric text.
- Removing Bronze metadata fields: `source_file`, `ingest_time`, or `batch_id`.
- Removing or renaming Silver required columns without updating the contract constants.
- Changing Silver deduplication keys without updating downstream validation logic.
- Removing Silver quarantine lineage or rule metadata fields.
- Renaming profile output columns without updating docs and any downstream review notebooks.
