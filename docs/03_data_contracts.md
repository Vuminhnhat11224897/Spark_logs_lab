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
- Renaming profile output columns without updating docs and any downstream review notebooks.
