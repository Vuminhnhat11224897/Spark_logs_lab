# Lakehouse Design

## Current Phase

The repository has completed the Raw/Bronze foundation work and now includes a runnable Silver
cleaning pipeline. It still does not migrate data to Iceberg and does not add Trino or Flink
runtime logic.

## Planned Layers

| Layer | Storage | Purpose |
|---|---|---|
| Raw | `data/raw/` | Original CSV files kept unchanged |
| Bronze | `warehouse/bronze/` | Parsed file ingestion output with ingestion metadata |
| Silver | `warehouse/silver/` | Cleaned and deduplicated event data |
| Gold | `warehouse/gold/` | Business marts and aggregate tables |

## Bronze Design

Bronze is implemented as a Parquet-oriented ingestion layer.

Expected responsibilities:

- Read Raw CSV files with explicit Raw schemas.
- Preserve source fields as inspectable values.
- Parse event timestamps and dates into typed derived columns.
- Add `source_file`, `ingest_time`, and `batch_id`.
- Write rerunnable output under `warehouse/bronze/`.

## Silver Design

Silver contracts are defined in `src/spark_log_lab/schemas/silver.py`, and the runnable cleaning
logic lives in `src/spark_log_lab/pipelines/silver_clean_parquet.py`.

Expected responsibilities:

- Read Bronze Parquet with typed timestamp/date fields already available.
- Normalize `price` to `decimal(18,2)`.
- Split `category_code` into `category_l1`, `category_l2`, and `category_l3`.
- Set canonical `event_date` from `to_date(event_timestamp)`.
- Recompute clean Monday-to-Sunday cohort week fields for `purchase_behavior`.
- Track warning flags such as missing category code or brand values.
- Deduplicate rows with the shared key set `user_id`, `user_session`, `event_timestamp`, `event_type`, `product_id`.
- Route failed records into a quarantine dataset that preserves original values as strings for inspection.
- Add `silver_processed_time` in addition to lineage metadata from earlier layers.

Hard quarantine rules:

- `MISSING_REQUIRED_FIELD`
- `EVENT_TIMESTAMP_PARSE_FAILED`
- `EVENT_DATE_PARSE_FAILED`
- `INVALID_EVENT_TYPE`
- `INVALID_REQUIRED_ID_CAST`
- `INVALID_PURCHASE_PRICE`
- `DUPLICATE_RECORD`

Warning-only rules:

- `CATEGORY_CODE_MISSING`
- `BRAND_MISSING`
- `CATEGORY_ID_CAST_FAILED`
- `PRICE_ZERO_OR_NEGATIVE`
- `SOURCE_COHORT_WEEK_MISMATCH`
- `EVENT_DATE_MISMATCH_WITH_TIMESTAMP`

## Rerun Principle

Batch jobs should be rerunnable. Current Bronze and Silver builds use overwrite mode; future
incremental jobs should scope reruns by `event_date` or `batch_id`.
