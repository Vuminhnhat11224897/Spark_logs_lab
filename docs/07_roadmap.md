# Roadmap

## Phase 0: Project Foundation

Status: complete.

- Clean project root: `/home/zseefvhu12/projects/spark_training`.
- Preserve Raw CSV files under `data/raw/`.
- Preserve Docker Compose at project root.
- Define Raw schemas with CSV-header order.
- Define Bronze schemas with source fields, derived fields, and metadata.
- Validate Raw schemas through Spark submit.

## Phase 1: Raw Exploration And Bronze Ingestion

- Wire thin jobs to package modules.
- Implement `jobs/01_build_bronze.py`.
- Read `data/raw/01-log-tracking.csv` and `data/raw/02-purchase-behavior.csv` with explicit schemas.
- Add Raw profiling with `jobs/00_2_profile_raw.py`, `scripts/submit_raw_profile.sh`, and
  per-dataset snapshots under `results/data_profiles/`.
- Add Bronze profiling with `jobs/01_2_profile_bronze.py`, `scripts/submit_bronze_profile.sh`, and
  per-dataset snapshots under `results/data_profiles/`.
- Add small sample-data generation for notebook exploration without committing generated samples.
- Add typed `event_timestamp` and `event_date` fields.
- Add metadata fields: `source_file`, `ingest_time`, `batch_id`.
- Write Bronze Parquet output under `warehouse/bronze/`.
- Add Bronze output checks for schema, sample rows, and required metadata.
- Keep jobs importable and testable.

## Phase 2: Silver Cleaning

Status: complete.

- Done: define Silver schemas from current Raw/Bronze observations.
- Done: define Silver required-column constants and shared deduplication keys.
- Done: define a Silver quarantine schema for rejected records.
- Done: implement `jobs/02_build_silver.py` and `src/spark_log_lab/pipelines/silver_clean_parquet.py`.
- Done: read Bronze Parquet and write cleaned Silver Parquet under `warehouse/silver/`.
- Done: standardize event fields, cast analysis-ready numeric/date types, preserve lineage metadata.
- Done: add transform-level Silver tests for canonical dates, warnings, hard quarantine, and duplicate handling.
- Done: add `scripts/submit_silver_build.sh`.
- Done: add `jobs/02_1_check_silver.py` and `scripts/submit_silver_check.sh`.

## Phase 3: Quality and Audit

- Add runnable quality job over Silver outputs.
- Record audit rows for pipeline runs.
- Keep unfinished entrypoints failing with `FEATURE_NOT_IMPLEMENTED` until they run real work.

## Phase 4: Gold and Benchmark

- Build Gold marts.
- Add Spark benchmark runner.

## Later Phases

- Add Trino serving only after Gold exists.
- Add Flink demo only after batch workflow is stable.
