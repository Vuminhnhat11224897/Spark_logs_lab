# Roadmap

## Phase 0: Project Foundation

Status: complete.

- Clean project root: `/home/zseefvhu12/projects/spark_training`.
- Preserve Raw CSV files under `data/raw/`.
- Preserve Docker Compose at project root.
- Define Raw schemas with CSV-header order.
- Define Bronze schemas with source fields, derived fields, and metadata.
- Validate Raw schemas through Spark submit.

## Phase 1: Bronze Ingestion

- Wire thin jobs to package modules.
- Implement `jobs/01_build_bronze.py`.
- Read `data/raw/01-log-tracking.csv` and `data/raw/02-purchase-behavior.csv` with explicit schemas.
- Add typed `event_timestamp` and `event_date` fields.
- Add metadata fields: `source_file`, `ingest_time`, `batch_id`.
- Write Bronze Parquet output under `warehouse/bronze/`.
- Add Bronze output checks for schema, sample rows, and required metadata.
- Keep jobs importable and testable.

## Phase 2: Quality and Audit

- Add runnable quality job.
- Record audit rows for pipeline runs.

## Phase 3: Gold and Benchmark

- Build Gold marts.
- Add Spark benchmark runner.

## Later Phases

- Add Trino serving only after Gold exists.
- Add Flink demo only after batch workflow is stable.
