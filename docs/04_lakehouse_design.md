# Lakehouse Design

## Current Phase

Phase 0 prepares project structure, Raw schemas, Bronze schemas, and validation commands. It does
not migrate data to Iceberg and does not add Trino or Flink runtime logic.

## Planned Layers

| Layer | Storage | Purpose |
|---|---|---|
| Raw | `data/raw/` | Original CSV files kept unchanged |
| Bronze | `warehouse/bronze/` | Parsed file ingestion output with ingestion metadata |
| Silver | `warehouse/silver/` | Cleaned and deduplicated event data |
| Gold | `warehouse/gold/` | Business marts and aggregate tables |

## Bronze Design

Bronze should be implemented as Parquet output in Phase 1.

Expected responsibilities:

- Read Raw CSV files with explicit Raw schemas.
- Preserve source fields as inspectable values.
- Parse event timestamps and dates into typed derived columns.
- Add `source_file`, `ingest_time`, and `batch_id`.
- Write rerunnable output under `warehouse/bronze/`.

## Rerun Principle

Future batch jobs should be idempotent by `event_date` or `batch_id`.
