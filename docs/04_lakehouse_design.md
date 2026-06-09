# Lakehouse Design

## Current Phase

This phase prepares structure only. It does not migrate data to Iceberg and does not add Trino or Flink runtime logic.

## Planned Layers

| Layer | Storage | Purpose |
|---|---|---|
| Raw | `data/raw/` | Original CSV files kept unchanged |
| Bronze | `warehouse/bronze/` | Parsed file ingestion output |
| Silver | `warehouse/silver/` | Cleaned and deduplicated event data |
| Gold | `warehouse/gold/` | Business marts and aggregate tables |

## Rerun Principle

Future batch jobs should be idempotent by `event_date` or `batch_id`.
