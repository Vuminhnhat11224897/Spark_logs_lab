# Architecture

## Goal

Local Spark training project prepared for Bronze, Silver, Gold, quality, audit, benchmark, and serving phases.

## Current Scope Before Phase 1

- Raw data is preserved under `data/raw/`.
- Spark Docker Compose is preserved at the project root.
- Raw schemas are defined and validated against the current CSV headers.
- Bronze schemas are defined for the first batch ingestion output.
- Iceberg, Trino, and Flink runtime logic are not implemented before the batch pipeline is stable.

## High-Level Flow

```text
data/raw CSV
  -> jobs/00_1_check_raw_files.py
  -> jobs/01_build_bronze.py
  -> src/spark_log_lab/pipelines/
  -> warehouse/bronze/
  -> warehouse/silver/
  -> warehouse/gold/
  -> quality + audit
  -> benchmark/report outputs
```

## Phase 0 Contract

Phase 0 owns project structure and source-data validation only. It must not mutate raw data.

| Area | Current Decision |
|---|---|
| Project root | `/home/zseefvhu12/projects/spark_training` |
| Package root | `src/spark_log_lab` |
| Raw storage | `data/raw/` |
| Bronze storage | `warehouse/bronze/` |
| Raw schema types | `StringType` for all source columns |
| Bronze metadata | `source_file`, `ingest_time`, `batch_id` |
| Ingestion timestamp | `ingest_time` |

## Package Boundaries

| Area | Responsibility |
|---|---|
| `jobs/` | Thin command-line entrypoints |
| `src/spark_log_lab/common/` | Config, paths, Spark session, logging, CLI helpers |
| `src/spark_log_lab/schemas/` | Raw/Bronze/Silver/Gold schema definitions |
| `src/spark_log_lab/io/` | Readers and writers |
| `src/spark_log_lab/pipelines/` | Main transformation logic |
| `src/spark_log_lab/quality/` | Data quality checks and result writer |
| `src/spark_log_lab/metadata/` | Run context, audit, lineage |
| `src/spark_log_lab/benchmarks/` | Spark benchmark logic |
| `src/spark_log_lab/serving/` | Future query serving helpers |
| `src/spark_log_lab/streaming/` | Future streaming demo helpers |
