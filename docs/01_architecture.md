# Architecture

## Goal

Local Spark training project prepared for Raw profiling, Bronze ingestion, Silver cleaning, Gold
marts, quality, audit, benchmark, and serving phases.

## Current Scope

- Raw data is preserved under `data/raw/`.
- Spark Docker Compose is preserved at the project root.
- Raw schemas are defined and validated against the current CSV headers.
- Raw profiling writes column-level metrics for source CSVs.
- Bronze schemas and the Bronze CSV-to-Parquet pipeline are defined for the first batch ingestion output.
- Silver schemas, cleaning transforms, quarantine handling, and the Silver build job are implemented.
- Gold, serving, and streaming modules mostly remain placeholders until the batch workflow is
  extended further.
- Spark benchmark scaffolding exists, but project-specific benchmark runs should be expanded after
  Gold marts are available.
- Iceberg, Trino, and Flink runtime logic are not implemented before the batch pipeline is stable.

## High-Level Flow

```text
data/raw CSV
  -> jobs/00_1_check_raw_files.py
  -> jobs/00_2_profile_raw.py
  -> jobs/01_build_bronze.py
  -> src/spark_log_lab/pipelines/
  -> warehouse/bronze/
  -> warehouse/silver/
  -> warehouse/gold/
  -> quality + audit
  -> benchmark/report outputs
```

## Phase 0 Contract

Foundation work owns project structure, source-data validation, Raw profiling, and Raw data
preservation. It must not mutate raw data.

| Area | Current Decision |
|---|---|
| Project root | `/home/zseefvhu12/projects/spark_training` |
| Package root | `src/spark_log_lab` |
| Raw storage | `data/raw/` |
| Raw profiles | `results/data_profiles/raw_<dataset>_profile.csv` |
| Bronze storage | `warehouse/bronze/` |
| Silver storage | `warehouse/silver/` |
| Silver outputs | `log_tracking/`, `purchase_behavior/`, `quarantine/` |
| Bronze profiles | `results/data_profiles/bronze_<dataset>_profile.csv` |
| Raw schema types | `StringType` for all source columns |
| Bronze metadata | `source_file`, `ingest_time`, `batch_id` |
| Silver metadata | `source_file`, `ingest_time`, `batch_id`, `silver_processed_time` |
| Silver quarantine | `SILVER_QUARANTINE_SCHEMA` keeps rejected rows as strings plus rule metadata |
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
