# Architecture

## Goal

Local Spark training project prepared for Bronze, Silver, Gold, quality, audit, benchmark, and serving phases.

## Current Scope Before Phase 1

- Raw data is preserved under `data/raw/`.
- Spark Docker Compose is preserved at the project root.
- Iceberg, Trino, and Flink runtime logic are not implemented in this phase.

## High-Level Flow

```text
data/raw CSV
  -> jobs/
  -> src/spark_log_lab/pipelines/
  -> warehouse/bronze
  -> warehouse/silver
  -> warehouse/gold
  -> quality + audit
  -> benchmark/report outputs
```

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
