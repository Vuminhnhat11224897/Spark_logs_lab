# Spark Training

Spark training project for local batch data engineering practice.

The repository has completed Phase 0 cleanup and is ready for Phase 1. It keeps the original
raw input data unchanged, preserves the existing Docker Compose Spark cluster, and provides a
clean package layout for Raw -> Bronze -> Silver -> Gold development.

## Current Layout

```text
spark_training/
├── src/
│   └── spark_log_lab/
│       ├── common/
│       ├── schemas/
│       ├── io/
│       ├── pipelines/
│       ├── quality/
│       ├── metadata/
│       ├── benchmarks/
│       ├── serving/
│       └── streaming/
├── configs/
├── data/
│   ├── raw/
│   ├── samples/
│   └── checkpoint/
├── docker/
│   ├── flink/
│   ├── spark/
│   └── trino/
├── docs/
├── jobs/
├── results/
├── sql/
├── tests/
├── warehouse/
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── .env.example
└── .gitignore
```

## Preserved Files

- `data/raw/01-log-tracking.csv`
- `data/raw/02-purchase-behavior.csv`
- `docker-compose.yml`
- `.env`

## Phase 0 Status

Phase 0 is complete.

- Project root: `/home/zseefvhu12/projects/spark_training`
- Raw files are preserved under `data/raw/`.
- Docker Compose is preserved at the project root.
- Raw schemas match the physical CSV headers and field order.
- Bronze schemas are defined with source fields first, derived fields next, and metadata fields last.
- `ingest_time` is the standard ingestion timestamp metadata field.
- Raw schema validation can be submitted to the Spark master through `scripts/submit_raw_check.sh`.

## Phase 1 Starting Point

The root project folder is now:

```text
/home/zseefvhu12/projects/spark_training
```

The Python package is:

```text
src/spark_log_lab
```

Phase 1 should implement the first real pipeline step: read Raw CSV files, parse basic event
date/time fields, add metadata, and write Bronze Parquet output.

No Iceberg, Trino, or Flink runtime logic is implemented before the batch workflow is stable.

## Quick Checks

```bash
python3 -m pytest -q
PYTHONPATH=src python3 -m py_compile src/spark_log_lab/schemas/raw.py src/spark_log_lab/schemas/bronze.py jobs/00_check_raw_files.py
./scripts/submit_raw_check.sh --sample-size 1 --null-sample-size 5
```
