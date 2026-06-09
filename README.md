# Spark Training

Spark training project scaffold for batch data engineering practice.

This repository is prepared for Phase 1. It keeps the raw input data and the Docker Compose file
that already runs the local Spark cluster.

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

## Phase 1 Starting Point

The root project folder is now:

```text
/home/zseefvhu12/projects/spark_training
```

The Python package is:

```text
src/spark_log_lab
```

No Iceberg, Trino, or Flink runtime logic is implemented in this phase.
