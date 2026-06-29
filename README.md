# Spark Training

Spark training project for local batch data engineering practice.

The repository has completed the foundation work and now includes the first batch workflow
building blocks: Raw schema validation, Raw profiling, sample-data generation for notebook
exploration, Bronze Parquet ingestion, and Silver cleaning. It keeps the original raw input data
unchanged, preserves the Docker Compose Spark cluster, and keeps runtime code focused on the
implemented Raw -> Bronze -> Silver batch workflow.

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
│       └── metadata/
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

## Current Status

Foundation, Bronze ingestion, and Silver cleaning are in place.

- Project root: `/home/zseefvhu12/projects/spark_training`
- Raw files are preserved under `data/raw/`.
- Docker Compose is preserved at the project root.
- Raw schemas match the physical CSV headers and field order.
- Raw profile snapshots can be written under `results/data_profiles/`.
- Bronze profile snapshots can be written under `results/data_profiles/`.
- Bronze schemas are defined with source fields first, derived fields next, and metadata fields last.
- Silver schemas are defined for `log_tracking`, `purchase_behavior`, and quarantine outputs.
- Silver contracts declare required columns and shared deduplication keys.
- Silver build reads Bronze Parquet, writes cleaned Silver Parquet, and writes quarantine records.
- Silver canonicalizes `event_date` from `event_timestamp`, recomputes purchase cohort weeks, and
  separates hard quarantine failures from warning-only data quality issues.
- `ingest_time` is the standard ingestion timestamp metadata field.
- Raw schema validation can be submitted to the Spark master through `scripts/submit_raw_check.sh`.
- Bronze build and Bronze output checks can be submitted through `scripts/submit_bronze_build.sh`
  and `scripts/submit_bronze_check.sh`.
- Silver build and Silver output checks can be submitted through `scripts/submit_silver_build.sh`
  and `scripts/submit_silver_check.sh`.

## Next Development Focus

The root project folder is now:

```text
/home/zseefvhu12/projects/spark_training
```

The Python package is:

```text
src/spark_log_lab
```

The next foundation step is runnable Silver quality checks and consistent audit records. Gold marts
come after the Silver batch workflow is checked and documented.

No Iceberg, Trino, or Flink runtime logic is implemented before the batch workflow is stable.

## Quick Checks

```bash
python3 -m pytest -q
PYTHONPATH=src python3 -m py_compile src/spark_log_lab/schemas/raw.py src/spark_log_lab/schemas/bronze.py jobs/00_1_check_raw_files.py jobs/01_1_check_bronze.py
PYTHONPATH=src python3 -m py_compile src/spark_log_lab/schemas/silver.py src/spark_log_lab/pipelines/silver_clean_parquet.py jobs/02_build_silver.py
./scripts/submit_raw_check.sh --sample-size 1 --null-sample-size 5
./scripts/submit_raw_profile.sh --dataset all
./scripts/submit_bronze_build.sh --batch-id dev_001
./scripts/submit_bronze_check.sh --sample-size 1 --null-sample-size 5
./scripts/submit_bronze_profile.sh --dataset all
./scripts/submit_silver_build.sh
./scripts/submit_silver_check.sh --sample-size 1 --null-sample-size 5
```

If `.env` points `SPARK_MASTER_URL` to the Docker Spark cluster, start it first with
`docker compose up -d`. For a local-only verification run, use
`SPARK_MASTER_URL='local[*]' PYTHONPATH=src python3 jobs/02_build_silver.py`.

## Sample Data

Generate small CSV samples for notebook exploration:

```bash
make samples
```

The generated files are written under `data/samples/`:

- `raw/`: Raw-shaped CSV samples for the two source datasets
- `bronze/`: parsed Bronze-shaped CSV samples for the two current Bronze tables

Gold samples are intentionally not generated yet. Silver output artifacts are produced by
`jobs/02_build_silver.py` or `scripts/submit_silver_build.sh` after Bronze Parquet exists.
