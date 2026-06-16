# Spark Training

Spark training project for local batch data engineering practice.

The repository has completed the foundation work and now includes the first batch workflow
building blocks: Raw schema validation, Raw profiling, sample-data generation for notebook
exploration, and Bronze Parquet ingestion. It keeps the original raw input data unchanged,
preserves the Docker Compose Spark cluster, and provides a clean package layout for
Raw -> Bronze -> Silver -> Gold development.

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

## Current Status

Foundation and Bronze ingestion scaffolding are in place.

- Project root: `/home/zseefvhu12/projects/spark_training`
- Raw files are preserved under `data/raw/`.
- Docker Compose is preserved at the project root.
- Raw schemas match the physical CSV headers and field order.
- Raw profile metrics can be written to `results/data_profiles.csv`.
- Bronze schemas are defined with source fields first, derived fields next, and metadata fields last.
- `ingest_time` is the standard ingestion timestamp metadata field.
- Raw schema validation can be submitted to the Spark master through `scripts/submit_raw_check.sh`.
- Bronze build and Bronze output checks can be submitted through `scripts/submit_bronze_build.sh`
  and `scripts/submit_bronze_check.sh`.

## Next Development Focus

The root project folder is now:

```text
/home/zseefvhu12/projects/spark_training
```

The Python package is:

```text
src/spark_log_lab
```

The next major pipeline step is Silver: read Bronze Parquet, clean and standardize analysis-ready
fields, keep lineage metadata, and write Silver Parquet output.

No Iceberg, Trino, or Flink runtime logic is implemented before the batch workflow is stable.

## Quick Checks

```bash
python3 -m pytest -q
PYTHONPATH=src python3 -m py_compile src/spark_log_lab/schemas/raw.py src/spark_log_lab/schemas/bronze.py jobs/00_1_check_raw_files.py jobs/01_1_check_bronze.py
./scripts/submit_raw_check.sh --sample-size 1 --null-sample-size 5
./scripts/submit_raw_profile.sh --dataset all
./scripts/submit_bronze_build.sh --batch-id dev_001
./scripts/submit_bronze_check.sh --sample-size 1 --null-sample-size 5
```

## Sample Data

Generate small CSV samples for notebook exploration:

```bash
make samples
```

The generated files are written under `data/samples/`:

- `raw/`: Raw-shaped CSV samples for the two source datasets
- `bronze/`: parsed Bronze-shaped CSV samples for the two current Bronze tables

Silver and Gold samples are intentionally not generated yet. Those layers should be designed from
the current Raw/Bronze samples when their business contracts are clear.
