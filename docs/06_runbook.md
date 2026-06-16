# Runbook

## Environment

1. Copy `.env.example` to `.env` if needed.
2. Start Spark with `docker compose up -d`.
3. Run checks with `make check-imports` and `pytest -q`.

## Normal Checks

```bash
scripts/check_repo.sh
make check-imports
python3 -m pytest -q
```

## Sample Data For Notebooks

Generate small CSV samples for local notebook exploration:

```bash
make samples
```

This writes ignored local files under `data/samples/raw/` and `data/samples/bronze/`.
Silver and Gold sample outputs are intentionally left for later phases.

## Raw Schema Check

Use this after changing `src/spark_log_lab/schemas/raw.py` or replacing Raw CSV files.

```bash
./scripts/submit_raw_check.sh --sample-size 1 --null-sample-size 5
```

Expected result:

- `log_tracking` reports no missing or extra fields.
- `log_tracking` reports `same_order: True`.
- `purchase_behavior` reports no missing or extra fields.
- `purchase_behavior` reports `same_order: True`.

The Raw schema order must match the physical CSV header. In particular,
`data/raw/02-purchase-behavior.csv` starts with `user_id`.

## Raw Profiling

Use this after Raw files are added or replaced and you want pandas-like profile metrics for each
column.

```bash
./scripts/submit_raw_profile.sh --dataset all
```

This writes column-level metrics including row count, null count, null rate, approximate distinct
count, mode, min/max, and numeric averages when available. It overwrites one current snapshot per
dataset under `results/data_profiles/`, for example `raw_log_tracking_profile.csv`. Use
`--profile-version <version>` to write a separate snapshot.

## Phase 1 Bronze Build

Phase 1 should make `jobs/01_build_bronze.py` runnable. The job should read Raw CSV, create typed
Bronze fields, add `source_file`, `ingest_time`, `batch_id`, and write Parquet output under
`warehouse/bronze/`.

```bash
./scripts/submit_bronze_build.sh --batch-id test_001
```

## Bronze Output Check

Use this after running the Bronze build.

```bash
./scripts/submit_bronze_check.sh --sample-size 1 --null-sample-size 5
```

Expected result:

- `log_tracking` reports no missing or extra Bronze fields.
- `log_tracking` reports `same_order: True`.
- `purchase_behavior` reports no missing or extra Bronze fields.
- `purchase_behavior` reports `same_order: True`.
- Required metadata fields `source_file`, `ingest_time`, and `batch_id` are populated.

## Bronze Profiling

Use this after running the Bronze build and Bronze output check.

```bash
./scripts/submit_bronze_profile.sh --dataset all
```

This overwrites one current snapshot per dataset under `results/data_profiles/`, for example
`bronze_log_tracking_profile.csv`. Use `--profile-version <version>` to write a separate snapshot.
Bronze has parsed timestamp and date columns, so its profile is usually more useful than Raw for
type-aware inspection.

## Common Issues

### Missing Raw File

Action:
- Confirm `data/raw/01-log-tracking.csv` exists.
- Confirm `data/raw/02-purchase-behavior.csv` exists.

### Docker Compose Fails

Action:
- Validate config with `docker compose config`.
- Check `.env` contains `SPARK_WORKER_CORES` and `SPARK_WORKER_MEMORY`.

### Import Fails

Action:
- Run from project root.
- Use `PYTHONPATH=src` or install with `python3 -m pip install -e .`.

## Guardrails

Do not delete or overwrite `data/raw/`.
Do not reorder Raw schemas unless the source CSV header changes.
