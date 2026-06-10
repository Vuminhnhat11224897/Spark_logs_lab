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

## Phase 1 Bronze Build

Phase 1 should make `jobs/01_build_bronze.py` runnable. The job should read Raw CSV, create typed
Bronze fields, add `source_file`, `ingest_time`, `batch_id`, and write Parquet output under
`warehouse/bronze/`.

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
