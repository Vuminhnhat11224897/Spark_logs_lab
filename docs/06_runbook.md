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

## Common Issues

### Missing Raw File

Action:
- Confirm `data/raw/01-log-tracking.csv` exists.
- Confirm `data/raw/02-purchase-behavior.csv` exists if join exercises need it.

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
