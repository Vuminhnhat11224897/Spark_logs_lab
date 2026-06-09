# Security

## Local Secrets

- `.env` is local-only and must not be committed.
- `.env.example` is safe to commit.

## Data

- Raw data stays local under `data/raw/`.

## Generated Outputs

- `warehouse/` is runtime output.
- `results/*.csv` is generated output.
- `logs/` is runtime output.

## Review Checklist

- Do not commit secrets.
- Do not commit large raw CSV files.
- Do not commit generated Parquet or warehouse data.
