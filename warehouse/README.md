# Warehouse

Local warehouse artifacts are written here by batch jobs.

Expected layer targets:

- `warehouse/bronze/` for parsed ingestion outputs
- `warehouse/silver/` for cleaned analytical outputs
- `warehouse/gold/` for marts and aggregates

At the current repo state, Bronze and Silver are implemented as local Parquet outputs. The runnable
Silver build writes:

- `warehouse/silver/log_tracking/`
- `warehouse/silver/purchase_behavior/`
- `warehouse/silver/quarantine/`

Gold outputs are still future work.
