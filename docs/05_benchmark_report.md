# Benchmark Report

## Purpose

Track Spark query and pipeline performance as the project evolves.

Current runnable batch layers are Bronze and Silver. Benchmark work should use Silver outputs as
the main analytical input until Gold marts are implemented.

## Planned Metrics

| Metric | Description |
|---|---|
| `run_id` | Unique benchmark run identifier |
| `benchmark_name` | Benchmark suite name |
| `query_name` | Query or operation being timed |
| `row_count` | Number of rows returned or processed |
| `elapsed_seconds` | Runtime in seconds |
| `sample_fraction` | Sample ratio when not using full data |

## Output Location

Benchmark outputs should be written under `results/benchmark_runs/`.

Suggested first benchmark targets:

- Silver build runtime for `jobs/02_build_silver.py`
- Filter/query runtime over `warehouse/silver/log_tracking/` partitioned by `event_date`
- Aggregations over `warehouse/silver/purchase_behavior/` by `event_date` and category levels
