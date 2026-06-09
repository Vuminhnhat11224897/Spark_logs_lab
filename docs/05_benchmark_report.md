# Benchmark Report

## Purpose

Track Spark query and pipeline performance as the project evolves.

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
