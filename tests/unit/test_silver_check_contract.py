from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def load_silver_check_module():
    job_path = Path(__file__).resolve().parents[2] / "jobs" / "02_1_check_silver.py"
    spec = importlib.util.spec_from_file_location("check_silver", job_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_partitioned_silver_outputs_expect_partition_column_at_read_tail():
    module = load_silver_check_module()
    datasets = {dataset.name: dataset for dataset in module.silver_datasets()}

    assert module.expected_read_fields(datasets["log_tracking"])[-1] == "event_date"
    assert module.expected_read_fields(datasets["purchase_behavior"])[-1] == "event_date"
    assert module.expected_read_fields(datasets["quarantine"]) == module.schema_fields(
        datasets["quarantine"].schema
    )
