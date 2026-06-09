from __future__ import annotations

import os
from pathlib import Path


def project_root() -> Path:
    configured = os.getenv("SPARK_TRAINING_ROOT") or os.getenv("SPARK_LOG_LAB_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[3]


def data_dir() -> Path:
    return project_root() / "data"


def raw_dir() -> Path:
    return data_dir() / "raw"


def samples_dir() -> Path:
    return data_dir() / "samples"


def checkpoint_dir() -> Path:
    return data_dir() / "checkpoint"


def warehouse_dir() -> Path:
    return project_root() / "warehouse"


def results_dir() -> Path:
    configured = os.getenv("SPARK_TRAINING_RESULTS_DIR") or os.getenv("SPARK_LOG_LAB_RESULTS_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    return project_root() / "results"


def reports_dir() -> Path:
    return results_dir() / "reports"
