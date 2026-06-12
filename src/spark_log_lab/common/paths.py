from __future__ import annotations

import os
from pathlib import Path


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def project_root() -> Path:
    configured = os.getenv("SPARK_TRAINING_ROOT") or os.getenv("SPARK_LOG_LAB_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(__file__).resolve().parents[3]


def data_dir() -> Path:
    return _ensure_dir(project_root() / "data")


def raw_dir() -> Path:
    return _ensure_dir(data_dir() / "raw")


def bronze_dir() -> Path:
    return _ensure_dir(warehouse_dir() / "bronze")


def samples_dir() -> Path:
    return _ensure_dir(data_dir() / "samples")


def checkpoint_dir() -> Path:
    return _ensure_dir(data_dir() / "checkpoint")


def warehouse_dir() -> Path:
    return _ensure_dir(project_root() / "warehouse")


def results_dir() -> Path:
    configured = os.getenv("SPARK_TRAINING_RESULTS_DIR") or os.getenv("SPARK_LOG_LAB_RESULTS_DIR")
    if configured:
        return _ensure_dir(Path(configured).expanduser().resolve())
    return _ensure_dir(project_root() / "results")


def reports_dir() -> Path:
    return _ensure_dir(results_dir() / "reports")
