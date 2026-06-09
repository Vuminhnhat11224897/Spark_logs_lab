from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from spark_log_lab.common.paths import data_dir, project_root, results_dir, warehouse_dir


@dataclass(frozen=True)
class ProjectConfig:
    root: Path
    data_path: Path
    results_path: Path
    warehouse_path: Path
    spark_master_url: str
    spark_log_level: str


def load_dotenv(path: Path | None = None) -> None:
    env_path = path or project_root() / ".env"
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_config() -> ProjectConfig:
    load_dotenv()
    return ProjectConfig(
        root=project_root(),
        data_path=Path(os.getenv("SPARK_TRAINING_DATA_DIR", data_dir())).resolve(),
        results_path=Path(os.getenv("SPARK_TRAINING_RESULTS_DIR", results_dir())).resolve(),
        warehouse_path=Path(os.getenv("SPARK_TRAINING_WAREHOUSE_DIR", warehouse_dir())).resolve(),
        spark_master_url=os.getenv("SPARK_MASTER_URL", "local[*]"),
        spark_log_level=os.getenv("SPARK_LOG_LEVEL", "WARN"),
    )
