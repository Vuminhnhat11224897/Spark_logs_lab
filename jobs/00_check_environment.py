from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from spark_log_lab.common.paths import project_root, raw_dir


def main() -> int:
    root = project_root()
    raw = raw_dir()
    print(f"project_root={root}")
    print(f"raw_dir={raw}")
    print(f"raw_exists={raw.exists()}")
    return 0 if raw.exists() else 1


if __name__ == "__main__":
    raise SystemExit(main())
