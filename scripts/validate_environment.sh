#!/usr/bin/env bash
set -euo pipefail

PYTHONPATH=src python3 -c "import spark_log_lab; from spark_log_lab.common.paths import project_root; print(project_root())"
docker compose config >/dev/null
echo "environment validation ok"
