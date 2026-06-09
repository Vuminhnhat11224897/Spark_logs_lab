#!/usr/bin/env bash
set -euo pipefail

test -f docker-compose.yml
test -d data/raw
test -d src/spark_log_lab
test -f pyproject.toml
echo "repo structure ok"
