#!/usr/bin/env bash
set -euo pipefail

rm -rf .pytest_cache
find . -type d -name __pycache__ -prune -exec rm -rf {} +
echo "runtime caches removed"
