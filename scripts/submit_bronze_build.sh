#!/usr/bin/env bash
set -euo pipefail

SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-4g}"
EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-10g}"
EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-3}"

docker exec \
  -e PYTHONPATH=/opt/spark-log-lab/src \
  spark-log-lab-master \
  /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  --executor-cores "${EXECUTOR_CORES}" \
  /opt/spark-log-lab/jobs/01_build_bronze.py "$@"
