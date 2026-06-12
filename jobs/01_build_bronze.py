from __future__ import annotations

import argparse
from datetime import datetime, timezone

from spark_log_lab.common.spark import create_spark_session
from spark_log_lab.pipelines.bronze_csv_to_parquet import build_bronze_pipeline

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build bronze layer from raw CSV data.")
    parser.add_argument("--batch-id", type=str, default=None, help="Batch ID for this run")
    return parser.parse_args()

def default_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

def main() -> int:
    args = parse_args()
    batch_id = args.batch_id or default_batch_id()
    print(f"Using batch ID: {batch_id}")
    spark = create_spark_session(app_name="Build Bronze Layer")
    try:
        build_bronze_pipeline(spark=spark, batch_id=batch_id)
    finally:
        spark.stop()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
