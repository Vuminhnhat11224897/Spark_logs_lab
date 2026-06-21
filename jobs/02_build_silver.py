from __future__ import annotations

from spark_log_lab.common.spark import create_spark_session
from spark_log_lab.pipelines.silver_clean_parquet import build_silver_pipeline


def main() -> int:
    spark = create_spark_session("spark-log-lab-silver")
    try:
        build_silver_pipeline(spark)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
