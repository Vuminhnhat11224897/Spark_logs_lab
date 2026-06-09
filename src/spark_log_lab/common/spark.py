from __future__ import annotations

import os
from typing import Mapping

from pyspark.sql import SparkSession

from spark_log_lab.common.config import load_dotenv


def create_spark_session(
    app_name: str,
    configs: Mapping[str, str] | None = None,
) -> SparkSession:
    load_dotenv()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    env_configs = {
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS"),
        "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY"),
        "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY"),
        "spark.executor.cores": os.getenv("SPARK_EXECUTOR_CORES"),
    }
    for key, value in env_configs.items():
        if value:
            builder = builder.config(key, value)

    if configs:
        for key, value in configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
