from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from spark_log_lab.common.cli import fail_with_error
from spark_log_lab.common.errors import ErrorCode
from spark_log_lab.common.paths import warehouse_dir
from spark_log_lab.common.spark import create_spark_session
from spark_log_lab.io.readers import read_parquet
from spark_log_lab.schemas.silver import (
    LOG_TRACKING_SILVER_REQUIRED_COLUMNS,
    LOG_TRACKING_SILVER_SCHEMA,
    PURCHASE_BEHAVIOR_SILVER_REQUIRED_COLUMNS,
    PURCHASE_BEHAVIOR_SILVER_SCHEMA,
    SILVER_QUARANTINE_SCHEMA,
)


@dataclass(frozen=True)
class SilverDataset:
    name: str
    path: Path
    schema: StructType
    required_fields: tuple[str, ...]
    partition_fields: tuple[str, ...] = ()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Silver Parquet outputs with Spark.")
    parser.add_argument("--sample-size", type=int, default=5, help="Number of rows to show per table.")
    parser.add_argument(
        "--null-sample-size",
        type=int,
        default=1000,
        help="Number of rows used for quick null-count checks.",
    )
    parser.add_argument(
        "--full-count",
        action="store_true",
        help="Run full row counts. This scans the Silver Parquet output.",
    )
    return parser.parse_args()


def schema_fields(schema: StructType) -> list[str]:
    return [field.name for field in schema.fields]


def schema_types(schema: StructType) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in schema.fields}


def expected_read_fields(dataset: SilverDataset) -> list[str]:
    fields = schema_fields(dataset.schema)
    return [field for field in fields if field not in dataset.partition_fields] + list(
        dataset.partition_fields
    )


def silver_datasets() -> list[SilverDataset]:
    root = warehouse_dir() / "silver"
    return [
        SilverDataset(
            name="log_tracking",
            path=root / "log_tracking",
            schema=LOG_TRACKING_SILVER_SCHEMA,
            required_fields=tuple(LOG_TRACKING_SILVER_REQUIRED_COLUMNS),
            partition_fields=("event_date",),
        ),
        SilverDataset(
            name="purchase_behavior",
            path=root / "purchase_behavior",
            schema=PURCHASE_BEHAVIOR_SILVER_SCHEMA,
            required_fields=tuple(PURCHASE_BEHAVIOR_SILVER_REQUIRED_COLUMNS),
            partition_fields=("event_date",),
        ),
        SilverDataset(
            name="quarantine",
            path=root / "quarantine",
            schema=SILVER_QUARANTINE_SCHEMA,
            required_fields=("dataset", "rule_id", "severity", "failed_reason", "batch_id"),
        ),
    ]


def print_schema_check(df: DataFrame, dataset: SilverDataset) -> None:
    actual_fields = df.columns
    expected_fields = schema_fields(dataset.schema)
    expected_read_order = expected_read_fields(dataset)
    actual_types = schema_types(df.schema)
    expected_types = schema_types(dataset.schema)
    missing = [field for field in expected_fields if field not in actual_fields]
    extra = [field for field in actual_fields if field not in expected_fields]
    type_mismatches = {
        field: {"actual": actual_types.get(field), "expected": expected_types[field]}
        for field in expected_fields
        if field in actual_types and actual_types[field] != expected_types[field]
    }

    print(f"\n=== {dataset.name}: silver schema check ===")
    print(f"path: {dataset.path}")
    print(f"exists: {dataset.path.exists()}")
    print(f"actual_fields: {actual_fields}")
    print(f"expected_fields: {expected_fields}")
    print(f"expected_read_order: {expected_read_order}")
    print(f"missing_fields: {missing}")
    print(f"extra_fields: {extra}")
    print(f"same_order: {actual_fields == expected_read_order}")
    print(f"type_mismatches: {type_mismatches}")


def print_data_checks(df: DataFrame, dataset: SilverDataset, args: argparse.Namespace) -> None:
    print(f"\n=== {dataset.name}: spark schema ===")
    df.printSchema()

    print(f"\n=== {dataset.name}: sample rows ===")
    df.show(args.sample_size, truncate=False)

    sample_df = df.limit(args.null_sample_size)
    null_counts = sample_df.select(
        *[
            F.count(F.when(F.col(column).isNull(), column)).alias(column)
            for column in dataset.required_fields
        ]
    )
    print(f"\n=== {dataset.name}: required-field null counts on first {args.null_sample_size} rows ===")
    null_counts.show(truncate=False)

    if args.full_count:
        print(f"\n=== {dataset.name}: full count ===")
        print(df.count())
    else:
        print(f"\n=== {dataset.name}: full count skipped; use --full-count to scan the table ===")


def check_dataset(spark: SparkSession, dataset: SilverDataset, args: argparse.Namespace) -> None:
    df = read_parquet(spark=spark, path=dataset.path)
    print_schema_check(df=df, dataset=dataset)
    print_data_checks(df=df, dataset=dataset, args=args)


def main() -> int:
    args = parse_args()
    datasets = silver_datasets()

    missing_paths = [str(dataset.path) for dataset in datasets if not dataset.path.exists()]
    if missing_paths:
        return fail_with_error(ErrorCode.MISSING_INPUT_PATHS, paths=missing_paths)

    spark = create_spark_session("02_1_check_silver")
    try:
        for dataset in datasets:
            check_dataset(spark=spark, dataset=dataset, args=args)
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
