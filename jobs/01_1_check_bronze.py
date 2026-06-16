from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "src"))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from spark_log_lab.common.paths import bronze_dir
from spark_log_lab.common.spark import create_spark_session
from spark_log_lab.io.readers import read_parquet
from spark_log_lab.schemas.bronze import (
    LOG_TRACKING_BRONZE_SCHEMA,
    PURCHASE_BEHAVIOR_BRONZE_SCHEMA,
)


@dataclass(frozen=True)
class BronzeDataset:
    name: str
    path: Path
    schema: StructType
    required_fields: tuple[str, ...]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Bronze Parquet outputs with Spark.")
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
        help="Run full row counts. This scans the Bronze Parquet output.",
    )
    return parser.parse_args()


def schema_fields(schema: StructType) -> list[str]:
    return [field.name for field in schema.fields]


def schema_types(schema: StructType) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in schema.fields}


def print_schema_check(df: DataFrame, dataset: BronzeDataset) -> None:
    actual_fields = df.columns
    expected_fields = schema_fields(dataset.schema)
    actual_types = schema_types(df.schema)
    expected_types = schema_types(dataset.schema)
    missing = [field for field in expected_fields if field not in actual_fields]
    extra = [field for field in actual_fields if field not in expected_fields]
    type_mismatches = {
        field: {"actual": actual_types.get(field), "expected": expected_types[field]}
        for field in expected_fields
        if field in actual_types and actual_types[field] != expected_types[field]
    }

    print(f"\n=== {dataset.name}: bronze schema check ===")
    print(f"path: {dataset.path}")
    print(f"exists: {dataset.path.exists()}")
    print(f"actual_fields: {actual_fields}")
    print(f"expected_fields: {expected_fields}")
    print(f"missing_fields: {missing}")
    print(f"extra_fields: {extra}")
    print(f"same_order: {actual_fields == expected_fields}")
    print(f"type_mismatches: {type_mismatches}")


def print_data_checks(df: DataFrame, dataset: BronzeDataset, args: argparse.Namespace) -> None:
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


def check_dataset(spark: SparkSession, dataset: BronzeDataset, args: argparse.Namespace) -> None:
    df = read_parquet(spark=spark, path=dataset.path)
    print_schema_check(df=df, dataset=dataset)
    print_data_checks(df=df, dataset=dataset, args=args)


def main() -> int:
    args = parse_args()
    datasets = [
        BronzeDataset(
            name="log_tracking",
            path=bronze_dir() / "log_tracking",
            schema=LOG_TRACKING_BRONZE_SCHEMA,
            required_fields=(
                "event_time",
                "event_type",
                "product_id",
                "user_id",
                "user_session",
                "event_timestamp",
                "event_date",
                "source_file",
                "ingest_time",
                "batch_id",
            ),
        ),
        BronzeDataset(
            name="purchase_behavior",
            path=bronze_dir() / "purchase_behavior",
            schema=PURCHASE_BEHAVIOR_BRONZE_SCHEMA,
            required_fields=(
                "event_time",
                "event_type",
                "product_id",
                "user_id",
                "user_session",
                "event_timestamp",
                "event_date",
                "source_file",
                "ingest_time",
                "batch_id",
            ),
        ),
    ]

    missing_paths = [str(dataset.path) for dataset in datasets if not dataset.path.exists()]
    if missing_paths:
        print("Missing Bronze output paths:")
        for path in missing_paths:
            print(f"- {path}")
        return 1

    spark = create_spark_session("01_1_check_bronze")
    try:
        for dataset in datasets:
            check_dataset(spark=spark, dataset=dataset, args=args)
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
