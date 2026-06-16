from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "src"))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from spark_log_lab.common.paths import raw_dir
from spark_log_lab.common.spark import create_spark_session
from spark_log_lab.io.readers import read_csv
from spark_log_lab.schemas.raw import LOG_TRACKING_SCHEMA, PURCHASE_BEHAVIOR_SCHEMA


@dataclass(frozen=True)
class RawDataset:
    name: str
    path: Path
    schema: StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check raw CSV files with Spark.")
    parser.add_argument("--sample-size", type=int, default=5, help="Number of rows to show per file.")
    parser.add_argument(
        "--null-sample-size",
        type=int,
        default=1000,
        help="Number of rows used for quick null-count checks.",
    )
    parser.add_argument(
        "--full-count",
        action="store_true",
        help="Run full row counts. This scans the large raw files.",
    )
    return parser.parse_args()


def read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)


def schema_fields(schema: StructType) -> list[str]:
    return [field.name for field in schema.fields]


def print_field_check(dataset: RawDataset) -> None:
    actual_fields = read_csv_header(dataset.path)
    expected_fields = schema_fields(dataset.schema)
    missing = [field for field in expected_fields if field not in actual_fields]
    extra = [field for field in actual_fields if field not in expected_fields]
    same_order = actual_fields == expected_fields

    print(f"\n=== {dataset.name}: field check ===")
    print(f"path: {dataset.path}")
    print(f"exists: {dataset.path.exists()}")
    print(f"actual_fields: {actual_fields}")
    print(f"expected_fields: {expected_fields}")
    print(f"missing_fields: {missing}")
    print(f"extra_fields: {extra}")
    print(f"same_order: {same_order}")


def print_spark_checks(df: DataFrame, dataset: RawDataset, args: argparse.Namespace) -> None:
    print(f"\n=== {dataset.name}: spark schema ===")
    df.printSchema()

    print(f"\n=== {dataset.name}: sample rows ===")
    df.show(args.sample_size, truncate=False)

    sample_df = df.limit(args.null_sample_size)
    null_counts = sample_df.select(
        *[
            F.count(F.when(F.col(column).isNull(), column)).alias(column)
            for column in df.columns
        ]
    )
    print(f"\n=== {dataset.name}: null counts on first {args.null_sample_size} rows ===")
    null_counts.show(truncate=False)

    if args.full_count:
        print(f"\n=== {dataset.name}: full count ===")
        print(df.count())
    else:
        print(f"\n=== {dataset.name}: full count skipped; use --full-count to scan the whole file ===")


def check_dataset(spark: SparkSession, dataset: RawDataset, args: argparse.Namespace) -> None:
    print_field_check(dataset)
    df = read_csv(spark=spark, path=dataset.path, schema=dataset.schema, header=True)
    print_spark_checks(df=df, dataset=dataset, args=args)


def main() -> int:
    args = parse_args()
    datasets = [
        RawDataset(
            name="log_tracking",
            path=raw_dir() / "01-log-tracking.csv",
            schema=LOG_TRACKING_SCHEMA,
        ),
        RawDataset(
            name="purchase_behavior",
            path=raw_dir() / "02-purchase-behavior.csv",
            schema=PURCHASE_BEHAVIOR_SCHEMA,
        ),
    ]

    missing_files = [str(dataset.path) for dataset in datasets if not dataset.path.exists()]
    if missing_files:
        print("Missing raw files:")
        for path in missing_files:
            print(f"- {path}")
        return 1

    spark = create_spark_session("00_1_check_raw_files")
    try:
        for dataset in datasets:
            check_dataset(spark=spark, dataset=dataset, args=args)
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
