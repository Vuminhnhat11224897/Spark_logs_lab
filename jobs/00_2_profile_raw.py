from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "src"))

from pyspark.sql.types import StructType

from spark_log_lab.common.paths import raw_dir
from spark_log_lab.common.spark import create_spark_session
from spark_log_lab.io.readers import read_csv
from spark_log_lab.metadata.run_context import create_run_context
from spark_log_lab.quality.profiler import profile_dataframe, write_profiles
from spark_log_lab.schemas.raw import LOG_TRACKING_SCHEMA, PURCHASE_BEHAVIOR_SCHEMA


@dataclass(frozen=True)
class RawProfileDataset:
    name: str
    path: Path
    schema: StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile Raw CSV datasets with Spark.")
    parser.add_argument("--run-id", default=None, help="Optional profile run identifier.")
    parser.add_argument(
        "--dataset",
        choices=("all", "log_tracking", "purchase_behavior"),
        default="all",
        help="Dataset to profile.",
    )
    return parser.parse_args()


def raw_profile_datasets() -> list[RawProfileDataset]:
    return [
        RawProfileDataset(
            name="log_tracking",
            path=raw_dir() / "01-log-tracking.csv",
            schema=LOG_TRACKING_SCHEMA,
        ),
        RawProfileDataset(
            name="purchase_behavior",
            path=raw_dir() / "02-purchase-behavior.csv",
            schema=PURCHASE_BEHAVIOR_SCHEMA,
        ),
    ]


def main() -> int:
    args = parse_args()
    ctx = create_run_context(job_name="00_2_profile_raw", batch_id=args.run_id)
    datasets = [
        dataset
        for dataset in raw_profile_datasets()
        if args.dataset == "all" or args.dataset == dataset.name
    ]

    missing_files = [str(dataset.path) for dataset in datasets if not dataset.path.exists()]
    if missing_files:
        print("Missing raw files:")
        for path in missing_files:
            print(f"- {path}")
        return 1

    spark = create_spark_session("00_2_profile_raw")
    try:
        for dataset in datasets:
            print(f"\n=== profiling raw.{dataset.name} ===")
            df = read_csv(spark=spark, path=dataset.path, schema=dataset.schema, header=True)
            profiles = profile_dataframe(
                df=df,
                run_id=ctx.batch_id or ctx.run_id,
                layer="raw",
                dataset=dataset.name,
            )
            write_profiles(profiles)
            print(f"wrote {len(profiles)} column profiles for raw.{dataset.name}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
