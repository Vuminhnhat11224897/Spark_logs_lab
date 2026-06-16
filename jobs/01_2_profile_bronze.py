from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "src"))

from spark_log_lab.common.paths import bronze_dir
from spark_log_lab.common.spark import create_spark_session
from spark_log_lab.io.readers import read_parquet
from spark_log_lab.metadata.run_context import create_run_context
from spark_log_lab.quality.profiler import data_profile_path, profile_dataframe, write_profiles


@dataclass(frozen=True)
class BronzeProfileDataset:
    name: str
    path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile Bronze Parquet tables with Spark.")
    parser.add_argument("--run-id", default=None, help="Optional profile run identifier.")
    parser.add_argument(
        "--profile-version",
        default=None,
        help="Optional version suffix for writing a separate profile snapshot.",
    )
    parser.add_argument(
        "--dataset",
        choices=("all", "log_tracking", "purchase_behavior"),
        default="all",
        help="Bronze dataset to profile.",
    )
    return parser.parse_args()


def bronze_profile_datasets() -> list[BronzeProfileDataset]:
    return [
        BronzeProfileDataset(
            name="log_tracking",
            path=bronze_dir() / "log_tracking",
        ),
        BronzeProfileDataset(
            name="purchase_behavior",
            path=bronze_dir() / "purchase_behavior",
        ),
    ]


def main() -> int:
    args = parse_args()
    ctx = create_run_context(job_name="01_2_profile_bronze", batch_id=args.run_id)
    datasets = [
        dataset
        for dataset in bronze_profile_datasets()
        if args.dataset == "all" or args.dataset == dataset.name
    ]

    missing_paths = [str(dataset.path) for dataset in datasets if not dataset.path.exists()]
    if missing_paths:
        print("Missing Bronze output paths:")
        for path in missing_paths:
            print(f"- {path}")
        return 1

    spark = create_spark_session("01_2_profile_bronze")
    try:
        for dataset in datasets:
            print(f"\n=== profiling bronze.{dataset.name} ===")
            df = read_parquet(spark=spark, path=dataset.path)
            profiles = profile_dataframe(
                df=df,
                run_id=ctx.batch_id or ctx.run_id,
                layer="bronze",
                dataset=dataset.name,
            )
            output_path = write_profiles(
                profiles,
                path=data_profile_path("bronze", dataset.name, version=args.profile_version),
            )
            print(f"wrote {len(profiles)} column profiles for bronze.{dataset.name} to {output_path}")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
