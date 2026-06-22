"""Writer helpers for Spark DataFrames.

Keep these helpers thin: they should standardize write options, not hide pipeline logic.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pyspark.sql import DataFrame

from spark_log_lab.common.errors import ErrorCode, raise_error


WriteMode = Literal["append", "overwrite", "error", "errorifexists", "ignore"]


def _path_str(path: str | Path) -> str:
    return str(path)


def _validate_write_mode(mode: str) -> None:
    valid_modes = {"append", "overwrite", "error", "errorifexists", "ignore"}
    if mode not in valid_modes:
        raise_error(ErrorCode.INVALID_WRITE_MODE, mode=mode, valid_modes=sorted(valid_modes))


def _validate_partition_columns(df: DataFrame, partition_by: list[str] | None) -> None:
    if not partition_by:
        return

    missing = [column for column in partition_by if column not in df.columns]
    if missing:
        raise_error(ErrorCode.INVALID_PARTITION_COLUMNS, columns=missing)


def write_parquet(
    df: DataFrame,
    path: str | Path,
    mode: WriteMode = "errorifexists",
    partition_by: list[str] | None = None,
    compression: str = "snappy",
) -> None:
    """Write a DataFrame as Parquet.

    Use this for Bronze/Silver/Gold datasets. For partitioned datasets, the Spark session
    should be configured with dynamic partition overwrite when doing scoped reruns.
    """
    _validate_write_mode(mode)
    _validate_partition_columns(df, partition_by)

    writer = df.write.mode(mode).option("compression", compression)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(_path_str(path))


def write_csv(
    df: DataFrame,
    path: str | Path,
    mode: WriteMode = "errorifexists",
    header: bool = True,
    partition_by: list[str] | None = None,
    coalesce: int | None = None,
) -> None:
    """Write a DataFrame as CSV.

    Use CSV mainly for human-readable reports or small exports. Dataset layers should prefer
    Parquet because it preserves schema and performs better for Spark queries.
    """
    _validate_write_mode(mode)
    _validate_partition_columns(df, partition_by)

    output_df = df.coalesce(coalesce) if coalesce else df
    writer = output_df.write.mode(mode).option("header", "true" if header else "false")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.csv(_path_str(path))


def write_single_csv(
    df: DataFrame,
    path: str | Path,
    mode: WriteMode = "overwrite",
    header: bool = True,
) -> None:
    """Write a small report-style CSV as one Spark part file."""
    write_csv(df=df, path=path, mode=mode, header=header, coalesce=1)
