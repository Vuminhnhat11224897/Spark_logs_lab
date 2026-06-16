from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType, StringType

from spark_log_lab.common.paths import results_dir
from spark_log_lab.metadata.run_context import utc_now_iso


PROFILE_HEADER = [
    "run_id",
    "layer",
    "dataset",
    "column_name",
    "data_type",
    "row_count",
    "null_count",
    "null_rate",
    "empty_count",
    "empty_rate",
    "approx_distinct_count",
    "mode_value",
    "mode_count",
    "min_value",
    "max_value",
    "avg_value",
    "stddev_value",
    "profiled_at",
]


@dataclass(frozen=True)
class ColumnProfile:
    run_id: str
    layer: str
    dataset: str
    column_name: str
    data_type: str
    row_count: int
    null_count: int
    null_rate: float
    empty_count: int | None
    empty_rate: float | None
    approx_distinct_count: int
    mode_value: str | None
    mode_count: int | None
    min_value: str | None
    max_value: str | None
    avg_value: float | None
    stddev_value: float | None
    profiled_at: str


def data_profile_path(layer: str) -> Path:
    return results_dir() / f"{layer}_data_profiles.csv"


def profile_results_path() -> Path:
    return data_profile_path("raw")


def _safe_string(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _mode(df: DataFrame, column_name: str) -> tuple[str | None, int | None]:
    rows = (
        df.filter(F.col(column_name).isNotNull())
        .groupBy(column_name)
        .agg(F.count("*").alias("value_count"))
        .orderBy(F.desc("value_count"), F.asc(column_name))
        .limit(1)
        .collect()
    )
    if not rows:
        return None, None
    return _safe_string(rows[0][column_name]), int(rows[0]["value_count"])


def profile_column(
    df: DataFrame,
    column_name: str,
    run_id: str,
    layer: str,
    dataset: str,
    row_count: int,
    profiled_at: str,
) -> ColumnProfile:
    field = df.schema[column_name]
    data_type = field.dataType.simpleString()
    is_string = isinstance(field.dataType, StringType)
    is_numeric = isinstance(field.dataType, NumericType)

    empty_expr = F.col(column_name).isNull()
    if is_string:
        empty_expr = F.trim(F.col(column_name)) == ""

    aggregate_exprs = [
        F.count(F.when(F.col(column_name).isNull(), column_name)).alias("null_count"),
        F.approx_count_distinct(F.col(column_name)).alias("approx_distinct_count"),
        F.min(F.col(column_name)).alias("min_value"),
        F.max(F.col(column_name)).alias("max_value"),
    ]
    if is_string:
        aggregate_exprs.append(F.count(F.when(empty_expr, column_name)).alias("empty_count"))
    if is_numeric:
        aggregate_exprs.extend(
            [
                F.avg(F.col(column_name)).alias("avg_value"),
                F.stddev(F.col(column_name)).alias("stddev_value"),
            ]
        )

    metrics = df.agg(*aggregate_exprs).collect()[0].asDict()
    null_count = int(metrics["null_count"])
    empty_count = int(metrics["empty_count"]) if is_string else None
    mode_value, mode_count = _mode(df=df, column_name=column_name)

    return ColumnProfile(
        run_id=run_id,
        layer=layer,
        dataset=dataset,
        column_name=column_name,
        data_type=data_type,
        row_count=row_count,
        null_count=null_count,
        null_rate=0.0 if row_count == 0 else round(null_count / row_count, 6),
        empty_count=empty_count,
        empty_rate=None
        if empty_count is None
        else 0.0
        if row_count == 0
        else round(empty_count / row_count, 6),
        approx_distinct_count=int(metrics["approx_distinct_count"]),
        mode_value=mode_value,
        mode_count=mode_count,
        min_value=_safe_string(metrics["min_value"]),
        max_value=_safe_string(metrics["max_value"]),
        avg_value=None if not is_numeric else metrics.get("avg_value"),
        stddev_value=None if not is_numeric else metrics.get("stddev_value"),
        profiled_at=profiled_at,
    )


def profile_dataframe(
    df: DataFrame,
    run_id: str,
    layer: str,
    dataset: str,
) -> list[ColumnProfile]:
    row_count = df.count()
    profiled_at = utc_now_iso()
    return [
        profile_column(
            df=df,
            column_name=column_name,
            run_id=run_id,
            layer=layer,
            dataset=dataset,
            row_count=row_count,
            profiled_at=profiled_at,
        )
        for column_name in df.columns
    ]


def write_profiles(profiles: list[ColumnProfile], path: Path | None = None) -> Path:
    output_path = path or profile_results_path()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_path.exists()
    with output_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PROFILE_HEADER)
        if write_header:
            writer.writeheader()
        for profile in profiles:
            writer.writerow(asdict(profile))
    return output_path
