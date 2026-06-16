from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_ROOT = PROJECT_ROOT / "data" / "samples"

EVENT_TYPES = ("view", "cart", "purchase")
BRANDS = ("samsung", "apple", "xiaomi", "sony", "lg", "lenovo")
CATEGORIES = (
    ("2053013555631882655", "electronics.smartphone"),
    ("2053013554415534427", "computers.notebook"),
    ("2053013554658804075", "appliances.kitchen"),
    ("2053013557192163841", "furniture.bedroom.bed"),
)


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def make_log_tracking_raw(row_count: int = 100) -> list[dict[str, object]]:
    start = datetime(2019, 11, 1, 8, 0, tzinfo=timezone.utc)
    rows: list[dict[str, object]] = []
    for index in range(row_count):
        event_time = start + timedelta(minutes=index * 17)
        category_id, category_code = CATEGORIES[index % len(CATEGORIES)]
        price = Decimal("19.99") + Decimal(index % 20) * Decimal("7.50")
        rows.append(
            {
                "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "event_type": EVENT_TYPES[index % len(EVENT_TYPES)],
                "product_id": str(1000000 + (index % 25)),
                "category_id": category_id,
                "category_code": category_code,
                "brand": BRANDS[index % len(BRANDS)],
                "price": f"{price:.2f}",
                "user_id": str(520000000 + (index % 30)),
                "user_session": f"sample-session-{index % 18:03d}",
            }
        )
    return rows


def make_purchase_behavior_raw(log_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, raw in enumerate(log_rows):
        event_timestamp = datetime.strptime(
            str(raw["event_time"]), "%Y-%m-%d %H:%M:%S UTC"
        ).replace(tzinfo=timezone.utc)
        event_date = event_timestamp.date()
        first_event_date = event_date - timedelta(days=index % 5)
        start_of_week = event_date - timedelta(days=event_date.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        week_number = int(event_timestamp.strftime("%V"))
        rows.append(
            {
                "user_id": raw["user_id"],
                "event_time": event_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": "purchase" if index % 3 == 0 else raw["event_type"],
                "product_id": raw["product_id"],
                "category_id": raw["category_id"],
                "category_code": raw["category_code"],
                "brand": raw["brand"],
                "price": raw["price"],
                "user_session": raw["user_session"],
                "event_date": event_date.isoformat(),
                "first_event_date": first_event_date.isoformat(),
                "start_of_week": start_of_week.isoformat(),
                "week_number": str(week_number),
                "end_of_week": end_of_week.isoformat(),
                "week_text": f"W{week_number:02d}",
                "cohort_index_week": f"W{week_number:02d} ({start_of_week} -> {end_of_week})",
                "week_after": str((event_date - first_event_date).days // 7),
            }
        )
    return rows


def make_log_tracking_bronze(raw_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    ingest_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows: list[dict[str, object]] = []
    for raw in raw_rows:
        event_timestamp = datetime.strptime(
            str(raw["event_time"]), "%Y-%m-%d %H:%M:%S UTC"
        ).replace(tzinfo=timezone.utc)
        row = dict(raw)
        row.update(
            {
                "event_timestamp": event_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "event_date": event_timestamp.date().isoformat(),
                "source_file": "data/raw/01-log-tracking.csv",
                "ingest_time": ingest_time,
                "batch_id": "sample_001",
            }
        )
        rows.append(row)
    return rows


def make_purchase_behavior_bronze(raw_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    ingest_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows: list[dict[str, object]] = []
    for raw in raw_rows:
        rows.append(
            {
                "event_time": raw["event_time"],
                "event_type": raw["event_type"],
                "product_id": raw["product_id"],
                "category_id": raw["category_id"],
                "category_code": raw["category_code"],
                "brand": raw["brand"],
                "price": raw["price"],
                "user_id": raw["user_id"],
                "user_session": raw["user_session"],
                "event_timestamp": raw["event_time"],
                "event_date": raw["event_date"],
                "first_event_date": raw["first_event_date"],
                "start_of_week": raw["start_of_week"],
                "week_number": raw["week_number"],
                "end_of_week": raw["end_of_week"],
                "week_text": raw["week_text"],
                "cohort_index_week": raw["cohort_index_week"],
                "week_after": raw["week_after"],
                "source_file": "data/raw/02-purchase-behavior.csv",
                "ingest_time": ingest_time,
                "batch_id": "sample_001",
            }
        )
    return rows


def main() -> int:
    log_raw = make_log_tracking_raw(row_count=100)
    purchase_raw = make_purchase_behavior_raw(log_raw)
    log_bronze = make_log_tracking_bronze(log_raw)
    purchase_bronze = make_purchase_behavior_bronze(purchase_raw)

    write_csv(
        SAMPLE_ROOT / "raw" / "01-log-tracking-sample.csv",
        log_raw,
        list(log_raw[0].keys()),
    )
    write_csv(
        SAMPLE_ROOT / "raw" / "02-purchase-behavior-sample.csv",
        purchase_raw,
        list(purchase_raw[0].keys()),
    )
    write_csv(
        SAMPLE_ROOT / "bronze" / "log_tracking_sample.csv",
        log_bronze,
        list(log_bronze[0].keys()),
    )
    write_csv(
        SAMPLE_ROOT / "bronze" / "purchase_behavior_sample.csv",
        purchase_bronze,
        list(purchase_bronze[0].keys()),
    )

    print(f"Raw and Bronze sample data written under: {SAMPLE_ROOT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
