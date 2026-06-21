# Data Dictionary

## Raw Dataset: log_tracking

Source file: `data/raw/01-log-tracking.csv`

Raw schema: `src/spark_log_lab/schemas/raw.py::LOG_TRACKING_SCHEMA`

| Order | Column | Raw Type | Required | Notes |
|---:|---|---|---:|---|
| 1 | `event_time` | string | yes | Source event timestamp text |
| 2 | `event_type` | string | yes | Event action such as `view`, `cart`, `purchase` |
| 3 | `product_id` | string | yes | Product identifier text in Raw |
| 4 | `category_id` | string | no | Product category identifier text in Raw |
| 5 | `category_code` | string | no | Dot-delimited product category path |
| 6 | `brand` | string | no | Product brand |
| 7 | `price` | string | no | Product price text in Raw |
| 8 | `user_id` | string | yes | User identifier text in Raw |
| 9 | `user_session` | string | yes | User session identifier |

## Raw Dataset: purchase_behavior

Source file: `data/raw/02-purchase-behavior.csv`

Raw schema: `src/spark_log_lab/schemas/raw.py::PURCHASE_BEHAVIOR_SCHEMA`

| Order | Column | Raw Type | Required | Notes |
|---:|---|---|---:|---|
| 1 | `user_id` | string | yes | User identifier text in Raw. This is the first physical CSV field. |
| 2 | `event_time` | string | yes | Source event timestamp text |
| 3 | `event_type` | string | yes | Event action |
| 4 | `product_id` | string | yes | Product identifier text in Raw |
| 5 | `category_id` | string | no | Product category identifier text in Raw |
| 6 | `category_code` | string | no | Dot-delimited product category path |
| 7 | `brand` | string | no | Product brand |
| 8 | `price` | string | no | Product price text in Raw |
| 9 | `user_session` | string | yes | User session identifier |
| 10 | `event_date` | string | yes | Source-provided event date text |
| 11 | `first_event_date` | string | no | Cohort first event date text |
| 12 | `start_of_week` | string | no | Cohort week start text |
| 13 | `week_number` | string | no | Week number text |
| 14 | `end_of_week` | string | no | Cohort week end text |
| 15 | `week_text` | string | no | Week label |
| 16 | `cohort_index_week` | string | no | Cohort display label |
| 17 | `week_after` | string | no | Cohort offset text |

## Bronze Metadata Fields

Bronze outputs add the following metadata fields after source and derived fields.

| Column | Type | Notes |
|---|---|---|
| `source_file` | string | Input file name or path used for lineage |
| `ingest_time` | timestamp | Timestamp when the batch ingests the row |
| `batch_id` | string | Logical run identifier for reruns and audit |

## Silver Dataset: log_tracking

Silver schema: `src/spark_log_lab/schemas/silver.py::LOG_TRACKING_SILVER_SCHEMA`

| Group | Columns |
|---|---|
| Event identity and time | `event_time`, `event_timestamp`, `event_date`, `event_type` |
| Product dimensions | `product_id`, `category_id`, `category_code`, `category_l1`, `category_l2`, `category_l3`, `brand` |
| Measures | `price` (`decimal(18,2)`) |
| User/session identity | `user_id`, `user_session` |
| Data quality flags | `is_category_code_missing`, `is_brand_missing`, `dq_warning_count`, `dq_warnings` |
| Lineage and processing metadata | `source_file`, `ingest_time`, `batch_id`, `silver_processed_time` |

Required columns: `event_timestamp`, `event_type`, `product_id`, `user_id`, `user_session`

Notes:

- `event_date` is canonical and derived from `to_date(event_timestamp)`.
- `category_l1`, `category_l2`, and `category_l3` are split from dot-delimited `category_code`.
- Missing category or brand values remain in Silver with warning flags instead of hard quarantine.

## Silver Dataset: purchase_behavior

Silver schema: `src/spark_log_lab/schemas/silver.py::PURCHASE_BEHAVIOR_SILVER_SCHEMA`

| Group | Columns |
|---|---|
| Event identity and time | `event_time`, `event_timestamp`, `event_date`, `event_type` |
| Product dimensions | `product_id`, `category_id`, `category_code`, `category_l1`, `category_l2`, `category_l3`, `brand` |
| Measures | `price` (`decimal(18,2)`) |
| User/session identity | `user_id`, `user_session` |
| Clean cohort fields | `first_event_date`, `cohort_week_start`, `cohort_week_end`, `cohort_week_number`, `cohort_week_label`, `week_after`, `is_cohort_week_mismatch` |
| Data quality flags | `is_category_code_missing`, `is_brand_missing`, `dq_warning_count`, `dq_warnings` |
| Lineage and processing metadata | `source_file`, `ingest_time`, `batch_id`, `silver_processed_time` |

Required columns: `event_timestamp`, `event_date`, `event_type`, `product_id`, `user_id`, `user_session`, `price`

Notes:

- Silver `event_date` is canonical and derived from `to_date(event_timestamp)`, not from the source
  `event_date` column.
- Source `event_date`, `start_of_week`, `end_of_week`, `week_number`, and `week_text` are validation
  inputs. They do not define canonical Silver dates.
- `cohort_week_start` is the Monday of the week containing Silver `event_date`.
- `cohort_week_end` is `cohort_week_start + 6 days`.
- `is_cohort_week_mismatch` is true when source cohort week fields disagree with Silver recomputed
  values.

## Silver Quarantine Dataset

Silver schema: `src/spark_log_lab/schemas/silver.py::SILVER_QUARANTINE_SCHEMA`

| Group | Columns |
|---|---|
| Quarantine metadata | `dataset`, `rule_id`, `rule_name`, `severity`, `failed_columns`, `failed_reason`, `quarantined_at` |
| Original event fields | `event_time`, `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session` |
| Purchase/cohort raw fields | `event_date`, `first_event_date`, `start_of_week`, `week_number`, `end_of_week`, `week_text`, `cohort_index_week`, `week_after` |
| Lineage | `source_file`, `ingest_time`, `batch_id` |

## Silver Shared Keys

- Deduplication keys: `user_id`, `user_session`, `event_timestamp`, `event_type`, `product_id`

Duplicate records are quarantined with `rule_id = DUPLICATE_RECORD`; one deterministic row per key
is retained in the main Silver dataset.
