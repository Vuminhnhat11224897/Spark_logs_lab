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
