# Data Dictionary

## Dataset: log_tracking

Source file: `data/raw/01-log-tracking.csv`

| Column | Expected Type | Required | Notes |
|---|---|---:|---|
| `event_time` | string/timestamp | yes | Source event timestamp |
| `event_type` | string | yes | Event action such as `view`, `cart`, `purchase` |
| `product_id` | integer/string | yes | Product identifier |
| `category_id` | integer/string | no | Product category identifier |
| `category_code` | string | no | Dot-delimited product category path |
| `brand` | string | no | Product brand |
| `price` | double | no | Product price |
| `user_id` | integer/string | yes | User identifier |
| `user_session` | string | yes | User session identifier |

## Dataset: purchase_behavior

Source file: `data/raw/02-purchase-behavior.csv`

This dataset is preserved for later join or cohort exercises. Its final contract will be documented when the join benchmark phase starts.
