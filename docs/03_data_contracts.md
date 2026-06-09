# Data Contracts

## Dataset: log_tracking

### Source

- Path: `data/raw/01-log-tracking.csv`
- Mode: local batch file

### Required Fields

- `event_time`
- `event_type`
- `product_id`
- `user_id`
- `user_session`

### Quality Rules

- Row count must be greater than zero.
- Required fields must exist.
- `event_time` should be parseable as timestamp.
- `event_type` should be normalized before Silver output.
- `price` should be numeric and non-negative when present.

### Breaking Changes

- Removing or renaming required columns.
- Changing timestamp format without updating parser logic.
- Changing `event_type` semantics.
- Changing `price` from numeric-compatible text to non-numeric text.
