from __future__ import annotations

from spark_log_lab.common.cli import fail_not_implemented


def main() -> int:
    return fail_not_implemented(
        feature="07_start_streaming_demo",
        next_step="add Flink streaming only after the batch workflow is stable",
    )


if __name__ == "__main__":
    raise SystemExit(main())
