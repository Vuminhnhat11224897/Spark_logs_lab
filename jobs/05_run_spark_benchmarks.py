from __future__ import annotations

from spark_log_lab.common.cli import fail_not_implemented


def main() -> int:
    return fail_not_implemented(
        feature="05_run_spark_benchmarks",
        next_step="build a benchmark runner after Gold marts exist",
    )


if __name__ == "__main__":
    raise SystemExit(main())
