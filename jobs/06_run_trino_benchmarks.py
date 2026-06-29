from __future__ import annotations

from spark_log_lab.common.cli import fail_not_implemented


def main() -> int:
    return fail_not_implemented(
        feature="06_run_trino_benchmarks",
        next_step="add Trino runtime and serving config after Gold is tested",
    )


if __name__ == "__main__":
    raise SystemExit(main())
