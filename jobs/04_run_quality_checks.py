from __future__ import annotations

from spark_log_lab.common.cli import fail_not_implemented


def main() -> int:
    return fail_not_implemented(
        feature="04_run_quality_checks",
        next_step="wire row-count, null-rate, and duplicate checks over Silver outputs",
    )


if __name__ == "__main__":
    raise SystemExit(main())
