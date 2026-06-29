from __future__ import annotations

from spark_log_lab.common.cli import fail_not_implemented


def main() -> int:
    return fail_not_implemented(
        feature="03_build_gold",
        next_step="implement tested Gold marts after Silver quality checks are stable",
    )


if __name__ == "__main__":
    raise SystemExit(main())
