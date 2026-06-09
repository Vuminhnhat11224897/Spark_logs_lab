from __future__ import annotations

import argparse


def add_common_pipeline_args(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--batch-id", default=None, help="Optional batch identifier.")
    parser.add_argument("--event-date", default=None, help="Optional event date, for example 2019-11-01.")
    parser.add_argument("--sample", type=float, default=0.0, help="Optional sample fraction.")
    parser.add_argument(
        "--mode",
        choices=["full-refresh", "date-partition"],
        default="full-refresh",
        help="Pipeline run mode.",
    )
    return parser
