"""CLI for causal shadow rollout quality gate."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.causal_rollout_quality import (
    DEFAULT_OUTPUT_DIR,
    run_causal_rollout_quality_gate,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run causal shadow rollout quality gate.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=24,
        help="Lookback window in hours (default: 24).",
    )
    parser.add_argument(
        "--target-percent",
        type=int,
        default=100,
        help="Expected pywhy primary percent (default: 100).",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=3,
        help="Minimum required shadow rows in window (default: 3).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output root for quality artifacts.",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    report = await run_causal_rollout_quality_gate(
        window_hours=args.window_hours,
        target_percent=args.target_percent,
        min_rows=args.min_rows,
        output_dir=Path(args.output_dir),
    )
    payload = report.to_dict()
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
