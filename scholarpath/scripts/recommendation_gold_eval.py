"""CLI entrypoint for recommendation prefilter/scenario gold evaluation."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.recommendation_gold_live import (
    DEFAULT_DATASET_PATH,
    DEFAULT_OUTPUT_DIR,
    run_recommendation_gold_eval,
)


def _parse_case_ids(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run gold eval for recommendation prefilter + multi-scenario inference.",
    )
    parser.add_argument(
        "--dataset",
        default=str(DEFAULT_DATASET_PATH),
        help="Path to recommendation gold dataset JSON.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Benchmark output directory (default: .benchmarks/recommendation_gold).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Optional sample size from dataset.",
    )
    parser.add_argument(
        "--case-ids",
        default="",
        help="Comma-separated explicit case ids (highest priority).",
    )
    parser.add_argument(
        "--eval-run-id",
        default="",
        help="Optional explicit eval_run_id for tracing.",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    report = await run_recommendation_gold_eval(
        dataset_path=Path(args.dataset),
        output_dir=Path(args.output_dir),
        sample_size=args.sample_size,
        case_ids=_parse_case_ids(args.case_ids),
        eval_run_id=args.eval_run_id or None,
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
