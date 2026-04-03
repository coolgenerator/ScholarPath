"""Manual CLI entrypoint for causal gold evaluation."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.causal_gold_live import (
    DEFAULT_DATASET_PATH,
    DEFAULT_OUTPUT_DIR,
    run_causal_gold_eval,
)


def _parse_case_ids(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run causal gold-set evaluation (legacy vs pywhy + judge).",
    )
    parser.add_argument(
        "--dataset",
        default=str(DEFAULT_DATASET_PATH),
        help="Path to causal gold dataset JSON.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Benchmark output directory (default: .benchmarks/causal).",
    )
    parser.add_argument(
        "--judge-enabled",
        dest="judge_enabled",
        action="store_true",
        default=True,
        help="Enable LLM judge (default: enabled).",
    )
    parser.add_argument(
        "--no-judge",
        dest="judge_enabled",
        action="store_false",
        help="Disable LLM judge.",
    )
    parser.add_argument(
        "--judge-concurrency",
        type=int,
        default=2,
        help="Judge concurrency (default: 2).",
    )
    parser.add_argument(
        "--judge-temperature",
        type=float,
        default=0.1,
        help="Judge temperature (default: 0.1).",
    )
    parser.add_argument(
        "--judge-max-tokens",
        type=int,
        default=1200,
        help="Judge max tokens (default: 1200).",
    )
    parser.add_argument(
        "--max-rpm-total",
        type=int,
        default=180,
        help="Total RPM budget for eval run (must be <=200).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=40,
        help="Sample size from dataset (default: 40).",
    )
    parser.add_argument(
        "--sample-strategy",
        default="full",
        choices=["full", "balanced_fixed"],
        help="Sampling strategy (default: full).",
    )
    parser.add_argument(
        "--case-ids",
        default="",
        help="Comma-separated explicit case ids (highest priority).",
    )
    parser.add_argument(
        "--eval-run-id",
        default="",
        help="Optional explicit eval_run_id for tracing usage rows.",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    case_ids = _parse_case_ids(args.case_ids)
    report = await run_causal_gold_eval(
        dataset_path=Path(args.dataset),
        output_dir=Path(args.output_dir),
        judge_enabled=args.judge_enabled,
        judge_concurrency=args.judge_concurrency,
        judge_temperature=args.judge_temperature,
        judge_max_tokens=args.judge_max_tokens,
        max_rpm_total=args.max_rpm_total,
        sample_size=args.sample_size,
        sample_strategy=args.sample_strategy,
        case_ids=case_ids,
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
