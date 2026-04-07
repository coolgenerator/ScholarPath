"""CLI for Advisor UX gold evaluation (candidate replay + A/B judge)."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.advisor_ux_live import (
    DEFAULT_OUTPUT_DIR,
    run_advisor_ux_gold_eval,
)


def _parse_case_ids(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _parse_buckets(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Advisor UX gold eval with A/B helper-judge.",
    )
    parser.add_argument(
        "--dataset",
        default="mini",
        choices=["mini", "full", "low_score_smoke"],
        help="Gold dataset size (mini=30, full=100, low_score_smoke=targeted replay set).",
    )
    parser.add_argument(
        "--baseline-run-id",
        default="",
        help="Baseline run id to compare against.",
    )
    parser.add_argument(
        "--judge-enabled",
        dest="judge_enabled",
        action="store_true",
        default=True,
        help="Enable A/B judge stage (default: enabled).",
    )
    parser.add_argument(
        "--no-judge",
        dest="judge_enabled",
        action="store_false",
        help="Disable A/B judge stage.",
    )
    parser.add_argument("--judge-concurrency", type=int, default=2)
    parser.add_argument("--judge-temperature", type=float, default=0.1)
    parser.add_argument("--judge-max-tokens", type=int, default=1200)
    parser.add_argument(
        "--candidate-run-id",
        default="",
        help="Optional explicit run id for candidate execution.",
    )
    parser.add_argument(
        "--case-ids",
        default="",
        help="Comma-separated case ids override.",
    )
    parser.add_argument(
        "--execution-concurrency",
        type=int,
        default=3,
        help="Candidate execution concurrency (default: 3).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Benchmark output root directory.",
    )
    parser.add_argument(
        "--unscored-buckets",
        default="recommendation,strategy,school_query",
        help=(
            "Comma-separated bucket ids excluded from judge scoring. "
            "multi_intent is always unscored."
        ),
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    report = await run_advisor_ux_gold_eval(
        dataset=args.dataset,
        baseline_run_id=(args.baseline_run_id or "").strip() or None,
        judge_enabled=bool(args.judge_enabled),
        judge_concurrency=max(1, int(args.judge_concurrency)),
        judge_temperature=float(args.judge_temperature),
        judge_max_tokens=max(200, int(args.judge_max_tokens)),
        candidate_run_id=(args.candidate_run_id or "").strip() or None,
        case_ids=_parse_case_ids(args.case_ids),
        output_dir=Path(args.output_dir),
        execution_concurrency=max(1, int(args.execution_concurrency)),
        unscored_buckets=_parse_buckets(args.unscored_buckets),
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
