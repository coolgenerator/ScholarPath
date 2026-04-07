"""CLI for recommendation UX gold evaluation (persona replay + A/B judge)."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.recommendation_ux_live import (
    DEFAULT_OUTPUT_DIR,
    run_recommendation_ux_gold_eval,
)


def _parse_case_ids(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run recommendation UX gold eval with persona replay and A/B judge.",
    )
    parser.add_argument(
        "--dataset",
        default="mini",
        help="Dataset alias or path (default: mini).",
    )
    parser.add_argument(
        "--baseline-run-id",
        default="",
        help="Baseline run id to compare against (required when judge enabled).",
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
        "--max-rpm-total",
        type=int,
        default=180,
        help="Total RPM budget for eval run (must be <=200).",
    )
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
    return parser


async def _run(args: argparse.Namespace) -> dict:
    report = await run_recommendation_ux_gold_eval(
        dataset=args.dataset,
        baseline_run_id=(args.baseline_run_id or "").strip() or None,
        judge_enabled=bool(args.judge_enabled),
        judge_concurrency=max(1, int(args.judge_concurrency)),
        judge_temperature=float(args.judge_temperature),
        judge_max_tokens=max(200, int(args.judge_max_tokens)),
        max_rpm_total=max(1, int(args.max_rpm_total)),
        candidate_run_id=(args.candidate_run_id or "").strip() or None,
        case_ids=_parse_case_ids(args.case_ids),
        output_dir=Path(args.output_dir),
        execution_concurrency=max(1, int(args.execution_concurrency)),
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
