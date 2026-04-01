"""Manual CLI entrypoint for DeepSearch live eval."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.deepsearch_live import (
    DEFAULT_DATASET_PATH,
    DEFAULT_OUTPUT_DIR,
    run_deepsearch_live_eval,
)


def _parse_required_fields(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run DeepSearch live evaluation (manual trigger, report mode).",
    )
    parser.add_argument(
        "--student-id",
        required=True,
        help="Target student UUID used by DeepSearch task context.",
    )
    parser.add_argument(
        "--dataset",
        default=str(DEFAULT_DATASET_PATH),
        help="Path to live eval dataset JSON.",
    )
    parser.add_argument(
        "--required-fields",
        default="",
        help="Comma-separated required fields override.",
    )
    parser.add_argument(
        "--freshness-days",
        type=int,
        default=90,
        help="Freshness window in days (default: 90).",
    )
    parser.add_argument(
        "--max-internal-websearch-per-school",
        type=int,
        default=1,
        help="Hard cap for internal web_search fallback per school.",
    )
    parser.add_argument(
        "--budget-mode",
        default="balanced",
        help="Budget mode passed to DeepSearch (default: balanced).",
    )
    parser.add_argument(
        "--second-pass",
        dest="second_pass",
        action="store_true",
        default=True,
        help="Enable second pass to measure DB hit uplift (default: enabled).",
    )
    parser.add_argument(
        "--no-second-pass",
        dest="second_pass",
        action="store_false",
        help="Disable second pass.",
    )
    parser.add_argument(
        "--judge-enabled",
        dest="judge_enabled",
        action="store_true",
        default=True,
        help="Enable LLM-as-judge stage (default: enabled).",
    )
    parser.add_argument(
        "--no-judge",
        dest="judge_enabled",
        action="store_false",
        help="Disable LLM-as-judge stage.",
    )
    parser.add_argument(
        "--judge-concurrency",
        type=int,
        default=2,
        help="Judge school-call concurrency (default: 2).",
    )
    parser.add_argument(
        "--judge-temperature",
        type=float,
        default=0.1,
        help="Judge model temperature (default: 0.1).",
    )
    parser.add_argument(
        "--judge-max-tokens",
        type=int,
        default=1200,
        help="Judge max output tokens per call (default: 1200).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output benchmark directory (default: .benchmarks/deepsearch).",
    )
    parser.add_argument(
        "--celery-timeout-seconds",
        type=int,
        default=900,
        help="Per-pass celery timeout in seconds.",
    )
    parser.add_argument(
        "--celery-poll-interval-seconds",
        type=float,
        default=2.0,
        help="Polling interval while waiting for celery result.",
    )
    parser.add_argument(
        "--cold-reset-out-group",
        dest="cold_reset_out_group",
        action="store_true",
        default=True,
        help="Pre-clean out_db cohort rows in DB before pass1 (default: enabled).",
    )
    parser.add_argument(
        "--no-cold-reset-out-group",
        dest="cold_reset_out_group",
        action="store_false",
        help="Disable out_db cold-reset pre-clean.",
    )
    parser.add_argument(
        "--cold-reset-window-days",
        type=int,
        default=None,
        help="Window days for cold-reset pre-clean (default: use freshness-days).",
    )
    parser.add_argument(
        "--validate-cohort",
        dest="validate_cohort",
        action="store_true",
        default=True,
        help="Validate in_db/out_db preflight before pass1 (default: enabled).",
    )
    parser.add_argument(
        "--no-validate-cohort",
        dest="validate_cohort",
        action="store_false",
        help="Disable cohort preflight validation.",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    required_fields = _parse_required_fields(args.required_fields)
    report = await run_deepsearch_live_eval(
        student_id=args.student_id,
        dataset_path=Path(args.dataset),
        required_fields=required_fields,
        freshness_days=args.freshness_days,
        max_internal_websearch_calls_per_school=args.max_internal_websearch_per_school,
        budget_mode=args.budget_mode,
        second_pass=args.second_pass,
        judge_enabled=args.judge_enabled,
        judge_concurrency=args.judge_concurrency,
        judge_temperature=args.judge_temperature,
        judge_max_tokens=args.judge_max_tokens,
        output_dir=Path(args.output_dir),
        celery_timeout_seconds=args.celery_timeout_seconds,
        celery_poll_interval_seconds=args.celery_poll_interval_seconds,
        cold_reset_out_group=args.cold_reset_out_group,
        cold_reset_window_days=args.cold_reset_window_days,
        validate_cohort=args.validate_cohort,
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
