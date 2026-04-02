"""Manual CLI entrypoint for advisor orchestrator gold evaluation."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.advisor_orchestrator_live import (
    DEFAULT_DATASET_PATH,
    DEFAULT_REEDIT_DATASET_PATH,
    DEFAULT_OUTPUT_DIR,
    run_advisor_orchestrator_eval,
)


def _parse_case_ids(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _json_default(value):  # type: ignore[no-untyped-def]
    if isinstance(value, (set, frozenset)):
        try:
            return sorted(value)
        except TypeError:
            return sorted(str(item) for item in value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Advisor Orchestrator Gold Eval (isolated orchestration + judge).",
    )
    parser.add_argument(
        "--dataset",
        default=str(DEFAULT_DATASET_PATH),
        help="Path to advisor orchestrator gold dataset JSON.",
    )
    parser.add_argument(
        "--reedit-dataset",
        default=str(DEFAULT_REEDIT_DATASET_PATH),
        help="Path to advisor re-edit gold dataset JSON.",
    )
    parser.add_argument(
        "--include-reedit",
        dest="include_reedit",
        action="store_true",
        default=True,
        help="Include re-edit phase in merged scoring (default: enabled).",
    )
    parser.add_argument(
        "--no-reedit",
        dest="include_reedit",
        action="store_false",
        help="Disable re-edit phase and only run orchestrator set.",
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
        help="Judge case-call concurrency (default: 2).",
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
        "--max-rpm-total",
        type=int,
        default=180,
        help="Global max RPM target for judge/router calls (default: 180, hard <= 200).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=40,
        help="Number of cases to run (default: 40). Ignored when --case-ids is provided.",
    )
    parser.add_argument(
        "--case-ids",
        default="",
        help="Comma-separated explicit case_ids to run.",
    )
    parser.add_argument(
        "--reedit-sample-size",
        type=int,
        default=None,
        help="Number of re-edit cases to run (default: full 12 when re-edit enabled).",
    )
    parser.add_argument(
        "--reedit-case-ids",
        default="",
        help="Comma-separated explicit re-edit case_ids to run.",
    )
    parser.add_argument(
        "--execution-lane",
        choices=("stub", "real", "both"),
        default="both",
        help="Execution lane for orchestrator eval (default: both).",
    )
    parser.add_argument(
        "--real-capabilities",
        default="",
        help=(
            "Comma-separated capability ids for real lane. "
            "Default covers undergrad.school.recommend, undergrad.school.query, offer.compare, offer.what_if."
        ),
    )
    parser.add_argument(
        "--warning-gate",
        dest="warning_gate",
        action="store_true",
        default=True,
        help="Treat warning counts / lane-gate failures as bad status (default: enabled).",
    )
    parser.add_argument(
        "--no-warning-gate",
        dest="warning_gate",
        action="store_false",
        help="Disable warning-gate override.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output benchmark directory (default: .benchmarks/advisor_orchestrator).",
    )
    parser.add_argument(
        "--usage-enabled",
        dest="usage_enabled",
        action="store_true",
        default=True,
        help="Enable token_usage DB aggregation in eval report (default: enabled).",
    )
    parser.add_argument(
        "--no-usage",
        dest="usage_enabled",
        action="store_false",
        help="Disable token_usage DB aggregation (useful for isolated tests).",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    report = await run_advisor_orchestrator_eval(
        dataset_path=Path(args.dataset),
        reedit_dataset_path=Path(args.reedit_dataset),
        include_reedit=bool(args.include_reedit),
        output_dir=Path(args.output_dir),
        judge_enabled=args.judge_enabled,
        judge_concurrency=args.judge_concurrency,
        judge_temperature=args.judge_temperature,
        judge_max_tokens=args.judge_max_tokens,
        max_rpm_total=args.max_rpm_total,
        sample_size=args.sample_size,
        case_ids=_parse_case_ids(args.case_ids),
        reedit_sample_size=args.reedit_sample_size,
        reedit_case_ids=_parse_case_ids(args.reedit_case_ids),
        execution_lane=str(args.execution_lane),
        real_capabilities=_parse_case_ids(args.real_capabilities),
        warning_gate=bool(args.warning_gate),
        usage_enabled=bool(args.usage_enabled),
    )
    payload = report.to_dict()
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))
    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
