"""CLI for advisor orchestrator gold evaluation."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.advisor_orchestrator_live import (
    DEFAULT_OUTPUT_DIR,
    run_advisor_orchestrator_eval,
)


def _parse_csv(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run advisor orchestrator eval (mini/full, optional re-edit, optional judge).",
    )
    parser.add_argument("--include-reedit", action="store_true", default=False)
    parser.add_argument("--sample-size", type=int, default=40)
    parser.add_argument("--case-ids", default="")
    parser.add_argument("--reedit-sample-size", type=int, default=None)
    parser.add_argument("--reedit-case-ids", default="")
    parser.add_argument(
        "--execution-lane",
        choices=["stub", "real", "both"],
        default="both",
    )
    parser.add_argument("--warning-gate", action="store_true", default=True)
    parser.add_argument("--no-warning-gate", dest="warning_gate", action="store_false")
    parser.add_argument("--judge-enabled", action="store_true", default=False)
    parser.add_argument("--judge-concurrency", type=int, default=2)
    parser.add_argument("--judge-temperature", type=float, default=0.1)
    parser.add_argument("--judge-max-tokens", type=int, default=900)
    parser.add_argument("--max-rpm-total", type=int, default=180)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--eval-run-id", default="")
    return parser


async def _run(args: argparse.Namespace) -> dict:
    payload = await run_advisor_orchestrator_eval(
        include_reedit=args.include_reedit,
        sample_size=args.sample_size,
        case_ids=_parse_csv(args.case_ids),
        reedit_sample_size=args.reedit_sample_size,
        reedit_case_ids=_parse_csv(args.reedit_case_ids),
        execution_lane=args.execution_lane,
        warning_gate=args.warning_gate,
        judge_enabled=args.judge_enabled,
        judge_concurrency=args.judge_concurrency,
        judge_temperature=args.judge_temperature,
        judge_max_tokens=args.judge_max_tokens,
        max_rpm_total=args.max_rpm_total,
        output_dir=Path(args.output_dir),
        eval_run_id=args.eval_run_id or None,
    )
    out = payload.to_dict()
    print(json.dumps(out, ensure_ascii=False, indent=2))
    return out


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
