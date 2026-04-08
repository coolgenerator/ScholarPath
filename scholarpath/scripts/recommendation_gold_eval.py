"""CLI for recommendation gold-set live evaluation."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from scholarpath.evals.recommendation_gold_live import (
    DEFAULT_OUTPUT_DIR,
    run_recommendation_gold_eval,
)


def _parse_csv(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run recommendation gold eval (persona mini set + optional LLM judge).",
    )
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--case-ids", default="")
    parser.add_argument("--judge-enabled", action="store_true", default=False)
    parser.add_argument("--judge-concurrency", type=int, default=2)
    parser.add_argument("--judge-temperature", type=float, default=0.1)
    parser.add_argument("--judge-max-tokens", type=int, default=900)
    parser.add_argument("--max-rpm-total", type=int, default=180)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--eval-run-id", default="")
    return parser


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    payload = await run_recommendation_gold_eval(
        output_dir=Path(args.output_dir),
        sample_size=args.sample_size,
        case_ids=_parse_csv(args.case_ids),
        judge_enabled=args.judge_enabled,
        judge_concurrency=args.judge_concurrency,
        judge_temperature=args.judge_temperature,
        judge_max_tokens=args.judge_max_tokens,
        max_rpm_total=args.max_rpm_total,
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

