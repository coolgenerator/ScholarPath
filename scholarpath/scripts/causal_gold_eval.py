"""Manual CLI entrypoint for causal gold-set evaluation."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.causal_gold_live import (
    DEFAULT_CAUSAL_GOLD_DATASET_PATH,
    DEFAULT_CAUSAL_OUTPUT_DIR,
    run_causal_gold_eval,
)


def _parse_case_ids(raw: str) -> list[str] | None:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Causal Gold Eval (manual trigger, report mode).",
    )
    parser.add_argument(
        "--dataset",
        default=str(DEFAULT_CAUSAL_GOLD_DATASET_PATH),
        help="Path to causal gold dataset JSON.",
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
        help="Global max RPM target for judge calls (default: 180, hard <= 200).",
    )
    parser.add_argument(
        "--engine-case-concurrency",
        type=int,
        default=4,
        help="Engine case-level concurrency for legacy/pywhy passes (default: 4).",
    )
    parser.add_argument(
        "--warning-mode",
        default="count_silent",
        help="Warning behavior: count_silent|silent|verbose (default: count_silent).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_CAUSAL_OUTPUT_DIR),
        help="Output benchmark directory (default: .benchmarks/causal).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=40,
        help="Number of cases to evaluate (default: 40).",
    )
    parser.add_argument(
        "--sample-strategy",
        default="full",
        help="Sampling strategy: full or balanced_fixed (default: full).",
    )
    parser.add_argument(
        "--case-ids",
        default="",
        help="Comma-separated explicit case_ids (takes precedence over sample strategy).",
    )
    parser.add_argument(
        "--pywhy-model-version-hint",
        default="latest_stable",
        help="PyWhy model version for evaluation (default: latest_stable active model).",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    report = await run_causal_gold_eval(
        dataset_path=Path(args.dataset),
        output_dir=Path(args.output_dir),
        judge_enabled=args.judge_enabled,
        judge_concurrency=args.judge_concurrency,
        judge_temperature=args.judge_temperature,
        judge_max_tokens=args.judge_max_tokens,
        max_rpm_total=args.max_rpm_total,
        engine_case_concurrency=args.engine_case_concurrency,
        warning_mode=args.warning_mode,
        sample_size=args.sample_size,
        sample_strategy=args.sample_strategy,
        case_ids=_parse_case_ids(args.case_ids),
        pywhy_model_version_hint=args.pywhy_model_version_hint,
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
