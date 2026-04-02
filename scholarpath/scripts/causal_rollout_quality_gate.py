"""Manual CLI for causal rollout quality gate."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from scholarpath.evals.causal_rollout_quality import (
    DEFAULT_CAUSAL_ROLLOUT_OUTPUT_DIR,
    run_causal_rollout_quality_gate,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run causal rollout quality gate and emit report artifacts.",
    )
    parser.add_argument(
        "--target-percent",
        type=int,
        default=None,
        help="Expected pywhy primary percent (default: read from runtime env).",
    )
    parser.add_argument(
        "--sample-schools",
        type=int,
        default=64,
        help="How many schools to sample per context (default: 64).",
    )
    parser.add_argument(
        "--contexts",
        type=int,
        default=2,
        help="How many contexts to run for sampling (default: 2).",
    )
    parser.add_argument(
        "--context-prefix",
        default="rollout_quality_gate",
        help="Context prefix for this run (default: rollout_quality_gate).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_CAUSAL_ROLLOUT_OUTPUT_DIR),
        help="Output root directory (default: .benchmarks/causal_rollout).",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=100,
        help="Gate threshold: minimum sampled rows (default: 100).",
    )
    parser.add_argument(
        "--ratio-tolerance",
        type=float,
        default=0.05,
        help="Gate threshold: allowed ratio deviation (default: 0.05).",
    )
    parser.add_argument(
        "--fallback-rate-max",
        type=float,
        default=0.02,
        help="Gate threshold: max fallback rate (default: 0.02).",
    )
    parser.add_argument(
        "--mae-gap-max",
        type=float,
        default=0.03,
        help="Gate threshold: max (pywhy - legacy) MAE gap (default: 0.03).",
    )
    parser.add_argument(
        "--history-window-runs",
        type=int,
        default=24,
        help="Trend window size in recent rollout runs (default: 24).",
    )
    parser.add_argument(
        "--emit-alert",
        dest="emit_alert",
        action="store_true",
        default=True,
        help="Emit structured alert events when thresholds are violated (default: enabled).",
    )
    parser.add_argument(
        "--no-emit-alert",
        dest="emit_alert",
        action="store_false",
        help="Disable alert event emission.",
    )
    parser.add_argument(
        "--pywhy-model-version-hint",
        default="latest_stable",
        help="PyWhy model version hint used for rollout sampling (default: latest_stable).",
    )
    return parser


async def _run(args: argparse.Namespace) -> dict:
    report = await run_causal_rollout_quality_gate(
        target_percent=args.target_percent,
        sample_schools=args.sample_schools,
        contexts=args.contexts,
        context_prefix=args.context_prefix,
        output_dir=Path(args.output_dir),
        min_rows=args.min_rows,
        ratio_tolerance=args.ratio_tolerance,
        fallback_rate_max=args.fallback_rate_max,
        mae_gap_max=args.mae_gap_max,
        history_window_runs=args.history_window_runs,
        emit_alert=args.emit_alert,
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
