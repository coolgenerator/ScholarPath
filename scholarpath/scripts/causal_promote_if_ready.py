"""Promote latest staged champion if shadow gate is clean."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from scholarpath.causal_engine.training import promote_model
from scholarpath.evals.causal_rollout_quality import run_causal_rollout_quality_gate


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote latest stage4 champion if rollout gate is clean.",
    )
    parser.add_argument(
        "--staged-output-dir",
        default=".benchmarks/causal_staged",
        help="Staged training output root.",
    )
    parser.add_argument(
        "--shadow-window-hours",
        type=int,
        default=24,
        help="Shadow gate window in hours.",
    )
    parser.add_argument(
        "--shadow-min-rows",
        type=int,
        default=3,
        help="Minimum shadow rows required.",
    )
    parser.add_argument(
        "--shadow-target-percent",
        type=int,
        default=100,
        help="Expected pywhy shadow percent.",
    )
    return parser


def _load_latest_stage4_candidate(staged_root: Path) -> tuple[str | None, dict[str, Any]]:
    if not staged_root.exists():
        return None, {"reason": "staged_output_not_found"}
    runs = sorted([item for item in staged_root.iterdir() if item.is_dir()], key=lambda p: p.name, reverse=True)
    for run_dir in runs:
        gate_path = run_dir / "gate_results.json"
        summary_path = run_dir / "stage_summary.json"
        report_path = run_dir / "report.json"
        if not gate_path.exists() or not summary_path.exists():
            continue
        gate = json.loads(gate_path.read_text(encoding="utf-8"))
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
        overrides_applied = report.get("overrides_applied", {}) if isinstance(report, dict) else {}
        if isinstance(overrides_applied, dict) and "stage4_min_admission_rows" in overrides_applied:
            continue
        stage4 = gate.get("stage_4") if isinstance(gate, dict) else None
        champion = summary.get("stage_4", {}).get("champion") if isinstance(summary, dict) else None
        model_version = None
        if isinstance(champion, dict):
            model_version = champion.get("model_version")
        if stage4 and stage4.get("passed") and model_version:
            return str(model_version), {
                "run_dir": str(run_dir),
                "stage4_gate": stage4,
            }
    return None, {"reason": "no_passed_stage4_candidate"}


async def _run(args: argparse.Namespace) -> dict:
    staged_root = Path(args.staged_output_dir)
    model_version, context = _load_latest_stage4_candidate(staged_root)
    if not model_version:
        payload = {
            "attempted": False,
            "promoted": False,
            "model_version": None,
            "reasons": [context.get("reason", "no_candidate")],
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return payload

    gate = await run_causal_rollout_quality_gate(
        window_hours=args.shadow_window_hours,
        target_percent=args.shadow_target_percent,
        min_rows=args.shadow_min_rows,
    )
    reasons: list[str] = []
    if not gate.passed:
        reasons.append("shadow_gate_failed")
        reasons.extend(gate.alerts)
        payload = {
            "attempted": True,
            "promoted": False,
            "model_version": model_version,
            "reasons": reasons,
            "shadow_gate": gate.to_dict(),
            "context": context,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return payload

    promote_result = await promote_model(model_version=model_version)
    promoted = promote_result.get("status") == "ok"
    if not promoted:
        reasons.append("promote_failed")

    payload = {
        "attempted": True,
        "promoted": promoted,
        "model_version": model_version,
        "reasons": reasons,
        "shadow_gate": gate.to_dict(),
        "promote_result": promote_result,
        "context": context,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
