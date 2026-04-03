"""Staged causal training orchestrator (Stage1-4, final-stage promote)."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, func, select

from scholarpath.causal_engine.training import promote_model, train_full_graph_model
from scholarpath.db.models import CausalFeatureSnapshot, CausalModelRegistry, CausalOutcomeEvent
from scholarpath.db.session import async_session_factory
from scholarpath.evals.causal_gold_live import run_causal_gold_eval

_OUTCOMES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]

_STAGE_DATA_THRESHOLDS: dict[int, dict[str, int]] = {
    1: {
        "snapshots": 3_000,
        "per_outcome": 3_000,
        "admission_true": 400,
        "other_true_or_anchor": 150,
    },
    2: {
        "snapshots": 7_000,
        "per_outcome": 7_000,
        "admission_true": 1_200,
        "other_true_or_anchor": 400,
    },
    3: {
        "snapshots": 12_000,
        "per_outcome": 12_000,
        "admission_true": 2_200,
        "other_true_or_anchor": 800,
    },
    4: {
        "snapshots": 15_000,
        "per_outcome": 15_000,
        "admission_true": 3_000,
        "other_true_or_anchor": 1_000,
    },
}

_STAGE_MIN_ROWS_PER_OUTCOME = {1: 200, 2: 400, 3: 800, 4: 1000}

_STAGE_GATE_THRESHOLDS: dict[int, dict[str, float]] = {
    1: {"judge": 65.0, "mae_margin": 0.01, "field_pass": 0.0, "fallback": 0.05},
    2: {"judge": 72.0, "mae_margin": 0.0, "field_pass": 0.50, "fallback": 0.03},
    3: {"judge": 78.0, "mae_margin": -0.01, "field_pass": 0.58, "fallback": 0.02},
    4: {"judge": 80.0, "mae_margin": 0.0, "field_pass": 0.60, "fallback": 0.02},
}


@dataclass(slots=True)
class CandidateResult:
    candidate_id: str
    model_version: str | None
    train_status: str
    eval_status: str
    gate_passed: bool
    score: float
    metrics: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run staged causal training with strict stage gates.",
    )
    parser.add_argument(
        "--stage",
        default="all",
        choices=["1", "2", "3", "4", "all"],
        help="Stage selector (default: all).",
    )
    parser.add_argument(
        "--train-candidates-per-stage",
        type=int,
        default=3,
        help="Candidate models per stage (default: 3).",
    )
    parser.add_argument(
        "--max-rpm-total",
        type=int,
        default=200,
        help="Total RPM budget, hard max 200.",
    )
    parser.add_argument(
        "--judge-concurrency",
        type=int,
        default=2,
        help="Judge concurrency for gold eval.",
    )
    parser.add_argument(
        "--promote-on-final-pass",
        dest="promote_on_final_pass",
        action="store_true",
        default=True,
        help="Promote stage4 champion when final pass conditions are met.",
    )
    parser.add_argument(
        "--no-promote-on-final-pass",
        dest="promote_on_final_pass",
        action="store_false",
        help="Do not auto promote even if stage4 passes.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/causal_staged",
        help="Output root for staged artifacts.",
    )
    return parser


async def _collect_coverage(lookback_days: int = 540) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, lookback_days))
    async with async_session_factory() as session:
        snapshots = int(
            (await session.scalar(
                select(func.count())
                .select_from(CausalFeatureSnapshot)
                .where(CausalFeatureSnapshot.observed_at >= cutoff)
            ))
            or 0
        )
        rows = (
            (
                await session.execute(
                    select(
                        CausalOutcomeEvent.outcome_name,
                        CausalOutcomeEvent.label_type,
                        CausalOutcomeEvent.label_confidence,
                    ).where(CausalOutcomeEvent.observed_at >= cutoff)
                )
            )
            .all()
        )

    counts = {key: 0 for key in _OUTCOMES}
    true_counts = {key: 0 for key in _OUTCOMES}
    anchor_counts = {key: 0 for key in _OUTCOMES}
    for outcome_name, label_type, label_confidence in rows:
        outcome = str(outcome_name)
        if outcome not in counts:
            continue
        counts[outcome] += 1
        if str(label_type).lower() == "true":
            true_counts[outcome] += 1
        if float(label_confidence or 0.0) >= 0.9:
            anchor_counts[outcome] += 1
    return {
        "snapshots": snapshots,
        "counts": counts,
        "true_counts": true_counts,
        "anchor_counts": anchor_counts,
    }


async def _get_active_model_version() -> str | None:
    async with async_session_factory() as session:
        row = await session.scalar(
            select(CausalModelRegistry)
            .where(CausalModelRegistry.is_active.is_(True))
            .order_by(CausalModelRegistry.updated_at.desc())
            .limit(1)
        )
    return str(row.model_version) if row else None


def _check_stage_data_gate(stage: int, coverage: dict[str, Any]) -> tuple[bool, list[str]]:
    cfg = _STAGE_DATA_THRESHOLDS[stage]
    reasons: list[str] = []
    if int(coverage["snapshots"]) < cfg["snapshots"]:
        reasons.append(f"snapshots<{cfg['snapshots']}")
    for outcome in _OUTCOMES:
        count = int(coverage["counts"].get(outcome, 0))
        if count < cfg["per_outcome"]:
            reasons.append(f"{outcome}_rows<{cfg['per_outcome']}")
    if int(coverage["true_counts"].get("admission_probability", 0)) < cfg["admission_true"]:
        reasons.append(f"admission_true<{cfg['admission_true']}")
    for outcome in _OUTCOMES:
        if outcome == "admission_probability":
            continue
        val = int(coverage["true_counts"].get(outcome, 0)) + int(coverage["anchor_counts"].get(outcome, 0))
        if val < cfg["other_true_or_anchor"]:
            reasons.append(f"{outcome}_true_or_anchor<{cfg['other_true_or_anchor']}")
    return len(reasons) == 0, reasons


def _stage_pass(stage: int, report: dict[str, Any]) -> tuple[bool, list[str]]:
    gate = _STAGE_GATE_THRESHOLDS[stage]
    metrics = report.get("metrics", {})
    pywhy_mae = float(metrics.get("mae_overall_pywhy", 1.0))
    legacy_mae = float(metrics.get("mae_overall_legacy", 1.0))
    judge_score = float(
        metrics.get("judge_score_pywhy", metrics.get("judge_overall_score", 0.0))
    )
    field_pass = float(report.get("pywhy_pass", {}).get("judge_field_pass_rate", 0.0))
    fallback = float(report.get("pywhy_pass", {}).get("fallback_rate", 1.0))
    rate_limit_error_count = int(metrics.get("rate_limit_error_count", 1))

    reasons: list[str] = []
    if judge_score < gate["judge"]:
        reasons.append(f"judge<{gate['judge']}")
    if pywhy_mae > legacy_mae + gate["mae_margin"]:
        reasons.append(f"pywhy_mae>{legacy_mae + gate['mae_margin']:.4f}")
    if field_pass < gate["field_pass"]:
        reasons.append(f"field_pass<{gate['field_pass']}")
    if fallback > gate["fallback"]:
        reasons.append(f"fallback>{gate['fallback']}")
    if stage == 4 and rate_limit_error_count > 0:
        reasons.append("rate_limit_error_count>0")
    return len(reasons) == 0, reasons


def _candidate_score(report: dict[str, Any]) -> float:
    metrics = report.get("metrics", {})
    judge = float(
        metrics.get("judge_score_pywhy", metrics.get("judge_overall_score", 0.0))
    ) / 100.0
    pywhy_mae = float(metrics.get("mae_overall_pywhy", 1.0))
    field_pass = float(report.get("pywhy_pass", {}).get("judge_field_pass_rate", 0.0))
    fallback = float(report.get("pywhy_pass", {}).get("fallback_rate", 1.0))
    normalized_mae = max(0.0, min(1.0, pywhy_mae))
    score = (
        0.35 * judge
        + 0.30 * (1.0 - normalized_mae)
        + 0.20 * field_pass
        + 0.15 * (1.0 - fallback)
    )
    return round(score, 6)


async def _run_stage(
    *,
    stage: int,
    run_id: str,
    candidates: int,
    max_rpm_total: int,
    judge_concurrency: int,
) -> dict[str, Any]:
    coverage = await _collect_coverage()
    data_gate_ok, data_gate_reasons = _check_stage_data_gate(stage, coverage)
    stage_result: dict[str, Any] = {
        "stage": stage,
        "coverage": coverage,
        "data_gate_passed": data_gate_ok,
        "data_gate_reasons": data_gate_reasons,
        "candidates": [],
        "champion": None,
        "passed": False,
    }
    if not data_gate_ok:
        return stage_result

    all_candidates: list[CandidateResult] = []
    for idx in range(1, max(1, candidates) + 1):
        candidate_id = f"s{stage}c{idx}"
        train_payload = await train_full_graph_model(
            dataset_version=None,
            profile="high_quality",
            lookback_days=540,
            min_rows_per_outcome=_STAGE_MIN_ROWS_PER_OUTCOME[stage],
            calibration_enabled=True,
        )
        model_version = str(train_payload.get("model_version") or "") or None
        if train_payload.get("status") != "ok" or not model_version:
            all_candidates.append(
                CandidateResult(
                    candidate_id=candidate_id,
                    model_version=model_version,
                    train_status=str(train_payload.get("status") or "failed"),
                    eval_status="skipped",
                    gate_passed=False,
                    score=0.0,
                    metrics={"train_result": train_payload},
                    reasons=["train_failed"],
                )
            )
            continue

        await promote_model(model_version=model_version)
        eval_run_id = f"{run_id}-{candidate_id}"
        report_obj = await run_causal_gold_eval(
            dataset_path="scholarpath/evals/datasets/causal_gold_v1.json",
            judge_enabled=True,
            judge_concurrency=judge_concurrency,
            max_rpm_total=max_rpm_total,
            sample_size=40,
            sample_strategy="full",
            eval_run_id=eval_run_id,
        )
        report = report_obj.to_dict()
        gate_passed, reasons = _stage_pass(stage, report)
        score = _candidate_score(report) if gate_passed else 0.0
        all_candidates.append(
            CandidateResult(
                candidate_id=candidate_id,
                model_version=model_version,
                train_status="ok",
                eval_status=report_obj.status,
                gate_passed=gate_passed,
                score=score,
                metrics={
                    "judge_score_pywhy": report["metrics"].get("judge_score_pywhy"),
                    "judge_score_legacy": report["metrics"].get("judge_score_legacy"),
                    "judge_overall_score": report["metrics"].get("judge_overall_score"),
                    "mae_overall_pywhy": report["metrics"].get("mae_overall_pywhy"),
                    "mae_overall_legacy": report["metrics"].get("mae_overall_legacy"),
                    "judge_field_pass_rate_pywhy": report["pywhy_pass"].get("judge_field_pass_rate"),
                    "fallback_rate_pywhy": report["pywhy_pass"].get("fallback_rate"),
                    "rate_limit_error_count": report["metrics"].get("rate_limit_error_count"),
                    "eval_run_id": report_obj.run_id,
                },
                reasons=reasons,
            )
        )

    winners = [item for item in all_candidates if item.gate_passed]
    champion = max(winners, key=lambda item: item.score, default=None)
    stage_result["candidates"] = [item.to_dict() for item in all_candidates]
    stage_result["champion"] = champion.to_dict() if champion else None
    stage_result["passed"] = champion is not None
    return stage_result


def _has_previous_stage4_pass(history_csv: Path) -> bool:
    if not history_csv.exists():
        return False
    rows = list(csv.DictReader(history_csv.read_text(encoding="utf-8").splitlines()))
    if not rows:
        return False
    for row in reversed(rows):
        if row.get("stage") == "4":
            return str(row.get("passed", "")).lower() == "true"
    return False


def _append_history(history_csv: Path, *, stage: int, passed: bool, champion_model_version: str | None) -> None:
    history_csv.parent.mkdir(parents=True, exist_ok=True)
    exists = history_csv.exists()
    row = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "passed": bool(passed),
        "champion_model_version": champion_model_version or "",
    }
    with history_csv.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_rpm_total > 200:
        raise ValueError("max-rpm-total must be <= 200")

    run_id = f"causal-staged-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    output_root = Path(args.output_dir)
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    active_model_before = await _get_active_model_version()

    stages = [1, 2, 3, 4] if args.stage == "all" else [int(args.stage)]
    summary: dict[str, Any] = {}
    gate_results: dict[str, Any] = {}

    for stage in stages:
        result = await _run_stage(
            stage=stage,
            run_id=run_id,
            candidates=args.train_candidates_per_stage,
            max_rpm_total=args.max_rpm_total,
            judge_concurrency=args.judge_concurrency,
        )
        summary[f"stage_{stage}"] = result
        gate_results[f"stage_{stage}"] = {
            "passed": bool(result.get("passed")),
            "champion_model_version": (result.get("champion") or {}).get("model_version"),
            "data_gate_passed": bool(result.get("data_gate_passed")),
        }
        _append_history(
            output_root / "history.csv",
            stage=stage,
            passed=bool(result.get("passed")),
            champion_model_version=(result.get("champion") or {}).get("model_version"),
        )
        if not result.get("passed"):
            break

    promotion = {
        "attempted": False,
        "promoted": False,
        "model_version": None,
        "reasons": [],
    }

    if stages[-1] == 4 and summary.get("stage_4", {}).get("passed") and args.promote_on_final_pass:
        current_champion = summary["stage_4"]["champion"]
        model_version = current_champion.get("model_version") if isinstance(current_champion, dict) else None
        previous_ok = _has_previous_stage4_pass(output_root / "history.csv")
        if not previous_ok:
            promotion["reasons"].append("stage4_needs_two_consecutive_passes")
        elif model_version:
            promote_result = await promote_model(model_version=str(model_version))
            promotion["attempted"] = True
            promotion["promoted"] = promote_result.get("status") == "ok"
            promotion["model_version"] = model_version
            if not promotion["promoted"]:
                promotion["reasons"].append("promote_failed")
            promotion["promote_result"] = promote_result
        else:
            promotion["reasons"].append("missing_champion_model_version")

    active_model_after = await _get_active_model_version()
    restored_active_model = None
    if not args.promote_on_final_pass and active_model_before and active_model_after != active_model_before:
        restore_result = await promote_model(model_version=active_model_before)
        if restore_result.get("status") == "ok":
            restored_active_model = active_model_before
            active_model_after = await _get_active_model_version()
        else:
            promotion["reasons"].append("restore_active_failed")
            promotion["restore_result"] = restore_result

    payload = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "stage": args.stage,
            "train_candidates_per_stage": args.train_candidates_per_stage,
            "max_rpm_total": args.max_rpm_total,
            "judge_concurrency": args.judge_concurrency,
            "promote_on_final_pass": args.promote_on_final_pass,
        },
        "stage_summary": summary,
        "gate_results": gate_results,
        "promotion_decision": promotion,
        "active_model_before": active_model_before,
        "active_model_after": active_model_after,
        "restored_active_model": restored_active_model,
    }

    (run_dir / "stage_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "gate_results.json").write_text(
        json.dumps(gate_results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "promotion_decision.json").write_text(
        json.dumps(promotion, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "report.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
