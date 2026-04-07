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


@dataclass(slots=True)
class CandidateTrainProfile:
    candidate_id: str
    profile: str = "high_quality"
    calibration_enabled: bool = True
    calibration_profile: str = "robust"
    calibration_disabled_outcomes: list[str] = field(default_factory=list)


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
    parser.add_argument(
        "--stage4-min-admission-rows",
        type=int,
        default=None,
        help=(
            "One-off Stage4 override for admission_probability row gate. "
            "Only applies when Stage4 is executed."
        ),
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


async def _clear_active_model() -> dict[str, Any]:
    async with async_session_factory() as session:
        rows = (
            await session.execute(
                select(CausalModelRegistry).where(CausalModelRegistry.is_active.is_(True))
            )
        ).scalars().all()
        cleared = 0
        for row in rows:
            row.is_active = False
            if str(row.status or "").lower() == "active":
                row.status = "trained"
            cleared += 1
        await session.commit()
    return {"status": "ok", "cleared": cleared}


def _candidate_train_profiles(stage: int, candidates: int) -> list[CandidateTrainProfile]:
    requested = max(1, candidates)
    if stage != 1:
        return [
            CandidateTrainProfile(candidate_id=f"s{stage}c{idx}")
            for idx in range(1, requested + 1)
        ]

    profiles = [
        CandidateTrainProfile(
            candidate_id="s1c1",
            profile="s1c1_raw_v2_robust_all",
            calibration_enabled=True,
            calibration_profile="robust",
            calibration_disabled_outcomes=[],
        ),
        CandidateTrainProfile(
            candidate_id="s1c2",
            profile="s1c2_raw_v2_robust_no_life_phd",
            calibration_enabled=True,
            calibration_profile="robust",
            calibration_disabled_outcomes=["life_satisfaction", "phd_probability"],
        ),
        CandidateTrainProfile(
            candidate_id="s1c3",
            profile="s1c3_raw_v2_calibration_disabled",
            calibration_enabled=False,
            calibration_profile="disabled",
            calibration_disabled_outcomes=[],
        ),
    ]
    if requested <= len(profiles):
        return profiles[:requested]
    for idx in range(len(profiles) + 1, requested + 1):
        profiles.append(
            CandidateTrainProfile(
                candidate_id=f"s1c{idx}",
                profile=f"s1c{idx}_raw_v2_robust_all",
                calibration_enabled=True,
                calibration_profile="robust",
                calibration_disabled_outcomes=[],
            )
        )
    return profiles


def _resolve_stage_data_thresholds(
    *,
    stage4_min_admission_rows: int | None = None,
) -> tuple[dict[int, dict[str, int]], dict[str, Any]]:
    thresholds: dict[int, dict[str, int]] = {
        int(stage): {k: int(v) for k, v in cfg.items()}
        for stage, cfg in _STAGE_DATA_THRESHOLDS.items()
    }
    overrides_applied: dict[str, Any] = {}
    if stage4_min_admission_rows is not None:
        stage4_cfg = dict(thresholds.get(4, {}))
        stage4_cfg["admission_rows"] = int(stage4_min_admission_rows)
        thresholds[4] = stage4_cfg
        overrides_applied["stage4_min_admission_rows"] = int(stage4_min_admission_rows)
    return thresholds, overrides_applied


def _check_stage_data_gate(
    stage: int,
    coverage: dict[str, Any],
    stage_data_thresholds: dict[int, dict[str, int]] | None = None,
) -> tuple[bool, list[str]]:
    cfg = (stage_data_thresholds or _STAGE_DATA_THRESHOLDS)[stage]
    reasons: list[str] = []
    if int(coverage["snapshots"]) < cfg["snapshots"]:
        reasons.append(f"snapshots<{cfg['snapshots']}")
    for outcome in _OUTCOMES:
        count = int(coverage["counts"].get(outcome, 0))
        min_rows = int(cfg.get("per_outcome", 0))
        if outcome == "admission_probability":
            min_rows = int(cfg.get("admission_rows", min_rows))
        if count < min_rows:
            reasons.append(f"{outcome}_rows<{min_rows}")
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


def _is_strict_stage4_threshold(cfg: dict[str, Any] | None) -> bool:
    if not isinstance(cfg, dict):
        return False
    default_stage4 = _STAGE_DATA_THRESHOLDS.get(4, {})
    strict_rows = int(default_stage4.get("per_outcome", 0))
    admission_rows = int(cfg.get("admission_rows", cfg.get("per_outcome", 0)) or 0)
    return admission_rows >= strict_rows


async def _run_stage(
    *,
    stage: int,
    run_id: str,
    candidates: int,
    max_rpm_total: int,
    judge_concurrency: int,
    stage_data_thresholds: dict[int, dict[str, int]] | None = None,
) -> dict[str, Any]:
    coverage = await _collect_coverage()
    data_gate_ok, data_gate_reasons = _check_stage_data_gate(
        stage,
        coverage,
        stage_data_thresholds=stage_data_thresholds,
    )
    effective_cfg = dict((stage_data_thresholds or _STAGE_DATA_THRESHOLDS)[stage])
    stage_result: dict[str, Any] = {
        "stage": stage,
        "coverage": coverage,
        "effective_data_thresholds": effective_cfg,
        "data_gate_passed": data_gate_ok,
        "data_gate_reasons": data_gate_reasons,
        "candidates": [],
        "champion": None,
        "passed": False,
    }
    if not data_gate_ok:
        return stage_result

    all_candidates: list[CandidateResult] = []
    for candidate_profile in _candidate_train_profiles(stage, candidates):
        candidate_id = candidate_profile.candidate_id
        train_payload = await train_full_graph_model(
            dataset_version=None,
            profile=candidate_profile.profile,
            lookback_days=540,
            min_rows_per_outcome=_STAGE_MIN_ROWS_PER_OUTCOME[stage],
            calibration_enabled=candidate_profile.calibration_enabled,
            calibration_profile=candidate_profile.calibration_profile,
            calibration_disabled_outcomes=candidate_profile.calibration_disabled_outcomes,
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
                    metrics={
                        "train_profile": {
                            "profile": candidate_profile.profile,
                            "calibration_enabled": candidate_profile.calibration_enabled,
                            "calibration_profile": candidate_profile.calibration_profile,
                            "calibration_disabled_outcomes": candidate_profile.calibration_disabled_outcomes,
                        },
                        "train_result": train_payload,
                    },
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
                    "train_profile": {
                        "profile": candidate_profile.profile,
                        "calibration_enabled": candidate_profile.calibration_enabled,
                        "calibration_profile": candidate_profile.calibration_profile,
                        "calibration_disabled_outcomes": candidate_profile.calibration_disabled_outcomes,
                    },
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


def _parse_boolish(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    return None


def _has_previous_stage4_pass(history_csv: Path, *, require_strict: bool = True) -> bool:
    if not history_csv.exists():
        return False
    rows = list(csv.DictReader(history_csv.read_text(encoding="utf-8").splitlines()))
    if not rows:
        return False

    stage4_rows = [row for row in rows if str(row.get("stage")) == "4"]
    if len(stage4_rows) < 2:
        return False
    previous_row = stage4_rows[-2]
    previous_pass = _parse_boolish(previous_row.get("passed")) is True
    if not previous_pass:
        return False
    if not require_strict:
        return True
    return _parse_boolish(previous_row.get("strict_stage4_gate")) is True


def _append_history(
    history_csv: Path,
    *,
    run_id: str,
    stage: int,
    passed: bool,
    champion_model_version: str | None,
    strict_stage4_gate: bool | None = None,
    stage4_min_admission_rows: int | None = None,
) -> None:
    history_csv.parent.mkdir(parents=True, exist_ok=True)
    exists = history_csv.exists()
    row = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "passed": bool(passed),
        "champion_model_version": champion_model_version or "",
        "strict_stage4_gate": (
            ""
            if stage != 4 or strict_stage4_gate is None
            else bool(strict_stage4_gate)
        ),
        "stage4_min_admission_rows": (
            ""
            if stage != 4 or stage4_min_admission_rows is None
            else int(stage4_min_admission_rows)
        ),
    }
    with history_csv.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.max_rpm_total > 200:
        raise ValueError("max-rpm-total must be <= 200")

    stage4_min_admission_rows = getattr(args, "stage4_min_admission_rows", None)
    if stage4_min_admission_rows is not None and int(stage4_min_admission_rows) <= 0:
        raise ValueError("stage4-min-admission-rows must be > 0")

    run_id = f"causal-staged-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    output_root = Path(args.output_dir)
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    active_model_before = await _get_active_model_version()

    stages = [1, 2, 3, 4] if args.stage == "all" else [int(args.stage)]
    effective_stage_data_thresholds, overrides_applied = _resolve_stage_data_thresholds(
        stage4_min_admission_rows=(
            int(stage4_min_admission_rows)
            if stage4_min_admission_rows is not None and 4 in stages
            else None
        ),
    )
    summary: dict[str, Any] = {}
    gate_results: dict[str, Any] = {}

    for stage in stages:
        result = await _run_stage(
            stage=stage,
            run_id=run_id,
            candidates=args.train_candidates_per_stage,
            max_rpm_total=args.max_rpm_total,
            judge_concurrency=args.judge_concurrency,
            stage_data_thresholds=effective_stage_data_thresholds,
        )
        summary[f"stage_{stage}"] = result
        gate_results[f"stage_{stage}"] = {
            "passed": bool(result.get("passed")),
            "champion_model_version": (result.get("champion") or {}).get("model_version"),
            "data_gate_passed": bool(result.get("data_gate_passed")),
            "effective_data_thresholds": result.get("effective_data_thresholds"),
        }
        strict_stage4_gate_for_row = None
        if stage == 4:
            strict_stage4_gate_for_row = _is_strict_stage4_threshold(
                result.get("effective_data_thresholds")
            )
        _append_history(
            output_root / "history.csv",
            run_id=run_id,
            stage=stage,
            passed=bool(result.get("passed")),
            champion_model_version=(result.get("champion") or {}).get("model_version"),
            strict_stage4_gate=strict_stage4_gate_for_row,
            stage4_min_admission_rows=(
                int(stage4_min_admission_rows)
                if stage == 4 and stage4_min_admission_rows is not None
                else None
            ),
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
        current_stage4_cfg = summary.get("stage_4", {}).get("effective_data_thresholds")
        current_stage4_strict = _is_strict_stage4_threshold(current_stage4_cfg)
        previous_ok = _has_previous_stage4_pass(output_root / "history.csv", require_strict=True)
        if not current_stage4_strict:
            promotion["reasons"].append("stage4_current_pass_not_strict")
        elif not previous_ok:
            promotion["reasons"].append("stage4_needs_two_consecutive_strict_passes")
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
    active_restore_attempted = False
    active_restore_status = "not_requested"
    active_restored_to = None
    active_restore_result: dict[str, Any] | None = None
    if stages == [1]:
        active_restore_attempted = True
        if active_model_before:
            restore_result = await promote_model(model_version=active_model_before)
            active_restore_result = restore_result
            if restore_result.get("status") == "ok":
                restored_active_model = active_model_before
                active_restored_to = active_model_before
                active_restore_status = "ok"
            else:
                active_restore_status = "failed"
                promotion["reasons"].append("stage1_restore_active_failed")
        else:
            clear_result = await _clear_active_model()
            active_restore_result = clear_result
            if clear_result.get("status") == "ok":
                active_restore_status = "ok"
                active_restored_to = None
            else:
                active_restore_status = "failed"
                promotion["reasons"].append("stage1_clear_active_failed")
        active_model_after = await _get_active_model_version()
    elif not args.promote_on_final_pass and active_model_before and active_model_after != active_model_before:
        active_restore_attempted = True
        restore_result = await promote_model(model_version=active_model_before)
        active_restore_result = restore_result
        if restore_result.get("status") == "ok":
            restored_active_model = active_model_before
            active_restored_to = active_model_before
            active_restore_status = "ok"
            active_model_after = await _get_active_model_version()
        else:
            active_restore_status = "failed"
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
            "stage4_min_admission_rows": stage4_min_admission_rows,
        },
        "stage_summary": summary,
        "gate_results": gate_results,
        "effective_stage_data_thresholds": {
            str(stage): cfg for stage, cfg in sorted(effective_stage_data_thresholds.items())
        },
        "overrides_applied": overrides_applied,
        "promotion_decision": promotion,
        "active_model_before": active_model_before,
        "active_model_after": active_model_after,
        "restored_active_model": restored_active_model,
        "active_restore_attempted": active_restore_attempted,
        "active_restore_status": active_restore_status,
        "active_restored_to": active_restored_to,
        "active_restore_result": active_restore_result,
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
