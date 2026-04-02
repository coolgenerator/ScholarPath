"""Stage-based causal training orchestrator (data -> train -> eval -> gate -> optional promote)."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
from sqlalchemy import and_, func, or_, select

from scholarpath.causal_engine.training import promote_model, train_full_graph_model
from scholarpath.config import settings
from scholarpath.db.models import (
    CausalFeatureSnapshot,
    CausalModelRegistry,
    CausalOutcomeEvent,
)
from scholarpath.db.session import async_session_factory
from scholarpath.evals.causal_gold_live import (
    DEFAULT_CAUSAL_GOLD_DATASET_PATH,
    CausalGoldEvalReport,
    load_causal_gold_dataset,
    run_causal_gold_eval,
)
from scholarpath.evals.causal_rollout_quality import (
    DEFAULT_CAUSAL_ROLLOUT_OUTPUT_DIR,
    RolloutQualityReport,
    run_causal_rollout_quality_gate,
)
from scholarpath.scripts.causal_activate_pywhy import (
    build_augmented_seed_cases,
    choose_seed_cases,
    ensure_seed_prerequisites,
    reset_causal_assets,
    seed_training_assets,
)

DEFAULT_OUTPUT_DIR = Path(".benchmarks/causal_staged")
DEFAULT_SHADOW_HISTORY_PATH = DEFAULT_CAUSAL_ROLLOUT_OUTPUT_DIR / "history.csv"
DEFAULT_STRONG_ANCHOR_CONFIDENCE = 0.85
_OUTCOMES = (
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
)
_AUX_OUTCOMES = ("academic_outcome", "career_outcome", "life_satisfaction", "phd_probability")


@dataclass(frozen=True)
class StageConfig:
    stage: int
    snapshots_target: int
    outcome_target: int
    admission_true_target: int
    other_anchor_target: int
    min_rows_per_outcome: int
    judge_min: float
    mae_margin: float
    field_pass_min: float | None
    fallback_max: float
    require_rate_limit_zero: bool


@dataclass
class CandidateSummary:
    candidate_id: str
    model_version: str
    random_seed: int
    train_metrics: dict[str, Any]
    gate_passed: bool
    gate_reasons: list[str]
    score: float
    eval_metrics: dict[str, Any]
    artifacts: dict[str, Any]


@dataclass
class StageSummary:
    stage: int
    status: str
    config: dict[str, Any]
    coverage_before: dict[str, Any]
    coverage_after: dict[str, Any]
    coverage_ok: bool
    coverage_reasons: list[str]
    seeded: dict[str, Any]
    candidate_count: int
    candidates: list[CandidateSummary]
    champion_model_version: str | None
    champion_score: float | None


@dataclass
class PromotionDecision:
    attempted: bool
    promoted: bool
    model_version: str | None
    reasons: list[str]


STAGE_CONFIGS: dict[int, StageConfig] = {
    1: StageConfig(
        stage=1,
        snapshots_target=3000,
        outcome_target=3000,
        admission_true_target=400,
        other_anchor_target=150,
        min_rows_per_outcome=200,
        judge_min=65.0,
        mae_margin=0.01,
        field_pass_min=None,
        fallback_max=0.05,
        require_rate_limit_zero=False,
    ),
    2: StageConfig(
        stage=2,
        snapshots_target=7000,
        outcome_target=7000,
        admission_true_target=1200,
        other_anchor_target=400,
        min_rows_per_outcome=400,
        judge_min=72.0,
        mae_margin=0.0,
        field_pass_min=0.50,
        fallback_max=0.03,
        require_rate_limit_zero=False,
    ),
    3: StageConfig(
        stage=3,
        snapshots_target=12000,
        outcome_target=12000,
        admission_true_target=2200,
        other_anchor_target=800,
        min_rows_per_outcome=800,
        judge_min=78.0,
        mae_margin=-0.01,
        field_pass_min=0.58,
        fallback_max=0.02,
        require_rate_limit_zero=False,
    ),
    4: StageConfig(
        stage=4,
        snapshots_target=15000,
        outcome_target=15000,
        admission_true_target=3000,
        other_anchor_target=1000,
        min_rows_per_outcome=1000,
        judge_min=80.0,
        mae_margin=0.0,
        field_pass_min=0.60,
        fallback_max=0.02,
        require_rate_limit_zero=True,
    ),
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _stage_sequence(raw_stage: str) -> list[int]:
    stage = str(raw_stage or "all").strip().lower()
    if stage == "all":
        return [1, 2, 3, 4]
    number = int(stage)
    if number not in STAGE_CONFIGS:
        raise ValueError("stage must be one of 1,2,3,4,all")
    return [number]


async def _collect_coverage() -> dict[str, Any]:
    async with async_session_factory() as session:
        snapshots = int(
            (
                await session.execute(
                    select(func.count()).select_from(CausalFeatureSnapshot),
                )
            ).scalar_one()
            or 0
        )
        outcome_rows = (
            await session.execute(
                select(
                    CausalOutcomeEvent.outcome_name,
                    func.count(),
                ).group_by(CausalOutcomeEvent.outcome_name),
            )
        ).all()
        outcomes_by_outcome = {str(name): int(count) for name, count in outcome_rows}

        admission_true = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(CausalOutcomeEvent)
                    .where(
                        CausalOutcomeEvent.outcome_name == "admission_probability",
                        CausalOutcomeEvent.label_type == "true",
                    ),
                )
            ).scalar_one()
            or 0
        )

        strong_anchor_rows = (
            await session.execute(
                select(
                    CausalOutcomeEvent.outcome_name,
                    func.count(),
                )
                .where(
                    CausalOutcomeEvent.outcome_name.in_(_AUX_OUTCOMES),
                    or_(
                        CausalOutcomeEvent.label_type == "true",
                        and_(
                            CausalOutcomeEvent.label_type == "proxy",
                            CausalOutcomeEvent.label_confidence >= DEFAULT_STRONG_ANCHOR_CONFIDENCE,
                        ),
                    ),
                )
                .group_by(CausalOutcomeEvent.outcome_name),
            )
        ).all()
        anchors_by_outcome = {str(name): int(count) for name, count in strong_anchor_rows}

    return {
        "snapshots": snapshots,
        "outcomes_by_outcome": outcomes_by_outcome,
        "admission_true": admission_true,
        "anchors_by_outcome": anchors_by_outcome,
    }


def _evaluate_coverage_against_stage(
    *,
    coverage: dict[str, Any],
    config: StageConfig,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    snapshots = int(coverage.get("snapshots", 0) or 0)
    if snapshots < config.snapshots_target:
        reasons.append(f"snapshots={snapshots} < target={config.snapshots_target}")

    outcome_counts = coverage.get("outcomes_by_outcome") or {}
    for outcome in _OUTCOMES:
        count = int(outcome_counts.get(outcome, 0) or 0)
        if count < config.outcome_target:
            reasons.append(f"{outcome}={count} < target={config.outcome_target}")

    admission_true = int(coverage.get("admission_true", 0) or 0)
    if admission_true < config.admission_true_target:
        reasons.append(
            f"admission_true={admission_true} < target={config.admission_true_target}"
        )

    anchors = coverage.get("anchors_by_outcome") or {}
    for outcome in _AUX_OUTCOMES:
        count = int(anchors.get(outcome, 0) or 0)
        if count < config.other_anchor_target:
            reasons.append(f"{outcome}_anchors={count} < target={config.other_anchor_target}")

    return len(reasons) == 0, reasons


def _estimate_required_case_rows(
    *,
    coverage: dict[str, Any],
    config: StageConfig,
) -> tuple[int, int]:
    snapshots_need = max(0, config.snapshots_target - int(coverage.get("snapshots", 0) or 0))
    outcome_counts = coverage.get("outcomes_by_outcome") or {}
    outcome_need = max(
        0,
        max(
            config.outcome_target - int(outcome_counts.get(name, 0) or 0)
            for name in _OUTCOMES
        ),
    )
    anchor_counts = coverage.get("anchors_by_outcome") or {}
    anchor_need = max(
        0,
        max(
            config.other_anchor_target - int(anchor_counts.get(name, 0) or 0)
            for name in _AUX_OUTCOMES
        ),
    )
    total_needed = max(snapshots_need, outcome_need, anchor_need)
    true_needed = max(
        0,
        config.admission_true_target - int(coverage.get("admission_true", 0) or 0),
    )
    return total_needed, true_needed


def _clone_case(
    case: Any,
    *,
    suffix: str,
    force_label_type: str | None = None,
    context_suffix: str | None = None,
) -> Any:
    context = str(case.context)
    if context_suffix:
        context = f"{context}:{context_suffix}"
    return type(case)(
        case_id=f"{case.case_id}-{suffix}",
        cohort=case.cohort,
        context=context,
        student_features=dict(case.student_features),
        school_features=dict(case.school_features),
        offer_features=dict(case.offer_features),
        gold_outcomes=dict(case.gold_outcomes),
        gold_tolerance=dict(case.gold_tolerance),
        label_type=str(force_label_type or case.label_type),
        intervention_checks=list(case.intervention_checks),
    )


def _build_stage_seed_cases(
    *,
    selected_cases: list[Any],
    total_cases_needed: int,
    true_cases_needed: int,
    nonce: str,
) -> list[Any]:
    if total_cases_needed <= 0 and true_cases_needed <= 0:
        return []
    true_pool = [case for case in selected_cases if str(case.label_type).lower() == "true"]
    if true_cases_needed > 0 and not true_pool:
        raise ValueError("No true-labeled seed cases available to satisfy admission true target")

    out: list[Any] = []
    for idx in range(true_cases_needed):
        base = true_pool[idx % len(true_pool)]
        out.append(
            _clone_case(
                base,
                suffix=f"{nonce}-t{idx + 1}",
                force_label_type="true",
                context_suffix="stage_true",
            )
        )

    remaining = max(0, total_cases_needed - len(out))
    if remaining > 0:
        multiplier = max(1, math.ceil(remaining / max(1, len(selected_cases))))
        augmented = build_augmented_seed_cases(
            seed_cases=selected_cases,
            synthetic_multiplier=multiplier,
            rng_seed=42 + remaining,
        )
        synthetic_pool = [
            case
            for case in augmented
            if "-syn-" in str(case.case_id) or ":synthetic" in str(case.context)
        ]
        if not synthetic_pool:
            synthetic_pool = list(selected_cases)

        for idx in range(remaining):
            base = synthetic_pool[idx % len(synthetic_pool)]
            out.append(
                _clone_case(
                    base,
                    suffix=f"{nonce}-p{idx + 1}",
                    force_label_type="proxy",
                    context_suffix="stage_proxy",
                )
            )
    return out


async def _prepare_stage_assets(
    *,
    config: StageConfig,
    student_id: str | None,
    seed_cases_count: int,
    strong_proxy_confidence: float = 0.90,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], bool, list[str]]:
    dataset = load_causal_gold_dataset(DEFAULT_CAUSAL_GOLD_DATASET_PATH)
    selected = choose_seed_cases(dataset_cases=dataset.cases, seed_cases=seed_cases_count)
    precheck = await ensure_seed_prerequisites(
        student_id=student_id,
        seed_cases=len(selected),
    )
    coverage_before = await _collect_coverage()
    total_needed, true_needed = _estimate_required_case_rows(
        coverage=coverage_before,
        config=config,
    )
    nonce = f"s{config.stage}-{uuid.uuid4().hex[:8]}"
    stage_seed_cases = _build_stage_seed_cases(
        selected_cases=selected,
        total_cases_needed=total_needed,
        true_cases_needed=true_needed,
        nonce=nonce,
    )
    seeded = {
        "requested_total_cases": total_needed,
        "requested_true_cases": true_needed,
        "actual_seed_cases": len(stage_seed_cases),
        "snapshots": 0,
        "outcomes": 0,
    }
    if stage_seed_cases:
        seeded_counts = await seed_training_assets(
            student_id=precheck.student_id,
            school_ids=precheck.school_ids,
            seed_cases=stage_seed_cases,
            synthetic_proxy_label_confidence=strong_proxy_confidence,
            real_proxy_label_confidence=strong_proxy_confidence,
        )
        seeded.update(seeded_counts)

    coverage_after = await _collect_coverage()
    coverage_ok, coverage_reasons = _evaluate_coverage_against_stage(
        coverage=coverage_after,
        config=config,
    )
    return coverage_before, coverage_after, seeded, coverage_ok, coverage_reasons


async def _train_candidate_model(
    *,
    model_version: str,
    random_seed: int,
    min_rows_per_outcome: int,
) -> dict[str, Any]:
    np.random.seed(int(random_seed))
    async with async_session_factory() as session:
        result = await train_full_graph_model(
            session,
            model_version=model_version,
            profile="high_quality",
            bootstrap_iters=300,
            stability_threshold=0.75,
            lookback_days=540,
            bootstrap_parallelism=4,
            checkpoint_interval=25,
            resume_from_checkpoint=False,
            early_stop_patience=40,
            discovery_sample_rows=500,
            discovery_max_features=12,
            min_rows_per_outcome=min_rows_per_outcome,
            calibration_enabled=True,
            warning_mode="count_silent",
        )
        await session.commit()
    return {
        "model_version": result.model_version,
        "metrics": result.metrics,
        "artifact_uri": result.artifact_uri,
    }


def _evaluate_candidate_gate(
    *,
    config: StageConfig,
    eval_metrics: dict[str, Any],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    judge = _safe_float(eval_metrics.get("judge_overall_score"))
    if judge < config.judge_min:
        reasons.append(f"judge_overall_score={judge:.4f} < {config.judge_min:.4f}")

    pywhy_case_count = int(_safe_float(eval_metrics.get("pywhy_case_count"), 0.0))
    pywhy_status = str(eval_metrics.get("pywhy_status", "")).strip().lower()
    if pywhy_case_count <= 0:
        reasons.append("pywhy_case_count <= 0")
    if pywhy_status != "ok":
        reasons.append(f"pywhy_status={pywhy_status or 'unknown'} != ok")

    mae_legacy = _safe_float(eval_metrics.get("mae_overall_legacy"))
    mae_pywhy = _safe_float(eval_metrics.get("mae_overall_pywhy"))
    if mae_pywhy > mae_legacy + config.mae_margin:
        reasons.append(
            f"mae_overall_pywhy={mae_pywhy:.6f} > mae_overall_legacy+margin={mae_legacy + config.mae_margin:.6f}"
        )

    rollout_passed = bool(eval_metrics.get("rollout_passed", False))
    if not rollout_passed:
        reasons.append("rollout_gate not passed")

    fallback = _safe_float(eval_metrics.get("fallback_rate_pywhy"))
    if fallback > config.fallback_max:
        reasons.append(f"fallback_rate_pywhy={fallback:.6f} > {config.fallback_max:.6f}")

    if config.field_pass_min is not None:
        field_pass = _safe_float(eval_metrics.get("judge_field_pass_rate_pywhy"))
        if field_pass < config.field_pass_min:
            reasons.append(f"judge_field_pass_rate_pywhy={field_pass:.6f} < {config.field_pass_min:.6f}")

    if config.require_rate_limit_zero:
        rate_limit_errors = int(_safe_float(eval_metrics.get("rate_limit_error_count"), 0.0))
        if rate_limit_errors != 0:
            reasons.append(f"rate_limit_error_count={rate_limit_errors} != 0")

    return len(reasons) == 0, reasons


def _compute_candidate_score(*, eval_metrics: dict[str, Any]) -> float:
    judge = max(0.0, min(100.0, _safe_float(eval_metrics.get("judge_overall_score")))) / 100.0
    pywhy_mae = max(0.0, min(1.0, _safe_float(eval_metrics.get("mae_overall_pywhy"))))
    field_pass = max(0.0, min(1.0, _safe_float(eval_metrics.get("judge_field_pass_rate_pywhy"))))
    fallback = max(0.0, min(1.0, _safe_float(eval_metrics.get("fallback_rate_pywhy"))))
    return (
        0.35 * judge
        + 0.30 * (1.0 - pywhy_mae)
        + 0.20 * field_pass
        + 0.15 * (1.0 - fallback)
    )


async def _annotate_candidate(
    *,
    model_version: str,
    stage: int,
    candidate_id: str,
    gate_passed: bool,
    gate_reasons: list[str],
    score: float,
    is_champion: bool,
) -> None:
    async with async_session_factory() as session:
        row = (
            await session.execute(
                select(CausalModelRegistry).where(CausalModelRegistry.model_version == model_version),
            )
        ).scalars().first()
        if row is None:
            return
        metrics = row.metrics_json if isinstance(row.metrics_json, dict) else {}
        metrics["staged_training"] = {
            "stage": stage,
            "candidate_id": candidate_id,
            "gate_passed": gate_passed,
            "gate_reasons": list(gate_reasons),
            "score": round(float(score), 6),
            "is_stage_champion": bool(is_champion),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        row.metrics_json = metrics
        row.status = "trained"
        await session.commit()


async def _run_candidate_eval(
    *,
    stage: int,
    candidate_id: str,
    model_version: str,
    output_dir: Path,
    max_rpm_total: int,
    judge_concurrency: int,
    engine_case_concurrency: int,
) -> tuple[CausalGoldEvalReport, RolloutQualityReport]:
    candidate_dir = output_dir / f"candidate_{candidate_id}"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    rollout_sample_schools = 20
    rollout_contexts = 2
    rollout_min_rows = max(40, rollout_sample_schools * rollout_contexts)
    gold = await run_causal_gold_eval(
        dataset_path=DEFAULT_CAUSAL_GOLD_DATASET_PATH,
        output_dir=candidate_dir / "gold_eval",
        judge_enabled=True,
        judge_concurrency=judge_concurrency,
        judge_temperature=0.1,
        judge_max_tokens=1200,
        max_rpm_total=max_rpm_total,
        engine_case_concurrency=engine_case_concurrency,
        warning_mode="count_silent",
        sample_size=40,
        sample_strategy="full",
        pywhy_model_version_hint=model_version,
    )
    rollout = await run_causal_rollout_quality_gate(
        target_percent=100,
        sample_schools=rollout_sample_schools,
        contexts=rollout_contexts,
        output_dir=candidate_dir / "rollout_eval",
        min_rows=rollout_min_rows,
        history_window_runs=24,
        emit_alert=True,
        pywhy_model_version_hint=model_version,
    )
    return gold, rollout


def _history_path(output_dir: Path) -> Path:
    return output_dir / "history.csv"


def _append_stage_history(
    *,
    output_root: Path,
    run_id: str,
    stage_summary: StageSummary,
) -> None:
    row = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "stage": str(stage_summary.stage),
        "status": stage_summary.status,
        "passed": str(stage_summary.status == "passed").lower(),
        "candidate_count": str(stage_summary.candidate_count),
        "champion_model_version": str(stage_summary.champion_model_version or ""),
        "champion_score": (
            f"{float(stage_summary.champion_score):.6f}"
            if stage_summary.champion_score is not None
            else ""
        ),
    }
    path = _history_path(output_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        return list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
    except Exception:
        return []


def _has_two_consecutive_stage4_passes(history_rows: list[dict[str, str]]) -> bool:
    stage4 = [row for row in history_rows if str(row.get("stage")) == "4"]
    if len(stage4) < 2:
        return False
    return (
        str(stage4[-1].get("passed", "")).strip().lower() == "true"
        and str(stage4[-2].get("passed", "")).strip().lower() == "true"
    )


def _is_shadow_window_clean(
    *,
    hours: int,
    history_path: Path,
    target_percent: int,
    min_rows: int,
) -> tuple[bool, list[str]]:
    rows = _read_csv_rows(history_path)
    reasons: list[str] = []
    if not rows:
        return False, [f"no rollout history rows available: {history_path}"]

    now = datetime.now(UTC)
    cutoff = now - timedelta(hours=max(1, int(hours)))
    window_rows: list[dict[str, str]] = []
    normalized_target = max(0, min(100, int(target_percent)))
    for row in rows:
        raw_ts = str(row.get("generated_at", "")).strip()
        if not raw_ts:
            continue
        try:
            ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        row_target_percent = _safe_int(row.get("target_percent"), -1)
        if ts >= cutoff and row_target_percent == normalized_target:
            window_rows.append(row)
    if not window_rows:
        return False, [
            f"no rollout rows in last {hours}h for target_percent={normalized_target}",
        ]

    if len(window_rows) < max(1, int(min_rows)):
        return False, [
            f"rollout rows in window {len(window_rows)} < shadow_min_rows {max(1, int(min_rows))}",
        ]

    for row in window_rows:
        if str(row.get("passed", "")).strip().lower() != "true":
            reasons.append(f"rollout row not passed: run_id={row.get('run_id')}")
        alerts_count = _safe_int(row.get("alerts_count"), 0)
        if alerts_count > 0:
            reasons.append(f"rollout alerts_count={alerts_count} for run_id={row.get('run_id')}")

    return len(reasons) == 0, reasons


async def _promote_if_ready(
    *,
    output_root: Path,
    champion_model_version: str | None,
    promote_on_final_pass: bool,
    require_shadow_window_hours: int,
    shadow_history_path: Path,
    shadow_target_percent: int,
    shadow_min_rows: int,
    shadow_refresh_before_promote: bool,
) -> PromotionDecision:
    if not promote_on_final_pass:
        return PromotionDecision(
            attempted=False,
            promoted=False,
            model_version=champion_model_version,
            reasons=["promote_on_final_pass disabled"],
        )
    if not champion_model_version:
        return PromotionDecision(
            attempted=False,
            promoted=False,
            model_version=None,
            reasons=["missing stage4 champion model version"],
        )

    history_rows = _read_csv_rows(_history_path(output_root))
    if not _has_two_consecutive_stage4_passes(history_rows):
        return PromotionDecision(
            attempted=True,
            promoted=False,
            model_version=champion_model_version,
            reasons=["stage4 consecutive pass count < 2"],
        )

    if shadow_refresh_before_promote:
        refresh_output_dir = shadow_history_path.parent
        try:
            await run_causal_rollout_quality_gate(
                target_percent=shadow_target_percent,
                output_dir=refresh_output_dir,
            )
        except Exception as exc:
            return PromotionDecision(
                attempted=True,
                promoted=False,
                model_version=champion_model_version,
                reasons=[f"shadow refresh failed: {exc}"],
            )

    shadow_ok, shadow_reasons = _is_shadow_window_clean(
        hours=require_shadow_window_hours,
        history_path=shadow_history_path,
        target_percent=shadow_target_percent,
        min_rows=shadow_min_rows,
    )
    if not shadow_ok:
        return PromotionDecision(
            attempted=True,
            promoted=False,
            model_version=champion_model_version,
            reasons=shadow_reasons,
        )

    async with async_session_factory() as session:
        await promote_model(session, model_version=champion_model_version)
        await session.commit()
    return PromotionDecision(
        attempted=True,
        promoted=True,
        model_version=champion_model_version,
        reasons=[],
    )


async def run_staged_training(args: argparse.Namespace) -> dict[str, Any]:
    stage_ids = _stage_sequence(args.stage)
    if int(args.train_candidates_per_stage) <= 0:
        raise ValueError("train-candidates-per-stage must be > 0")
    if int(args.max_rpm_total) <= 0 or int(args.max_rpm_total) > 200:
        raise ValueError("max-rpm-total must be in [1, 200]")
    if int(args.judge_concurrency) <= 0:
        raise ValueError("judge-concurrency must be > 0")
    if int(args.shadow_window_hours) <= 0:
        raise ValueError("shadow-window-hours must be > 0")
    if int(args.shadow_min_rows) <= 0:
        raise ValueError("shadow-min-rows must be > 0")
    if int(args.shadow_target_percent) < 0 or int(args.shadow_target_percent) > 100:
        raise ValueError("shadow-target-percent must be in [0, 100]")

    run_id = f"causal-staged-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    run_root = Path(args.output_dir) / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    if args.reset_causal_assets:
        await reset_causal_assets()

    stage_summaries: list[StageSummary] = []
    stage4_champion: str | None = None
    execution_status = "ok"

    for stage_id in stage_ids:
        config = STAGE_CONFIGS[stage_id]
        stage_dir = run_root / f"stage_{stage_id}"
        stage_dir.mkdir(parents=True, exist_ok=True)

        (
            coverage_before,
            coverage_after,
            seeded,
            coverage_ok,
            coverage_reasons,
        ) = await _prepare_stage_assets(
            config=config,
            student_id=args.student_id,
            seed_cases_count=args.seed_cases,
        )
        candidates: list[CandidateSummary] = []
        champion_model: str | None = None
        champion_score: float | None = None

        if coverage_ok:
            for candidate_index in range(1, int(args.train_candidates_per_stage) + 1):
                candidate_id = f"s{stage_id}c{candidate_index}"
                random_seed = 20260000 + stage_id * 100 + candidate_index
                model_version = (
                    f"pywhy-stage{stage_id}-cand{candidate_index}-"
                    f"{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
                )
                train_payload = await _train_candidate_model(
                    model_version=model_version,
                    random_seed=random_seed,
                    min_rows_per_outcome=config.min_rows_per_outcome,
                )
                gold_report, rollout_report = await _run_candidate_eval(
                    stage=stage_id,
                    candidate_id=candidate_id,
                    model_version=model_version,
                    output_dir=stage_dir,
                    max_rpm_total=int(args.max_rpm_total),
                    judge_concurrency=int(args.judge_concurrency),
                    engine_case_concurrency=int(args.engine_case_concurrency),
                )
                eval_metrics = dict(gold_report.metrics)
                eval_metrics["rollout_status"] = rollout_report.decision.status
                eval_metrics["rollout_passed"] = bool(rollout_report.decision.passed)
                eval_metrics["rollout_fallback_rate"] = rollout_report.metrics.fallback_rate
                pywhy_pass = gold_report.pywhy_pass
                eval_metrics["pywhy_status"] = (
                    str(pywhy_pass.status)
                    if pywhy_pass is not None
                    else "missing"
                )
                eval_metrics["pywhy_case_count"] = (
                    int(pywhy_pass.case_count)
                    if pywhy_pass is not None
                    else 0
                )
                gate_passed, gate_reasons = _evaluate_candidate_gate(
                    config=config,
                    eval_metrics=eval_metrics,
                )
                score = _compute_candidate_score(eval_metrics=eval_metrics) if gate_passed else 0.0

                summary = CandidateSummary(
                    candidate_id=candidate_id,
                    model_version=model_version,
                    random_seed=random_seed,
                    train_metrics=train_payload.get("metrics", {}),
                    gate_passed=gate_passed,
                    gate_reasons=gate_reasons,
                    score=round(score, 6),
                    eval_metrics=eval_metrics,
                    artifacts={
                        "gold_eval_run_id": gold_report.run_id,
                        "gold_eval_output_dir": gold_report.config.get("output_dir"),
                        "rollout_run_id": rollout_report.run_id,
                    },
                )
                candidates.append(summary)

            eligible = [candidate for candidate in candidates if candidate.gate_passed]
            if eligible:
                champion = max(eligible, key=lambda item: item.score)
                champion_model = champion.model_version
                champion_score = champion.score
                stage_status = "passed"
            else:
                stage_status = "failed"
                execution_status = "failed"

            for candidate in candidates:
                await _annotate_candidate(
                    model_version=candidate.model_version,
                    stage=stage_id,
                    candidate_id=candidate.candidate_id,
                    gate_passed=candidate.gate_passed,
                    gate_reasons=candidate.gate_reasons,
                    score=candidate.score,
                    is_champion=(candidate.model_version == champion_model),
                )
        else:
            stage_status = "failed_precondition"
            execution_status = "failed"

        stage_summary = StageSummary(
            stage=stage_id,
            status=stage_status,
            config=asdict(config),
            coverage_before=coverage_before,
            coverage_after=coverage_after,
            coverage_ok=coverage_ok,
            coverage_reasons=coverage_reasons,
            seeded=seeded,
            candidate_count=len(candidates),
            candidates=candidates,
            champion_model_version=champion_model,
            champion_score=champion_score,
        )
        stage_summaries.append(stage_summary)
        _append_stage_history(
            output_root=Path(args.output_dir),
            run_id=run_id,
            stage_summary=stage_summary,
        )
        (stage_dir / "stage_summary.json").write_text(
            json.dumps(asdict(stage_summary), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (stage_dir / "gate_results.json").write_text(
            json.dumps(
                {
                    "stage": stage_id,
                    "candidates": [
                        {
                            "candidate_id": candidate.candidate_id,
                            "model_version": candidate.model_version,
                            "gate_passed": candidate.gate_passed,
                            "gate_reasons": candidate.gate_reasons,
                            "score": candidate.score,
                        }
                        for candidate in candidates
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        if stage_id == 4 and stage_status == "passed":
            stage4_champion = champion_model

        if stage_status not in {"passed"}:
            break

    promotion = PromotionDecision(
        attempted=False,
        promoted=False,
        model_version=stage4_champion,
        reasons=["stage4 not executed or failed"],
    )
    if 4 in stage_ids and stage4_champion:
        promotion = await _promote_if_ready(
            output_root=Path(args.output_dir),
            champion_model_version=stage4_champion,
            promote_on_final_pass=bool(args.promote_on_final_pass),
            require_shadow_window_hours=int(args.shadow_window_hours),
            shadow_history_path=Path(args.shadow_history_path),
            shadow_target_percent=int(args.shadow_target_percent),
            shadow_min_rows=int(args.shadow_min_rows),
            shadow_refresh_before_promote=bool(args.shadow_refresh_before_promote),
        )

    payload = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": execution_status,
        "config": {
            "stage": args.stage,
            "train_candidates_per_stage": int(args.train_candidates_per_stage),
            "max_rpm_total": int(args.max_rpm_total),
            "judge_concurrency": int(args.judge_concurrency),
            "engine_case_concurrency": int(args.engine_case_concurrency),
            "promote_on_final_pass": bool(args.promote_on_final_pass),
            "seed_cases": int(args.seed_cases),
            "shadow_window_hours": int(args.shadow_window_hours),
            "shadow_history_path": str(Path(args.shadow_history_path)),
            "shadow_target_percent": int(args.shadow_target_percent),
            "shadow_min_rows": int(args.shadow_min_rows),
            "shadow_refresh_before_promote": bool(args.shadow_refresh_before_promote),
            "output_dir": str(run_root),
        },
        "stages": [asdict(item) for item in stage_summaries],
        "promotion_decision": asdict(promotion),
    }
    (run_root / "run_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_root / "promotion_decision.json").write_text(
        json.dumps(asdict(promotion), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run staged causal training and gated evaluation.",
    )
    parser.add_argument(
        "--stage",
        default="all",
        help="Stage selector: 1|2|3|4|all (default: all).",
    )
    parser.add_argument(
        "--train-candidates-per-stage",
        type=int,
        default=3,
        help="Candidate model count per stage (default: 3).",
    )
    parser.add_argument(
        "--max-rpm-total",
        type=int,
        default=200,
        help="Judge/eval total RPM cap (default: 200, hard <= 200).",
    )
    parser.add_argument(
        "--judge-concurrency",
        type=int,
        default=2,
        help="Judge concurrency for gold eval (default: 2).",
    )
    parser.add_argument(
        "--engine-case-concurrency",
        type=int,
        default=4,
        help="Engine case concurrency for causal gold eval (default: 4).",
    )
    parser.add_argument(
        "--promote-on-final-pass",
        dest="promote_on_final_pass",
        action="store_true",
        default=True,
        help="Enable final promote when Stage4 criteria are satisfied (default: enabled).",
    )
    parser.add_argument(
        "--no-promote-on-final-pass",
        dest="promote_on_final_pass",
        action="store_false",
        help="Disable final promote.",
    )
    parser.add_argument(
        "--shadow-window-hours",
        type=int,
        default=24,
        help="Shadow window hours required before final promotion (default: 24).",
    )
    parser.add_argument(
        "--shadow-history-path",
        default=str(DEFAULT_SHADOW_HISTORY_PATH),
        help="Rollout history CSV path used by promotion gate (default: .benchmarks/causal_rollout/history.csv).",
    )
    parser.add_argument(
        "--shadow-target-percent",
        type=int,
        default=max(0, min(100, int(settings.CAUSAL_PYWHY_PRIMARY_PERCENT))),
        help="Target percent filter for rollout rows in shadow window (default: CAUSAL_PYWHY_PRIMARY_PERCENT).",
    )
    parser.add_argument(
        "--shadow-min-rows",
        type=int,
        default=3,
        help="Minimum rollout rows required in shadow window (default: 3).",
    )
    parser.add_argument(
        "--shadow-refresh-before-promote",
        dest="shadow_refresh_before_promote",
        action="store_true",
        default=True,
        help="Refresh rollout gate once before promotion check (default: enabled).",
    )
    parser.add_argument(
        "--no-shadow-refresh-before-promote",
        dest="shadow_refresh_before_promote",
        action="store_false",
        help="Skip rollout gate refresh before promotion check.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output root for staged reports (default: .benchmarks/causal_staged).",
    )
    parser.add_argument(
        "--student-id",
        default=None,
        help="Optional student UUID used for asset seeding.",
    )
    parser.add_argument(
        "--seed-cases",
        type=int,
        default=40,
        help="Number of gold cases used as seed template (default: 40).",
    )
    parser.add_argument(
        "--reset-causal-assets",
        action="store_true",
        help="Reset causal_* assets before staged run.",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    result = asyncio.run(run_staged_training(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
