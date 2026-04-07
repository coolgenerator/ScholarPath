"""Causal training utilities backed by registry tables."""

from __future__ import annotations

import statistics
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, func, select

from scholarpath.causal_engine.scoring import CausalFeatureView, compute_pywhy_raw_scores
from scholarpath.causal_engine.training_calibration import apply_calibration, fit_linear_calibration
from scholarpath.db.models import (
    CausalDatasetVersion,
    CausalFeatureSnapshot,
    CausalModelRegistry,
    CausalOutcomeEvent,
)
from scholarpath.db.session import async_session_factory

_OUTCOMES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]
_NON_ADMISSION_OUTCOMES = {
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
}


async def train_full_graph_model(
    *,
    dataset_version: str | None = None,
    profile: str = "high_quality",
    lookback_days: int = 540,
    min_rows_per_outcome: int = 200,
    calibration_enabled: bool = True,
    active_outcomes: list[str] | None = None,
    calibration_profile: str = "robust",
    calibration_disabled_outcomes: list[str] | None = None,
) -> dict[str, Any]:
    """Build a new pywhy model registry row from current training assets."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=max(lookback_days, 1))
    model_version = f"pywhy-{now:%Y%m%d-%H%M%S}-{uuid4().hex[:8]}"

    async with async_session_factory() as session:
        ds_row = None
        if dataset_version:
            ds_row = await session.scalar(
                select(CausalDatasetVersion).where(CausalDatasetVersion.version == dataset_version),
            )
            if active_outcomes is None and ds_row is not None:
                configured = ds_row.config_json.get("active_outcomes") if isinstance(ds_row.config_json, dict) else None
                if isinstance(configured, list):
                    active_outcomes = [str(item).strip() for item in configured if str(item).strip()]

        active = [
            outcome
            for outcome in (active_outcomes or _OUTCOMES)
            if outcome in _OUTCOMES
        ]
        if not active:
            active = ["admission_probability"]

        snapshot_count_stmt = (
            select(func.count())
            .select_from(CausalFeatureSnapshot)
            .where(CausalFeatureSnapshot.observed_at >= start)
        )
        snapshot_count = int((await session.scalar(snapshot_count_stmt)) or 0)

        snapshot_rows = (
            await session.execute(
                select(
                    CausalFeatureSnapshot.student_id,
                    CausalFeatureSnapshot.school_id,
                    CausalFeatureSnapshot.observed_at,
                    CausalFeatureSnapshot.feature_payload,
                ).where(CausalFeatureSnapshot.observed_at >= start)
            )
        ).all()

        outcome_rows = (
            (
                await session.execute(
                    select(
                        CausalOutcomeEvent.student_id,
                        CausalOutcomeEvent.school_id,
                        CausalOutcomeEvent.outcome_name,
                        CausalOutcomeEvent.label_type,
                        CausalOutcomeEvent.outcome_value,
                        CausalOutcomeEvent.observed_at,
                    ).where(CausalOutcomeEvent.observed_at >= start)
                )
            )
            .all()
        )

        by_outcome: dict[str, list[float]] = defaultdict(list)
        by_outcome_true: dict[str, list[float]] = defaultdict(list)
        label_type_counts: dict[str, int] = defaultdict(int)
        for _, _, outcome_name, label_type, value, _ in outcome_rows:
            name = str(outcome_name or "").strip()
            if name not in active:
                continue
            by_outcome[name].append(float(value))
            if str(label_type).strip().lower() == "true":
                by_outcome_true[name].append(float(value))
            label_type_counts[str(label_type)] += 1

        missing = [outcome for outcome in active if len(by_outcome[outcome]) < min_rows_per_outcome]
        if missing:
            return {
                "status": "failed_precondition",
                "missing_outcomes": missing,
                "snapshot_count": snapshot_count,
                "outcome_counts": {k: len(v) for k, v in by_outcome.items()},
            }

        metrics: dict[str, Any] = {
            "profile": profile,
            "raw_formula_version": "v2",
            "dataset_version": dataset_version,
            "active_outcomes": active,
            "snapshot_count": snapshot_count,
            "outcome_counts": {k: len(by_outcome[k]) for k in active},
            "true_counts_by_outcome": {k: len(by_outcome_true[k]) for k in active},
            "label_type_counts": dict(label_type_counts),
            "calibration_enabled": bool(calibration_enabled),
            "calibration_profile": str(calibration_profile or "robust"),
            "calibration_disabled_outcomes": sorted(
                {
                    str(item).strip()
                    for item in (calibration_disabled_outcomes or [])
                    if str(item).strip() in active
                }
            ),
            "mean_outcome_values": {
                k: round(float(statistics.fmean(by_outcome[k])), 6)
                for k in active
            },
            "trained_at": now.isoformat(),
        }

        if calibration_enabled:
            robust_guard = str(calibration_profile or "robust").strip().lower() != "legacy"
            calibration, diagnostics = _fit_calibration_from_alignment(
                snapshot_rows=snapshot_rows,
                outcome_rows=outcome_rows,
                active_outcomes=active,
                calibration_disabled_outcomes=metrics["calibration_disabled_outcomes"],
                robust_non_admission_guard=robust_guard,
            )
            metrics["calibration"] = calibration
            metrics["calibration_diagnostics"] = diagnostics

        graph_json = {
            "nodes": active,
            "edges": _graph_edges_for_outcomes(active),
        }

        row = CausalModelRegistry(
            model_name="pywhy",
            model_version=model_version,
            status="trained",
            engine_type="pywhy",
            discovery_method="ges+pc",
            estimator_method="econml",
            artifact_uri=f"registry://{model_version}",
            graph_json=graph_json,
            metrics_json=metrics,
            refuter_json={"status": "not_run"},
            training_window_start=start,
            training_window_end=now,
            is_active=False,
        )
        session.add(row)
        await session.flush()

        if ds_row is not None:
            ds_stats = dict(ds_row.stats_json or {})
            ds_stats["last_trained_model_version"] = model_version
            ds_row.stats_json = ds_stats

        await session.commit()

        return {
            "status": "ok",
            "model_version": model_version,
            "metrics": metrics,
            "snapshot_count": snapshot_count,
            "outcome_counts": metrics["outcome_counts"],
        }


async def promote_model(*, model_version: str) -> dict[str, Any]:
    """Mark one model active and demote others."""
    async with async_session_factory() as session:
        target = await session.scalar(
            select(CausalModelRegistry).where(CausalModelRegistry.model_version == model_version),
        )
        if target is None:
            return {"status": "not_found", "model_version": model_version}

        rows = (
            await session.execute(
                select(CausalModelRegistry).where(
                    and_(
                        CausalModelRegistry.model_name == target.model_name,
                        CausalModelRegistry.engine_type == target.engine_type,
                    )
                )
            )
        ).scalars().all()

        for row in rows:
            row.is_active = row.model_version == model_version
            if row.is_active:
                row.status = "active"

        await session.commit()
        return {"status": "ok", "model_version": model_version}


def _fit_calibration_from_alignment(
    *,
    snapshot_rows: list[tuple[Any, Any, datetime, dict[str, Any]]],
    outcome_rows: list[tuple[Any, Any, str, str, float, datetime]],
    active_outcomes: list[str] | None = None,
    calibration_disabled_outcomes: list[str] | None = None,
    robust_non_admission_guard: bool = True,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_pair: dict[tuple[str, str], list[tuple[float, dict[str, Any]]]] = defaultdict(list)
    for student_id, school_id, observed_at, feature_payload in snapshot_rows:
        if student_id is None or school_id is None or observed_at is None:
            continue
        key = (str(student_id), str(school_id))
        by_pair[key].append((observed_at.timestamp(), feature_payload or {}))

    for key in by_pair:
        by_pair[key].sort(key=lambda row: row[0])

    y_pred: dict[str, list[float]] = defaultdict(list)
    y_true: dict[str, list[float]] = defaultdict(list)
    active = [outcome for outcome in (active_outcomes or _OUTCOMES) if outcome in _OUTCOMES]
    if not active:
        active = ["admission_probability"]
    forced_disabled = {
        str(item).strip()
        for item in (calibration_disabled_outcomes or [])
        if str(item).strip() in active
    }

    for student_id, school_id, outcome_name, _, value, observed_at in outcome_rows:
        name = str(outcome_name or "").strip()
        if name not in active:
            continue
        if student_id is None or school_id is None or observed_at is None:
            continue
        key = (str(student_id), str(school_id))
        snapshots = by_pair.get(key)
        if not snapshots:
            continue
        payload = _nearest_snapshot_payload(snapshots, observed_at.timestamp())
        raw_scores = compute_pywhy_raw_scores(CausalFeatureView.from_payload(payload))
        pred = raw_scores.get(name)
        if pred is None:
            continue
        y_pred[name].append(float(pred))
        y_true[name].append(_clip01(float(value)))

    calibration: dict[str, dict[str, Any]] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    for outcome in active:
        preds = y_pred.get(outcome, [])
        truths = y_true.get(outcome, [])
        if not preds or len(preds) != len(truths):
            calibration[outcome] = {"method": "none"}
            diagnostics[outcome] = {
                "sample_count": 0,
                "mae_raw": 0.0,
                "mae_calibrated": 0.0,
                "spearman_raw": 0.0,
                "spearman_calibrated": 0.0,
                "direction_guard_applied": False,
                "fallback_reason": "insufficient_alignment_rows",
                "slope": 0.0,
                "std_raw": 0.0,
                "std_calibrated": 0.0,
                "std_ratio": 0.0,
                "mean_shift": 0.0,
                "guard_triggered": False,
                "guard_reasons": [],
            }
            continue

        mae_raw = _mae(preds, truths)
        sp_raw = _spearman_rank(preds, truths)
        if outcome in forced_disabled:
            calibration[outcome] = {"method": "none"}
            diagnostics[outcome] = {
                "sample_count": len(preds),
                "mae_raw": round(mae_raw, 6),
                "mae_calibrated": round(mae_raw, 6),
                "spearman_raw": round(sp_raw, 6),
                "spearman_calibrated": round(sp_raw, 6),
                "direction_guard_applied": False,
                "fallback_reason": "calibration_forced_disabled",
                "slope": 1.0,
                "std_raw": round(_stddev(preds), 6),
                "std_calibrated": round(_stddev(preds), 6),
                "std_ratio": 1.0,
                "mean_shift": 0.0,
                "guard_triggered": False,
                "guard_reasons": [],
            }
            continue

        cfg = fit_linear_calibration(y_pred=preds, y_true=truths)
        calibrated = [_clip01(apply_calibration(v, cfg)) for v in preds]
        mae_cali = _mae(calibrated, truths)
        sp_cali = _spearman_rank(calibrated, truths)

        fallback_reason = ""
        direction_guard = False

        if outcome == "admission_probability" and sp_cali <= 0.0:
            inv_cfg = fit_linear_calibration(y_pred=[1.0 - v for v in preds], y_true=truths)
            if str(inv_cfg.get("method") or "").lower() == "linear":
                inv_a = float(inv_cfg.get("a", 1.0))
                inv_b = float(inv_cfg.get("b", 0.0))
                cfg = {"method": "linear", "a": -inv_a, "b": inv_a + inv_b}
            else:
                cfg = {"method": "linear", "a": -1.0, "b": 1.0}
            calibrated = [_clip01(apply_calibration(v, cfg)) for v in preds]
            mae_cali = _mae(calibrated, truths)
            sp_cali = _spearman_rank(calibrated, truths)
            direction_guard = True
            fallback_reason = "admission_direction_guard"

        if outcome == "phd_probability" and mae_cali > mae_raw:
            mean_true = _clip01(sum(truths) / len(truths))
            candidates = [
                ("identity", {"method": "linear", "a": 1.0, "b": 0.0}),
                ("shrink_to_mean", {"method": "linear", "a": 0.5, "b": 0.5 * mean_true}),
                ("constant_mean", {"method": "linear", "a": 0.0, "b": mean_true}),
            ]
            best_name = ""
            best_cfg = cfg
            best_mae = mae_cali
            for name, candidate_cfg in candidates:
                candidate_pred = [_clip01(apply_calibration(v, candidate_cfg)) for v in preds]
                candidate_mae = _mae(candidate_pred, truths)
                if candidate_mae < best_mae:
                    best_name = name
                    best_cfg = candidate_cfg
                    best_mae = candidate_mae
            cfg = best_cfg
            calibrated = [_clip01(apply_calibration(v, cfg)) for v in preds]
            mae_cali = _mae(calibrated, truths)
            sp_cali = _spearman_rank(calibrated, truths)
            if best_name:
                fallback_reason = (
                    f"{fallback_reason};phd_mae_guard:{best_name}"
                    if fallback_reason
                    else f"phd_mae_guard:{best_name}"
                )

        slope = float(cfg.get("a", 1.0)) if str(cfg.get("method") or "").lower() == "linear" else 1.0
        std_raw = _stddev(preds)
        std_cal = _stddev(calibrated)
        std_ratio = std_cal / max(std_raw, 1e-12)
        mean_shift = _mean(calibrated) - _mean(preds)
        guard_reasons: list[str] = []
        if robust_non_admission_guard and outcome in _NON_ADMISSION_OUTCOMES:
            if sp_raw < 0.15:
                guard_reasons.append("spearman_raw<0.15")
            if abs(slope) < 0.25 or abs(slope) > 1.75:
                guard_reasons.append("|slope|_outside_[0.25,1.75]")
            if std_ratio < 0.45:
                guard_reasons.append("std_ratio<0.45")
        guard_triggered = bool(guard_reasons)
        if guard_triggered:
            cfg = {"method": "none"}
            calibrated = [_clip01(v) for v in preds]
            mae_cali = _mae(calibrated, truths)
            sp_cali = _spearman_rank(calibrated, truths)
            slope = 1.0
            std_cal = _stddev(calibrated)
            std_ratio = std_cal / max(std_raw, 1e-12)
            mean_shift = _mean(calibrated) - _mean(preds)
            fallback_reason = (
                f"{fallback_reason};non_admission_guard"
                if fallback_reason
                else "non_admission_guard"
            )

        calibration[outcome] = cfg
        diagnostics[outcome] = {
            "sample_count": len(preds),
            "mae_raw": round(mae_raw, 6),
            "mae_calibrated": round(mae_cali, 6),
            "spearman_raw": round(sp_raw, 6),
            "spearman_calibrated": round(sp_cali, 6),
            "direction_guard_applied": direction_guard,
            "fallback_reason": fallback_reason or None,
            "slope": round(slope, 6),
            "std_raw": round(std_raw, 6),
            "std_calibrated": round(std_cal, 6),
            "std_ratio": round(std_ratio, 6),
            "mean_shift": round(mean_shift, 6),
            "guard_triggered": guard_triggered,
            "guard_reasons": guard_reasons,
        }

    return calibration, diagnostics


def _graph_edges_for_outcomes(active_outcomes: list[str]) -> list[list[str]]:
    active = set(active_outcomes)
    edges: list[list[str]] = []
    full_edges = [
        ["admission_probability", "academic_outcome"],
        ["academic_outcome", "career_outcome"],
        ["career_outcome", "life_satisfaction"],
        ["academic_outcome", "phd_probability"],
    ]
    for parent, child in full_edges:
        if parent in active and child in active:
            edges.append([parent, child])
    return edges


def _nearest_snapshot_payload(
    snapshots: list[tuple[float, dict[str, Any]]],
    ts: float,
) -> dict[str, Any]:
    if not snapshots:
        return {}
    times = [row[0] for row in snapshots]
    pos = bisect_left(times, ts)
    if pos <= 0:
        return snapshots[0][1]
    if pos >= len(times):
        return snapshots[-1][1]
    prev_row = snapshots[pos - 1]
    next_row = snapshots[pos]
    if abs(prev_row[0] - ts) <= abs(next_row[0] - ts):
        return prev_row[1]
    return next_row[1]


def _mae(pred: list[float], truth: list[float]) -> float:
    if not pred or not truth:
        return 0.0
    n = min(len(pred), len(truth))
    return sum(abs(pred[i] - truth[i]) for i in range(n)) / n


def _spearman_rank(x: list[float], y: list[float]) -> float:
    if len(x) != len(y) or len(x) < 2:
        return 0.0
    rx = _rank(x)
    ry = _rank(y)
    n = len(x)
    den = n * (n**2 - 1)
    if den == 0:
        return 0.0
    num = 6.0 * sum((rx[i] - ry[i]) ** 2 for i in range(n))
    return 1.0 - (num / den)


def _rank(values: list[float]) -> list[float]:
    ordered = sorted((value, idx) for idx, value in enumerate(values))
    ranks = [0.0] * len(values)
    i = 0
    while i < len(ordered):
        j = i
        while j + 1 < len(ordered) and ordered[j + 1][0] == ordered[i][0]:
            j += 1
        rank_value = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[ordered[k][1]] = rank_value
        i = j + 1
    return ranks


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_val = _mean(values)
    return (sum((value - mean_val) ** 2 for value in values) / len(values)) ** 0.5
