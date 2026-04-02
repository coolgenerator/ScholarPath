"""Offline training and model registry workflows for PyWhy causal engine."""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import networkx as nx
import numpy as np
import pandas as pd
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.causal.dag_builder import AdmissionDAGBuilder
from scholarpath.causal_engine.training_calibration import (
    fit_outcome_calibrators,
)
from scholarpath.causal_engine.warning_audit import (
    WarningAudit,
    capture_stage_warnings,
    normalize_warning_mode,
)
from scholarpath.db.models import (
    CausalFeatureSnapshot,
    CausalModelRegistry,
    CausalOutcomeEvent,
)

logger = logging.getLogger(__name__)

_DEFAULT_FORBIDDEN_EDGES = {
    ("life_satisfaction", "admission_probability"),
    ("career_outcome", "admission_probability"),
}
_OUTCOME_NAMES: tuple[str, ...] = (
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
)


@dataclass(slots=True)
class TrainingResult:
    model_version: str
    metrics: dict[str, Any]
    refuters: dict[str, Any]
    graph_json: dict[str, Any]
    artifact_uri: str | None


async def train_full_graph_model(
    session: AsyncSession,
    *,
    model_version: str | None = None,
    profile: str = "high_quality",
    bootstrap_iters: int = 100,
    stability_threshold: float = 0.7,
    lookback_days: int = 365,
    bootstrap_parallelism: int = 1,
    checkpoint_interval: int = 25,
    resume_from_checkpoint: bool = False,
    early_stop_patience: int = 0,
    discovery_sample_rows: int = 300,
    discovery_max_features: int = 12,
    min_rows_per_outcome: int = 200,
    calibration_enabled: bool = True,
    warning_mode: str = "count_silent",
) -> TrainingResult:
    resolved_warning_mode = normalize_warning_mode(warning_mode)
    resolved_profile = str(profile or "high_quality").strip().lower()
    (
        bootstrap_iters,
        stability_threshold,
        lookback_days,
        discovery_sample_rows,
    ) = _resolve_training_profile(
        profile=resolved_profile,
        bootstrap_iters=bootstrap_iters,
        stability_threshold=stability_threshold,
        lookback_days=lookback_days,
        discovery_sample_rows=discovery_sample_rows,
    )
    started = time.monotonic()
    frame = await _build_training_frame(session, lookback_days=lookback_days)
    if frame.empty:
        raise ValueError("No training data found in causal_feature_snapshots + causal_outcome_events")
    coverage = _ensure_outcome_coverage(
        frame=frame,
        min_rows_per_outcome=max(1, int(min_rows_per_outcome)),
    )

    now = datetime.now(UTC)
    resolved_model_version = model_version or f"pywhy-{now.strftime('%Y%m%d-%H%M%S')}"
    graph_json, discovery_metrics = _discover_graph_with_consensus(
        frame,
        model_version=resolved_model_version,
        bootstrap_iters=bootstrap_iters,
        stability_threshold=stability_threshold,
        bootstrap_parallelism=bootstrap_parallelism,
        checkpoint_interval=checkpoint_interval,
        resume_from_checkpoint=resume_from_checkpoint,
        early_stop_patience=early_stop_patience,
        discovery_sample_rows=discovery_sample_rows,
        discovery_max_features=discovery_max_features,
    )
    estimation_warning_audit = WarningAudit()
    estimation_metrics, refuters = _run_pywhy_estimations(
        frame,
        warning_audit=estimation_warning_audit,
        warning_mode=resolved_warning_mode,
    )
    calibration_payload = fit_outcome_calibrators(
        frame=frame,
        enabled=bool(calibration_enabled),
        warning_mode=resolved_warning_mode,
        warning_audit=estimation_warning_audit,
        outcome_names=_OUTCOME_NAMES,
    )
    refuter_summary = _summarize_refuters(refuters)

    real_rows = int((frame["label_type"] == "true").sum()) if "label_type" in frame else 0
    synthetic_rows = (
        int((frame["data_origin"] == "synthetic").sum())
        if "data_origin" in frame
        else 0
    )
    proxy_rows = int(len(frame) - real_rows)

    elapsed = time.monotonic() - started
    merged_metrics = {
        "row_count": int(len(frame)),
        "feature_count": int(
            len(
                [
                    c
                    for c in frame.columns
                    if c
                    not in {
                        "student_id",
                        "school_id",
                        "outcome_name",
                        "outcome_value",
                        "label_type",
                        "label_confidence",
                        "source",
                        "data_origin",
                        "observed_at",
                    }
                ]
            )
        ),
        "data_mix_ratio": {
            "real": round(real_rows / max(len(frame), 1), 4),
            "synthetic": round(synthetic_rows / max(len(frame), 1), 4),
            "proxy": round(proxy_rows / max(len(frame), 1), 4),
        },
        "profile": resolved_profile,
        "profile_effective_params": {
            "bootstrap_iters": int(bootstrap_iters),
            "stability_threshold": float(stability_threshold),
            "lookback_days": int(lookback_days),
            "discovery_sample_rows": int(discovery_sample_rows),
            "discovery_max_features": int(discovery_max_features),
            "bootstrap_parallelism": int(bootstrap_parallelism),
        },
        "outcome_coverage": coverage,
        "train_wall_time_sec": round(elapsed, 3),
        "discovery": discovery_metrics,
        "estimation": estimation_metrics,
        "refuter_summary": refuter_summary,
        "calibration": calibration_payload,
        "warning_mode": resolved_warning_mode,
        "warnings_total": int(estimation_warning_audit.total),
        "warnings_by_stage": dict(sorted(estimation_warning_audit.by_stage.items())),
        "warnings_by_family": dict(sorted(estimation_warning_audit.by_family.items())),
    }

    artifact_uri = _write_training_artifact(
        resolved_model_version,
        graph_json,
        merged_metrics,
        refuters,
    )

    model = CausalModelRegistry(
        model_name="pywhy_full_graph",
        model_version=resolved_model_version,
        status="trained",
        engine_type="pywhy",
        discovery_method="causal-learn:pc+ges",
        estimator_method="dowhy+econml",
        artifact_uri=artifact_uri,
        graph_json=graph_json,
        metrics_json=merged_metrics,
        refuter_json=refuters,
        training_window_start=frame["observed_at"].min().to_pydatetime() if "observed_at" in frame else None,
        training_window_end=frame["observed_at"].max().to_pydatetime() if "observed_at" in frame else None,
        is_active=False,
    )
    session.add(model)
    await session.flush()

    return TrainingResult(
        model_version=resolved_model_version,
        metrics=merged_metrics,
        refuters=refuters,
        graph_json=graph_json,
        artifact_uri=artifact_uri,
    )


async def promote_model(
    session: AsyncSession,
    *,
    model_version: str,
) -> dict[str, Any]:
    target_stmt = select(CausalModelRegistry).where(CausalModelRegistry.model_version == model_version)
    row = await session.execute(target_stmt)
    model = row.scalars().first()
    if model is None:
        raise ValueError(f"Model version '{model_version}' not found")

    await session.execute(
        update(CausalModelRegistry)
        .values(is_active=False)
        .where(CausalModelRegistry.is_active.is_(True))
    )
    model.is_active = True
    model.status = "active"
    await session.flush()

    return {
        "model_version": model.model_version,
        "status": model.status,
        "is_active": model.is_active,
    }


async def shadow_audit(
    session: AsyncSession,
    *,
    active_only: bool = True,
) -> dict[str, Any]:
    from scholarpath.db.models import CausalShadowComparison

    stmt = select(CausalShadowComparison)
    if active_only:
        cutoff = datetime.now(UTC) - timedelta(days=7)
        stmt = stmt.where(CausalShadowComparison.created_at >= cutoff)
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        return {
            "rows": 0,
            "mae": 0.0,
            "fallback_rate": 0.0,
        }

    abs_errors: list[float] = []
    fallback = 0
    for row in rows:
        for k, v in (row.diff_scores or {}).items():
            abs_errors.append(abs(float(v)))
        if row.fallback_used:
            fallback += 1
    mae = float(np.mean(abs_errors)) if abs_errors else 0.0

    return {
        "rows": len(rows),
        "mae": mae,
        "fallback_rate": fallback / len(rows),
    }


async def _build_training_frame(
    session: AsyncSession,
    *,
    lookback_days: int,
) -> pd.DataFrame:
    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
    snaps = (
        await session.execute(
            select(CausalFeatureSnapshot).where(CausalFeatureSnapshot.observed_at >= cutoff)
        )
    ).scalars().all()
    events = (
        await session.execute(
            select(CausalOutcomeEvent).where(CausalOutcomeEvent.observed_at >= cutoff)
        )
    ).scalars().all()

    if not snaps or not events:
        return pd.DataFrame()

    latest_event = {}
    for event in events:
        key = (str(event.student_id), str(event.school_id), event.outcome_name)
        prev = latest_event.get(key)
        if prev is None or event.observed_at > prev.observed_at:
            latest_event[key] = event

    rows: list[dict[str, Any]] = []
    for snap in snaps:
        payload = snap.feature_payload or {}
        features: dict[str, float] = {}
        features.update(payload.get("student_features", {}))
        features.update(payload.get("school_features", {}))
        features.update(payload.get("interaction_features", {}))
        features = {str(k): float(v) for k, v in features.items() if isinstance(v, (int, float))}

        for outcome_name in _OUTCOME_NAMES:
            event = latest_event.get((str(snap.student_id), str(snap.school_id), outcome_name))
            if event is None:
                continue
            row = dict(features)
            row["student_id"] = str(snap.student_id)
            row["school_id"] = str(snap.school_id)
            row["outcome_name"] = outcome_name
            row["outcome_value"] = float(event.outcome_value)
            row["label_type"] = event.label_type
            row["label_confidence"] = float(event.label_confidence)
            row["source"] = str(event.source or "")
            event_meta = event.metadata_ or {}
            if isinstance(event_meta, dict):
                row["data_origin"] = str(
                    event_meta.get("data_origin")
                    or ("synthetic" if "synthetic" in str(event.source or "").lower() else "real")
                )
            else:
                row["data_origin"] = "synthetic" if "synthetic" in str(event.source or "").lower() else "real"
            row["observed_at"] = snap.observed_at
            rows.append(row)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _resolve_training_profile(
    *,
    profile: str,
    bootstrap_iters: int,
    stability_threshold: float,
    lookback_days: int,
    discovery_sample_rows: int,
) -> tuple[int, float, int, int]:
    if profile == "high_quality":
        return 300, 0.75, 540, 500
    return (
        max(1, int(bootstrap_iters)),
        float(stability_threshold),
        max(1, int(lookback_days)),
        max(1, int(discovery_sample_rows)),
    )


def _ensure_outcome_coverage(
    *,
    frame: pd.DataFrame,
    min_rows_per_outcome: int,
) -> dict[str, Any]:
    per_outcome: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    threshold = max(1, int(min_rows_per_outcome))

    for outcome in _OUTCOME_NAMES:
        outcome_frame = frame[frame["outcome_name"] == outcome]
        row_count = int(len(outcome_frame))
        label_type_ratio: dict[str, float] = {}
        if row_count > 0 and "label_type" in outcome_frame.columns:
            for label in ("true", "proxy"):
                label_count = int((outcome_frame["label_type"] == label).sum())
                label_type_ratio[label] = round(label_count / row_count, 4)
        per_outcome[outcome] = {
            "rows": row_count,
            "label_type_ratio": label_type_ratio,
        }
        if row_count < threshold:
            missing.append(f"{outcome}={row_count}")

    if missing:
        raise ValueError(
            "failed_precondition: insufficient rows per outcome "
            f"(min_rows_per_outcome={threshold}, missing={', '.join(missing)})",
        )

    return {
        "min_rows_per_outcome": threshold,
        "rows_by_outcome": per_outcome,
    }


def _summarize_refuters(refuters: dict[str, Any]) -> dict[str, Any]:
    passed = 0
    failed = 0
    skipped = 0
    by_method: dict[str, dict[str, int]] = {}

    for payload in refuters.values():
        if not isinstance(payload, dict):
            continue
        for method, raw in payload.items():
            text = str(raw or "").strip().lower()
            bucket = by_method.setdefault(str(method), {"passed": 0, "failed": 0, "skipped": 0})
            if "skipped" in text:
                skipped += 1
                bucket["skipped"] += 1
            elif "failed" in text or "error" in text:
                failed += 1
                bucket["failed"] += 1
            else:
                passed += 1
                bucket["passed"] += 1

    total = passed + failed + skipped
    return {
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "pass_rate": round(passed / max(1, passed + failed), 4),
        "by_method": dict(sorted(by_method.items())),
    }


def _fit_outcome_calibrators(
    *,
    frame: pd.DataFrame,
    enabled: bool,
    warning_mode: str,
    warning_audit: WarningAudit,
) -> dict[str, Any]:
    return fit_outcome_calibrators(
        frame=frame,
        enabled=enabled,
        warning_mode=warning_mode,
        warning_audit=warning_audit,
        outcome_names=_OUTCOME_NAMES,
    )


def _discover_graph_with_consensus(
    frame: pd.DataFrame,
    *,
    model_version: str,
    bootstrap_iters: int,
    stability_threshold: float,
    bootstrap_parallelism: int,
    checkpoint_interval: int,
    resume_from_checkpoint: bool,
    early_stop_patience: int,
    discovery_sample_rows: int,
    discovery_max_features: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    numeric_cols = [
        c
        for c in frame.columns
        if c
        not in {
            "student_id",
            "school_id",
            "outcome_name",
            "label_type",
            "source",
            "data_origin",
            "observed_at",
        }
    ]
    outcome_specific = frame[frame["outcome_name"] == "admission_probability"].copy()
    if outcome_specific.empty:
        outcome_specific = frame.copy()
    row_cap = max(1, int(discovery_sample_rows))
    if len(outcome_specific) > row_cap:
        outcome_specific = outcome_specific.sample(n=row_cap, random_state=42)

    X = outcome_specific[numeric_cols].fillna(0.0)
    columns = list(X.columns)
    feature_cap = max(1, int(discovery_max_features))
    if len(columns) > feature_cap:
        priority_cols = [
            "school_selectivity",
            "school_acceptance_rate",
            "school_grad_rate",
            "school_net_price_norm",
            "school_endowment_norm",
            "student_gpa_norm",
            "student_sat_norm",
            "student_budget_norm",
            "academic_match",
            "affordability_ratio_norm",
            "affordability_gap_norm",
            "has_offer_signal",
        ]
        selected = [c for c in priority_cols if c in columns]
        selected.extend([c for c in columns if c not in selected])
        columns = selected[:feature_cap]
        X = X[columns]

    matrix = X.to_numpy(dtype=float)
    if len(matrix) == 0:
        raise ValueError("No rows available for graph discovery")

    checkpoint_path = _checkpoint_path_for_model(model_version)
    edge_counter: Counter[tuple[str, str]]
    pc_success: int
    ges_success: int
    completed_iters: int
    if resume_from_checkpoint:
        (
            edge_counter,
            pc_success,
            ges_success,
            completed_iters,
        ) = _load_discovery_checkpoint(checkpoint_path, columns)
    else:
        edge_counter = Counter()
        pc_success = 0
        ges_success = 0
        completed_iters = 0

    target_iters = max(1, int(bootstrap_iters))
    parallelism = max(1, int(bootstrap_parallelism))
    checkpoint_every = max(1, int(checkpoint_interval))
    patience = max(0, int(early_stop_patience))
    stable_iters = 0
    prev_selected: set[tuple[str, str]] | None = None
    early_stopped = False

    with ThreadPoolExecutor(max_workers=parallelism) as pool:
        while completed_iters < target_iters:
            batch_size = min(parallelism, target_iters - completed_iters)
            futures = [
                pool.submit(
                    _bootstrap_discovery_once,
                    matrix,
                    columns,
                    seed=np.random.randint(0, 2**31 - 1),
                )
                for _ in range(batch_size)
            ]
            batch_pc_success = 0
            batch_ges_success = 0
            for future in as_completed(futures):
                pc_edges, ges_edges, pc_ok, ges_ok = future.result()
                if pc_ok:
                    batch_pc_success += 1
                if ges_ok:
                    batch_ges_success += 1
                for edge in pc_edges & ges_edges:
                    edge_counter[edge] += 1

            completed_iters += batch_size
            pc_success += batch_pc_success
            ges_success += batch_ges_success

            should_checkpoint = (
                completed_iters % checkpoint_every == 0
                or completed_iters >= target_iters
            )
            if not should_checkpoint:
                continue

            current_selected = _selected_edges_for_threshold(
                edge_counter=edge_counter,
                completed_iters=completed_iters,
                stability_threshold=stability_threshold,
            )
            if prev_selected is not None:
                union = current_selected | prev_selected
                if union:
                    delta_ratio = len(current_selected ^ prev_selected) / len(union)
                else:
                    delta_ratio = 0.0
                if delta_ratio <= 0.005:
                    stable_iters += batch_size
                else:
                    stable_iters = 0
            prev_selected = set(current_selected)

            _write_discovery_checkpoint(
                checkpoint_path=checkpoint_path,
                columns=columns,
                edge_counter=edge_counter,
                pc_success=pc_success,
                ges_success=ges_success,
                completed_iters=completed_iters,
            )

            if patience > 0 and stable_iters >= patience:
                early_stopped = True
                break

    threshold_count = max(1, int(np.ceil(completed_iters * stability_threshold)))
    selected = {
        edge: cnt / max(1, completed_iters)
        for edge, cnt in edge_counter.items()
        if cnt >= threshold_count and edge not in _DEFAULT_FORBIDDEN_EDGES
    }
    if not selected:
        selected = _fallback_domain_graph()

    dag = nx.DiGraph()
    for node in columns:
        dag.add_node(node)
    for (src, dst), stability in selected.items():
        dag.add_edge(src, dst, stability=stability)

    # DAG repair if needed
    while not nx.is_directed_acyclic_graph(dag):
        cycle = next(nx.simple_cycles(dag))
        weakest = None
        weakest_stability = 10.0
        for i in range(len(cycle)):
            u, v = cycle[i], cycle[(i + 1) % len(cycle)]
            if dag.has_edge(u, v):
                st = float(dag.edges[u, v].get("stability", 0.0))
                if st < weakest_stability:
                    weakest_stability = st
                    weakest = (u, v)
        if weakest:
            dag.remove_edge(*weakest)
        else:
            break

    graph_json = {
        "nodes": [{"id": n} for n in dag.nodes()],
        "edges": [
            {
                "source": u,
                "target": v,
                "stability": float(dag.edges[u, v].get("stability", 0.0)),
            }
            for u, v in dag.edges()
        ],
    }
    metrics = {
        "bootstrap_iters_target": target_iters,
        "bootstrap_iters_completed": completed_iters,
        "stability_threshold": stability_threshold,
        "discovery_sample_rows": row_cap,
        "discovery_max_features": feature_cap,
        "bootstrap_parallelism": parallelism,
        "checkpoint_interval": checkpoint_every,
        "resume_from_checkpoint": bool(resume_from_checkpoint),
        "early_stop_patience": patience,
        "early_stopped": early_stopped,
        "checkpoint_path": str(checkpoint_path),
        "selected_edge_count": len(graph_json["edges"]),
        "pc_success": pc_success,
        "ges_success": ges_success,
        "pc_success_rate": round(pc_success / max(1, completed_iters), 4),
        "ges_success_rate": round(ges_success / max(1, completed_iters), 4),
        "edge_stability_p50": (
            round(float(np.percentile([e["stability"] for e in graph_json["edges"]], 50)), 4)
            if graph_json["edges"]
            else 0.0
        ),
        "edge_stability_p90": (
            round(float(np.percentile([e["stability"] for e in graph_json["edges"]], 90)), 4)
            if graph_json["edges"]
            else 0.0
        ),
    }
    return graph_json, metrics


def _bootstrap_discovery_once(
    matrix: np.ndarray,
    columns: list[str],
    *,
    seed: int,
) -> tuple[set[tuple[str, str]], set[tuple[str, str]], bool, bool]:
    """Run one bootstrap trial of PC+GES and return discovered directed edges."""
    rng = np.random.default_rng(seed)
    idx = rng.integers(low=0, high=len(matrix), size=len(matrix))
    sampled = matrix[idx]
    pc_edges: set[tuple[str, str]] = set()
    ges_edges: set[tuple[str, str]] = set()
    pc_ok = False
    ges_ok = False
    try:
        pc_edges = _run_pc(sampled, columns)
        pc_ok = True
    except Exception:
        pc_edges = set()
    try:
        ges_edges = _run_ges(sampled, columns)
        ges_ok = True
    except Exception:
        ges_edges = set()
    return pc_edges, ges_edges, pc_ok, ges_ok


def _checkpoint_path_for_model(model_version: str) -> Path:
    root = Path(".benchmarks/causal_models/checkpoints")
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{model_version}.json"


def _edge_to_key(edge: tuple[str, str]) -> str:
    return f"{edge[0]}->{edge[1]}"


def _key_to_edge(raw: str) -> tuple[str, str] | None:
    if "->" not in raw:
        return None
    src, dst = raw.split("->", 1)
    src = src.strip()
    dst = dst.strip()
    if not src or not dst:
        return None
    return src, dst


def _selected_edges_for_threshold(
    *,
    edge_counter: Counter[tuple[str, str]],
    completed_iters: int,
    stability_threshold: float,
) -> set[tuple[str, str]]:
    threshold_count = max(1, int(np.ceil(max(1, completed_iters) * stability_threshold)))
    return {
        edge
        for edge, cnt in edge_counter.items()
        if cnt >= threshold_count and edge not in _DEFAULT_FORBIDDEN_EDGES
    }


def _write_discovery_checkpoint(
    *,
    checkpoint_path: Path,
    columns: list[str],
    edge_counter: Counter[tuple[str, str]],
    pc_success: int,
    ges_success: int,
    completed_iters: int,
) -> None:
    payload = {
        "columns": columns,
        "pc_success": int(pc_success),
        "ges_success": int(ges_success),
        "completed_iters": int(completed_iters),
        "edge_counter": {
            _edge_to_key(edge): int(count)
            for edge, count in edge_counter.items()
        },
        "updated_at": datetime.now(UTC).isoformat(),
    }
    checkpoint_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _load_discovery_checkpoint(
    checkpoint_path: Path,
    columns: list[str],
) -> tuple[Counter[tuple[str, str]], int, int, int]:
    if not checkpoint_path.exists():
        return Counter(), 0, 0, 0
    try:
        raw = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to load checkpoint %s, start from scratch", checkpoint_path)
        return Counter(), 0, 0, 0

    saved_cols = raw.get("columns")
    if not isinstance(saved_cols, list) or [str(v) for v in saved_cols] != columns:
        logger.warning("Checkpoint %s columns mismatch, ignore resume", checkpoint_path)
        return Counter(), 0, 0, 0

    edge_counter = Counter()
    saved_counter = raw.get("edge_counter")
    if isinstance(saved_counter, dict):
        for key, value in saved_counter.items():
            edge = _key_to_edge(str(key))
            if edge is None:
                continue
            try:
                edge_counter[edge] = int(value)
            except (TypeError, ValueError):
                continue

    def _to_int(name: str) -> int:
        try:
            return max(0, int(raw.get(name, 0)))
        except (TypeError, ValueError):
            return 0

    pc_success = _to_int("pc_success")
    ges_success = _to_int("ges_success")
    completed_iters = _to_int("completed_iters")
    logger.info(
        "Resumed discovery checkpoint %s (iters=%d, edges=%d)",
        checkpoint_path,
        completed_iters,
        len(edge_counter),
    )
    return edge_counter, pc_success, ges_success, completed_iters


def _run_pywhy_estimations(
    frame: pd.DataFrame,
    *,
    warning_audit: WarningAudit,
    warning_mode: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        from dowhy import CausalModel
    except Exception as exc:
        raise RuntimeError("DoWhy/EconML not available for offline training") from exc

    metrics: dict[str, Any] = {}
    refuter_out: dict[str, Any] = {}
    feature_cols = [
        c for c in frame.columns
        if c
        not in {
            "student_id",
            "school_id",
            "outcome_name",
            "outcome_value",
            "label_type",
            "label_confidence",
            "source",
            "data_origin",
            "observed_at",
        }
    ]
    treatment_col = "school_selectivity"
    if treatment_col not in frame.columns:
        return {"warning": "school_selectivity not in feature set"}, {}

    # Prevent treatment leakage in offline estimation diagnostics.
    model_feature_cols = [c for c in feature_cols if c != treatment_col]
    fit_attempts = 0
    fit_success = 0
    fit_fallback = 0
    effect_means: list[float] = []
    effect_stds: list[float] = []

    for outcome_name in _OUTCOME_NAMES:
        df = frame[frame["outcome_name"] == outcome_name].copy()
        if len(df) < 40:
            metrics[outcome_name] = {"skipped": "insufficient_rows", "rows": len(df)}
            continue
        fit_attempts += 1
        y = df["outcome_value"].to_numpy(dtype=float)
        t = df[treatment_col].to_numpy(dtype=float)
        x = df[model_feature_cols].fillna(0.0).to_numpy(dtype=float)
        binary = set(np.unique(y)).issubset({0.0, 1.0})
        sample_size = min(128, len(x))

        with capture_stage_warnings(
            stage=f"training.identify.{outcome_name}",
            warning_mode=warning_mode,
            audit=warning_audit,
        ):
            model = CausalModel(
                data=df[[*model_feature_cols, treatment_col, "outcome_value"]],
                treatment=treatment_col,
                outcome="outcome_value",
                common_causes=model_feature_cols,
            )
            identified = model.identify_effect(proceed_when_unidentifiable=True)

        estimator_name = ""
        fallback_used = False
        effect_arr: np.ndarray
        try:
            if binary:
                estimator_name = "forest_dr"
                with capture_stage_warnings(
                    stage=f"training.fit.{outcome_name}.forest_dr",
                    warning_mode=warning_mode,
                    audit=warning_audit,
                ):
                    effect_arr = _estimate_binary_forest_dr(y=y, t=t, X=x, sample_size=sample_size)
            else:
                estimator_name = "causal_forest_dml"
                with capture_stage_warnings(
                    stage=f"training.fit.{outcome_name}.causal_forest",
                    warning_mode=warning_mode,
                    audit=warning_audit,
                ):
                    effect_arr = _estimate_continuous_causal_forest(
                        y=y,
                        t=t,
                        X=x,
                        sample_size=sample_size,
                    )
            fit_success += 1
        except Exception as exc:
            fit_fallback += 1
            fallback_used = True
            try:
                if binary:
                    estimator_name = "dr_learner_fallback"
                    with capture_stage_warnings(
                        stage=f"training.fit.{outcome_name}.dr_fallback",
                        warning_mode=warning_mode,
                        audit=warning_audit,
                    ):
                        effect_arr = _estimate_binary_dr_fallback(
                            y=y,
                            t=t,
                            X=x,
                            sample_size=sample_size,
                        )
                else:
                    estimator_name = "linear_dml_fallback"
                    with capture_stage_warnings(
                        stage=f"training.fit.{outcome_name}.linear_dml_fallback",
                        warning_mode=warning_mode,
                        audit=warning_audit,
                    ):
                        effect_arr = _estimate_continuous_linear_dml_fallback(
                            y=y,
                            t=t,
                            X=x,
                            sample_size=sample_size,
                        )
                fit_success += 1
            except Exception as fallback_exc:
                metrics[outcome_name] = {
                    "rows": len(df),
                    "binary_outcome": binary,
                    "failed": f"aggressive_error={exc}; fallback_error={fallback_exc}",
                }
                refuter_out[outcome_name] = {
                    "placebo_treatment_refuter": "skipped: fit failed",
                    "random_common_cause": "skipped: fit failed",
                    "data_subset_refuter": "skipped: fit failed",
                }
                continue

        effect_arr = np.asarray(effect_arr, dtype=float).reshape(-1)
        ate = float(np.mean(effect_arr)) if len(effect_arr) > 0 else 0.0
        effect_std = float(np.std(effect_arr)) if len(effect_arr) > 0 else 0.0
        effect_means.append(ate)
        effect_stds.append(effect_std)
        metrics[outcome_name] = {
            "rows": len(df),
            "binary_outcome": binary,
            "estimator_name": estimator_name,
            "fallback_used": fallback_used,
            "ate_proxy": ate,
            "mean_outcome": float(np.mean(y)),
            "std_outcome": float(np.std(y)),
            "effect_stats": {
                "mean": ate,
                "std": effect_std,
                "p10": float(np.percentile(effect_arr, 10)) if len(effect_arr) > 0 else 0.0,
                "p50": float(np.percentile(effect_arr, 50)) if len(effect_arr) > 0 else 0.0,
                "p90": float(np.percentile(effect_arr, 90)) if len(effect_arr) > 0 else 0.0,
            },
        }

        # DoWhy refuters
        refuter_out[outcome_name] = {}
        for refuter in ("placebo_treatment_refuter", "random_common_cause", "data_subset_refuter"):
            with capture_stage_warnings(
                stage=f"training.refuter.{outcome_name}.{refuter}",
                warning_mode=warning_mode,
                audit=warning_audit,
            ):
                try:
                    ref = model.refute_estimate(
                        identified,
                        estimate=None,
                        method_name=refuter,
                    )
                    refuter_out[outcome_name][refuter] = str(ref)
                except Exception as exc:
                    refuter_out[outcome_name][refuter] = f"failed: {exc}"

    metrics["_summary"] = {
        "fit_attempts": fit_attempts,
        "fit_success_count": fit_success,
        "fit_success_rate": round(fit_success / max(1, fit_attempts), 4),
        "fit_fallback_count": fit_fallback,
        "warning_count": int(warning_audit.total),
        "effect_distribution": {
            "mean_of_means": float(np.mean(effect_means)) if effect_means else 0.0,
            "mean_of_stds": float(np.mean(effect_stds)) if effect_stds else 0.0,
            "max_abs_mean": float(max((abs(v) for v in effect_means), default=0.0)),
        },
    }
    return metrics, refuter_out


def _estimate_binary_forest_dr(
    *,
    y: np.ndarray,
    t: np.ndarray,
    X: np.ndarray,
    sample_size: int,
) -> np.ndarray:
    from econml.dr import ForestDRLearner
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor

    y_binary = (y >= np.median(y)).astype(int)
    t_binary = (t >= np.median(t)).astype(int)
    learner = ForestDRLearner(
        model_propensity=RandomForestClassifier(
            n_estimators=120,
            random_state=42,
            max_depth=8,
            min_samples_leaf=3,
        ),
        model_regression=RandomForestRegressor(
            n_estimators=120,
            random_state=42,
            max_depth=8,
            min_samples_leaf=3,
        ),
        n_estimators=160,
        max_depth=10,
        min_samples_leaf=3,
        random_state=42,
    )
    learner.fit(y_binary, t_binary, X=X)
    return np.asarray(learner.effect(X[:sample_size]), dtype=float)


def _estimate_binary_dr_fallback(
    *,
    y: np.ndarray,
    t: np.ndarray,
    X: np.ndarray,
    sample_size: int,
) -> np.ndarray:
    from econml.dr import DRLearner
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor

    y_binary = (y >= np.median(y)).astype(int)
    t_binary = (t >= np.median(t)).astype(int)
    learner = DRLearner(
        model_propensity=RandomForestClassifier(
            n_estimators=80,
            random_state=42,
            max_depth=6,
        ),
        model_regression=RandomForestRegressor(
            n_estimators=80,
            random_state=42,
            max_depth=6,
        ),
        random_state=42,
    )
    learner.fit(y_binary, t_binary, X=X)
    return np.asarray(learner.effect(X[:sample_size]), dtype=float)


def _estimate_continuous_causal_forest(
    *,
    y: np.ndarray,
    t: np.ndarray,
    X: np.ndarray,
    sample_size: int,
) -> np.ndarray:
    from econml.dml import CausalForestDML
    from sklearn.ensemble import RandomForestRegressor

    learner = CausalForestDML(
        model_y=RandomForestRegressor(
            n_estimators=120,
            random_state=42,
            max_depth=8,
            min_samples_leaf=3,
        ),
        model_t=RandomForestRegressor(
            n_estimators=120,
            random_state=42,
            max_depth=8,
            min_samples_leaf=3,
        ),
        n_estimators=160,
        max_depth=10,
        min_samples_leaf=3,
        random_state=42,
        inference=False,
    )
    learner.fit(y, t, X=X)
    t_q25 = float(np.quantile(t, 0.25))
    t_q75 = float(np.quantile(t, 0.75))
    return np.asarray(
        learner.effect(
            X[:sample_size],
            T0=np.full(sample_size, t_q25),
            T1=np.full(sample_size, t_q75),
        ),
        dtype=float,
    )


def _estimate_continuous_linear_dml_fallback(
    *,
    y: np.ndarray,
    t: np.ndarray,
    X: np.ndarray,
    sample_size: int,
) -> np.ndarray:
    from econml.dml import LinearDML
    from sklearn.ensemble import RandomForestRegressor

    learner = LinearDML(
        model_y=RandomForestRegressor(n_estimators=100, random_state=42, max_depth=8),
        model_t=RandomForestRegressor(n_estimators=100, random_state=42, max_depth=8),
        random_state=42,
    )
    learner.fit(y, t, X=X)
    t_q25 = float(np.quantile(t, 0.25))
    t_q75 = float(np.quantile(t, 0.75))
    return np.asarray(
        learner.effect(
            X[:sample_size],
            T0=np.full(sample_size, t_q25),
            T1=np.full(sample_size, t_q75),
        ),
        dtype=float,
    )


def _run_pc(matrix: np.ndarray, columns: list[str]) -> set[tuple[str, str]]:
    try:
        from causallearn.search.ConstraintBased.PC import pc
    except Exception as exc:
        raise RuntimeError("causal-learn PC unavailable") from exc

    result = pc(matrix, alpha=0.05, stable=True)
    graph = result.G.graph
    edges = set()
    # graph[i,j] == -1 and graph[j,i] == 1 means i -> j in causal-learn.
    for i in range(graph.shape[0]):
        for j in range(graph.shape[1]):
            if graph[i, j] == -1 and graph[j, i] == 1:
                edges.add((columns[i], columns[j]))
    return edges


def _run_ges(matrix: np.ndarray, columns: list[str]) -> set[tuple[str, str]]:
    try:
        from causallearn.search.ScoreBased.GES import ges
    except Exception as exc:
        raise RuntimeError("causal-learn GES unavailable") from exc

    result = ges(matrix)
    graph = result["G"].graph
    edges = set()
    for i in range(graph.shape[0]):
        for j in range(graph.shape[1]):
            if graph[i, j] == -1 and graph[j, i] == 1:
                edges.add((columns[i], columns[j]))
    return edges


def _fallback_domain_graph() -> dict[tuple[str, str], float]:
    builder = AdmissionDAGBuilder()
    dag = builder.build_admission_dag()
    fallback = {}
    for u, v, attrs in dag.edges(data=True):
        fallback[(u, v)] = float(attrs.get("evidence_score", 0.5))
    return fallback


def _write_training_artifact(
    model_version: str,
    graph_json: dict[str, Any],
    metrics: dict[str, Any],
    refuters: dict[str, Any],
) -> str:
    root = Path(".benchmarks/causal_models")
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{model_version}.json"
    payload = {
        "model_version": model_version,
        "graph": graph_json,
        "metrics": metrics,
        "refuters": refuters,
        "created_at": datetime.now(UTC).isoformat(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    return str(path)
