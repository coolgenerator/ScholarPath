"""PyWhy adapter with safe fallback semantics.

This module intentionally keeps runtime deterministic and low-risk:
if no active model exists, the caller should fallback to legacy.
"""

from __future__ import annotations

import math
from typing import Any

from sqlalchemy import select

from scholarpath.causal_engine.scoring import CausalFeatureView, compute_pywhy_raw_scores
from scholarpath.causal_engine.training_calibration import apply_calibration
from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
)
from scholarpath.db.models import CausalModelRegistry
from scholarpath.db.session import async_session_factory

_OUTCOMES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]


class PyWhyCausalEngine:
    """Lightweight runtime adapter reading active model metadata."""

    engine_name = "pywhy"

    async def estimate(self, request: CausalRequestContext) -> CausalEstimateResult:
        model = await _get_active_model()
        if model is None:
            raise RuntimeError("No active pywhy model found")

        raw_scores = compute_pywhy_raw_scores(
            CausalFeatureView(
                student_features=request.student_features,
                school_features=request.school_features,
                interaction_features=request.interaction_features,
            )
        )
        calibrated = _apply_calibration(raw_scores, model.metrics_json or {})

        return CausalEstimateResult(
            scores=calibrated,
            confidence=0.88,
            engine_used=self.engine_name,
            model_version=model.model_version,
            metadata={
                "calibration_applied": True,
                "calibration_version": model.model_version,
                "calibration_method_by_outcome": _calibration_method_map(model.metrics_json or {}),
            },
        )

    async def intervene(
        self,
        request: CausalRequestContext,
        interventions: dict[str, float],
    ) -> CausalInterventionResult:
        original = await self.estimate(request)
        modified_req = CausalRequestContext(
            context=request.context,
            student_id=request.student_id,
            school_id=request.school_id,
            offer_id=request.offer_id,
            student_features=dict(request.student_features),
            school_features=dict(request.school_features),
            interaction_features=dict(request.interaction_features),
            metadata=dict(request.metadata),
        )
        for key, value in interventions.items():
            target_value = _clip01(float(value))
            if key in modified_req.student_features:
                modified_req.student_features[key] = target_value
            elif key in modified_req.school_features:
                modified_req.school_features[key] = target_value
            elif key in modified_req.interaction_features:
                modified_req.interaction_features[key] = target_value
        modified = await self.estimate(modified_req)
        deltas = {
            outcome: round(modified.scores.get(outcome, 0.0) - original.scores.get(outcome, 0.0), 6)
            for outcome in _OUTCOMES
        }
        return CausalInterventionResult(
            original_scores=original.scores,
            modified_scores=modified.scores,
            deltas=deltas,
            engine_used=self.engine_name,
            model_version=modified.model_version,
            metadata={"interventions": interventions},
        )

    async def explain(self, request: CausalRequestContext) -> CausalExplainResult:
        result = await self.estimate(request)
        ordered = sorted(result.scores.items(), key=lambda item: item[1], reverse=True)
        explanation = (
            f"PyWhy model {result.model_version} estimated strongest outcomes at "
            f"{ordered[0][0]} ({ordered[0][1]:.2f}) and {ordered[1][0]} ({ordered[1][1]:.2f})."
        )
        return CausalExplainResult(
            explanation=explanation,
            reasons=[{"type": "score_rank", "ordered_scores": ordered}],
            engine_used=self.engine_name,
            model_version=result.model_version,
            metadata=result.metadata,
        )


async def _get_active_model() -> CausalModelRegistry | None:
    async with async_session_factory() as session:
        stmt = (
            select(CausalModelRegistry)
            .where(CausalModelRegistry.is_active.is_(True))
            .order_by(CausalModelRegistry.updated_at.desc())
            .limit(1)
        )
        return (await session.execute(stmt)).scalars().first()


def _clip01(value: float) -> float:
    return round(max(0.0, min(1.0, value)), 6)


def _apply_calibration(scores: dict[str, float], metrics: dict[str, Any]) -> dict[str, float]:
    cfg = metrics.get("calibration") if isinstance(metrics, dict) else {}
    if not isinstance(cfg, dict):
        return scores
    out = dict(scores)
    for outcome, score in scores.items():
        row = cfg.get(outcome)
        if not isinstance(row, dict):
            continue
        method = str(row.get("method") or "").strip().lower()
        if method in {"linear", "none"}:
            out[outcome] = _clip01(apply_calibration(score, row))
        elif method == "sigmoid":
            k = float(row.get("k", 1.0))
            x0 = float(row.get("x0", 0.5))
            out[outcome] = _clip01(1.0 / (1.0 + math.exp(-k * (score - x0))))
    return out


def _calibration_method_map(metrics: dict[str, Any]) -> dict[str, str]:
    cfg = metrics.get("calibration") if isinstance(metrics, dict) else {}
    if not isinstance(cfg, dict):
        return {}
    out: dict[str, str] = {}
    for outcome, row in cfg.items():
        if isinstance(row, dict):
            out[outcome] = str(row.get("method") or "none")
    return out
