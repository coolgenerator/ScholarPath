"""PyWhy-based causal engine (DoWhy + EconML)."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.causal_engine.interfaces import CausalEngine
from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
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


class PyWhyUnavailableError(RuntimeError):
    """Raised when PyWhy stack is unavailable or unusable."""


@dataclass(slots=True)
class _OutcomeFittedModel:
    outcome: str
    estimator_name: str
    fitted_with_fallback: bool
    model: Any
    is_binary_outcome: bool
    x_names: list[str]
    y_mean: float
    y_std: float
    t_median: float
    t_q25: float
    t_q75: float
    label_type: str
    label_confidence: float
    row_count: int
    warnings_total: int
    warnings_by_stage: dict[str, int]
    diagnostics: dict[str, Any]


class PyWhyCausalEngine(CausalEngine):
    """Causal engine using DoWhy + EconML estimators."""

    engine_version = "pywhy_v1"
    _PROCESS_FITTED_CACHE: dict[str, _OutcomeFittedModel] = {}
    _PROCESS_FIT_LOCKS: dict[str, asyncio.Lock] = {}
    _PROCESS_WARMED_MODELS: set[str] = set()

    def __init__(
        self,
        *,
        session: AsyncSession,
        model_version_hint: str = "latest_stable",
        lookback_days: int = 365,
        warning_mode: str = "count_silent",
    ) -> None:
        self._session = session
        self._model_version_hint = model_version_hint
        self._lookback_days = lookback_days
        self._warning_mode = normalize_warning_mode(warning_mode)
        self._active_model: CausalModelRegistry | None = None
        self._active_model_version: str | None = None
        self._cache_model_version: str | None = None
        self._rows_cache: dict[str, list[dict[str, float]]] = {}
        self._fitted_cache: dict[str, _OutcomeFittedModel] = {}
        self._calibration_version: str = "none"
        self._calibration_by_outcome: dict[str, dict[str, Any]] = {}
        self._warmup_applied: bool = False

    async def estimate(
        self,
        ctx: CausalRequestContext,
        outcomes: list[str],
    ) -> CausalEstimateResult:
        self._ensure_dependencies()
        model = await self._resolve_model()
        self._ensure_cache_for_model(model.model_version)
        requested = outcomes or [
            "admission_probability",
            "academic_outcome",
            "career_outcome",
            "life_satisfaction",
            "phd_probability",
        ]

        scores: dict[str, float] = {}
        conf: dict[str, float] = {}
        label_confidences: list[float] = []
        label_types: list[str] = []
        warning_total = 0
        warning_by_stage: dict[str, int] = {}
        outcome_estimators: dict[str, str] = {}
        estimator_fallback_outcomes: list[str] = []
        row_count_by_outcome: dict[str, int] = {}
        outcome_diagnostics: dict[str, Any] = {}
        calibration_method_by_outcome: dict[str, str] = {}
        cache_hit_by_outcome: dict[str, bool] = {}
        fit_reused = False
        calibration_applied = False

        for outcome in requested:
            y_value, y_conf, y_label, meta = await self._estimate_single_outcome(
                ctx,
                outcome,
            )
            scores[outcome] = y_value
            conf[outcome] = y_conf
            label_confidences.append(y_label[1])
            label_types.append(y_label[0])
            warning_total += int(meta.get("warnings_total", 0) or 0)
            for stage, count in (meta.get("warnings_by_stage") or {}).items():
                warning_by_stage[stage] = warning_by_stage.get(stage, 0) + int(count or 0)
            estimator_name = str(meta.get("estimator_name") or "").strip()
            if estimator_name:
                outcome_estimators[outcome] = estimator_name
            if bool(meta.get("fitted_with_fallback", False)):
                estimator_fallback_outcomes.append(outcome)
            row_count_by_outcome[outcome] = int(meta.get("row_count", 0) or 0)
            diag = meta.get("diagnostics")
            if isinstance(diag, dict):
                outcome_diagnostics[outcome] = diag
            method = str(meta.get("calibration_method") or "").strip()
            if method:
                calibration_method_by_outcome[outcome] = method
            if bool(meta.get("calibration_applied")):
                calibration_applied = True
            cache_hit = bool(meta.get("cache_hit"))
            cache_hit_by_outcome[outcome] = cache_hit
            fit_reused = fit_reused or cache_hit

        estimate_conf = float(np.mean(list(conf.values()))) if conf else 0.5
        label_type = "true" if any(v == "true" for v in label_types) else "proxy"
        label_conf = float(np.mean(label_confidences)) if label_confidences else 0.5

        return CausalEstimateResult(
            scores=scores,
            confidence_by_outcome=conf,
            estimate_confidence=max(0.0, min(1.0, estimate_conf)),
            label_type=label_type,
            label_confidence=max(0.0, min(1.0, label_conf)),
            causal_engine_version=self.engine_version,
            causal_model_version=model.model_version,
            metadata={
                "model_status": model.status,
                "warnings_total": warning_total,
                "warnings_by_stage": dict(sorted(warning_by_stage.items())),
                "warning_mode": self._warning_mode,
                "estimator_by_outcome": outcome_estimators,
                "estimator_fallback_outcomes": estimator_fallback_outcomes,
                "row_count_by_outcome": row_count_by_outcome,
                "outcome_diagnostics": outcome_diagnostics,
                "calibration_applied": calibration_applied,
                "calibration_method_by_outcome": dict(sorted(calibration_method_by_outcome.items())),
                "calibration_version": self._calibration_version,
                "cache_hit_by_outcome": dict(sorted(cache_hit_by_outcome.items())),
                "warmup_applied": bool(self._warmup_applied),
                "fit_reused": bool(fit_reused),
            },
        )

    async def warmup(self, outcomes: list[str] | None = None) -> dict[str, bool]:
        self._ensure_dependencies()
        model = await self._resolve_model()
        self._ensure_cache_for_model(model.model_version)
        requested = outcomes or [
            "admission_probability",
            "academic_outcome",
            "career_outcome",
            "life_satisfaction",
            "phd_probability",
        ]
        model_version = model.model_version
        warm_key = f"{model_version}:{int(self._lookback_days)}"
        cache_hits: dict[str, bool] = {}
        if warm_key in self._PROCESS_WARMED_MODELS:
            self._warmup_applied = False
            for outcome in requested:
                key = self._process_cache_key(
                    model_version=model_version,
                    outcome=outcome,
                    lookback_days=self._lookback_days,
                )
                cache_hits[outcome] = key in self._PROCESS_FITTED_CACHE
            return cache_hits

        self._warmup_applied = True
        for outcome in requested:
            _, cache_hit = await self._get_or_fit_outcome_model(outcome)
            cache_hits[outcome] = cache_hit
        self._PROCESS_WARMED_MODELS.add(warm_key)
        return cache_hits

    async def intervene(
        self,
        ctx: CausalRequestContext,
        interventions: dict[str, float],
        outcomes: list[str],
    ) -> CausalInterventionResult:
        baseline = await self.estimate(ctx, outcomes)
        overridden = self._context_with_interventions(ctx, interventions)
        modified = await self.estimate(overridden, outcomes)
        deltas = {
            key: round(modified.scores.get(key, 0.0) - baseline.scores.get(key, 0.0), 4)
            for key in modified.scores
        }
        return CausalInterventionResult(
            original_scores=baseline.scores,
            modified_scores=modified.scores,
            deltas=deltas,
            estimate_confidence=min(baseline.estimate_confidence, modified.estimate_confidence),
            label_type=baseline.label_type,
            label_confidence=baseline.label_confidence,
            causal_engine_version=self.engine_version,
            causal_model_version=baseline.causal_model_version,
        )

    async def explain(
        self,
        ctx: CausalRequestContext,
        result: CausalEstimateResult,
    ) -> CausalExplainResult:
        top = sorted(result.scores.items(), key=lambda kv: kv[1], reverse=True)[:3]
        factors = [f"{name}: {value:.1%}" for name, value in top]
        return CausalExplainResult(
            summary="PyWhy estimate composed from DoWhy-identified effects and EconML estimators.",
            key_factors=factors,
            causal_engine_version=self.engine_version,
            causal_model_version=result.causal_model_version,
        )

    async def _resolve_model(self) -> CausalModelRegistry:
        if self._active_model is not None:
            return self._active_model

        if self._model_version_hint != "latest_stable":
            stmt = select(CausalModelRegistry).where(
                CausalModelRegistry.model_version == self._model_version_hint,
            )
            row = await self._session.execute(stmt)
            model = row.scalars().first()
            if model is None:
                raise PyWhyUnavailableError(
                    f"Causal model version '{self._model_version_hint}' not found",
                )
            self._active_model = model
            self._active_model_version = model.model_version
            self._load_calibration_from_model(model)
            return model

        stmt = (
            select(CausalModelRegistry)
            .where(CausalModelRegistry.is_active.is_(True))
            .order_by(CausalModelRegistry.updated_at.desc())
            .limit(1)
        )
        row = await self._session.execute(stmt)
        model = row.scalars().first()
        if model is None:
            raise PyWhyUnavailableError("No active causal model in registry")

        self._active_model = model
        self._active_model_version = model.model_version
        self._load_calibration_from_model(model)
        return model

    async def _estimate_single_outcome(
        self,
        ctx: CausalRequestContext,
        outcome: str,
    ) -> tuple[float, float, tuple[str, float], dict[str, Any]]:
        fitted, cache_hit = await self._get_or_fit_outcome_model(outcome)
        t_curr = float(ctx.school_features.get("school_selectivity", 0.5))
        x_curr = np.array(
            [[ctx.all_features.get(name, 0.0) for name in fitted.x_names]],
            dtype=float,
        )
        effect_raw = self._predict_effect_for_context(
            fitted=fitted,
            x_curr=x_curr,
            t_curr=t_curr,
        )
        score_raw = self._compose_score(
            y_mean=fitted.y_mean,
            y_std=fitted.y_std,
            effect_value=effect_raw,
            row_count=fitted.row_count,
        )
        score, calibration_method, calibration_applied = self._apply_outcome_calibration(
            outcome=outcome,
            raw_score=score_raw,
        )
        confidence = self._estimate_confidence(
            y_std=fitted.y_std,
            row_count=fitted.row_count,
            fitted_with_fallback=fitted.fitted_with_fallback,
            warning_count=fitted.warnings_total,
        )
        meta = {
            "estimator_name": fitted.estimator_name,
            "fitted_with_fallback": fitted.fitted_with_fallback,
            "row_count": fitted.row_count,
            "warnings_total": fitted.warnings_total,
            "warnings_by_stage": dict(sorted(fitted.warnings_by_stage.items())),
            "diagnostics": dict(fitted.diagnostics),
            "calibration_applied": calibration_applied,
            "calibration_method": calibration_method,
            "score_raw": score_raw,
            "cache_hit": cache_hit,
        }
        return score, confidence, (fitted.label_type, fitted.label_confidence), meta

    async def _get_or_fit_outcome_model(self, outcome: str) -> tuple[_OutcomeFittedModel, bool]:
        cached = self._fitted_cache.get(outcome)
        if cached is not None:
            return cached, True

        model_version = self._active_model_version or "latest_stable"
        cache_key = self._process_cache_key(
            model_version=model_version,
            outcome=outcome,
            lookback_days=self._lookback_days,
        )
        process_cached = self._PROCESS_FITTED_CACHE.get(cache_key)
        if process_cached is not None:
            self._fitted_cache[outcome] = process_cached
            return process_cached, True

        lock = self._PROCESS_FIT_LOCKS.get(cache_key)
        if lock is None:
            lock = asyncio.Lock()
            self._PROCESS_FIT_LOCKS[cache_key] = lock
        async with lock:
            cached = self._fitted_cache.get(outcome)
            if cached is not None:
                return cached, True

            process_cached = self._PROCESS_FITTED_CACHE.get(cache_key)
            if process_cached is not None:
                self._fitted_cache[outcome] = process_cached
                return process_cached, True

            fitted = await self._fit_outcome_model(outcome)
            self._fitted_cache[outcome] = fitted
            self._PROCESS_FITTED_CACHE[cache_key] = fitted
            return fitted, False

    async def _fit_outcome_model(self, outcome: str) -> _OutcomeFittedModel:
        frame = await self._load_training_rows(outcome)
        if len(frame) < 30:
            raise PyWhyUnavailableError(
                f"Insufficient training rows for {outcome}: {len(frame)}",
            )

        y = np.array([r["y"] for r in frame], dtype=float)
        t = np.array([r["t"] for r in frame], dtype=float)
        # Prevent treatment leakage: school_selectivity (t) must not appear in X.
        x_names = sorted(
            k
            for k in frame[0]
            if k
            not in {
                "y",
                "t",
                "label_type",
                "label_confidence",
                "school_selectivity",
            }
        )
        X = np.array([[r.get(name, 0.0) for name in x_names] for r in frame], dtype=float)
        label_type = "true" if any(r.get("label_type") == "true" for r in frame) else "proxy"
        label_conf = float(np.mean([r.get("label_confidence", 0.5) for r in frame]))
        is_binary_outcome = set(np.unique(y)).issubset({0.0, 1.0})
        t_median = float(np.median(t))
        t_q25 = float(np.quantile(t, 0.25))
        t_q75 = float(np.quantile(t, 0.75))
        warning_audit = WarningAudit()
        fit_diagnostics: dict[str, Any] = {
            "x_feature_count": len(x_names),
            "row_count": len(frame),
            "is_binary_outcome": is_binary_outcome,
        }

        if is_binary_outcome:
            estimator_name = "forest_dr"
            fitted_with_fallback = False
            try:
                with capture_stage_warnings(
                    stage=f"estimate.fit.{outcome}.forest_dr",
                    warning_mode=self._warning_mode,
                    audit=warning_audit,
                ):
                    model = self._fit_forest_dr(y=y, t=t, X=X)
            except Exception as exc:
                fitted_with_fallback = True
                estimator_name = "dr_learner_fallback"
                fit_diagnostics["fallback_reason"] = str(exc)
                with capture_stage_warnings(
                    stage=f"estimate.fit.{outcome}.dr_fallback",
                    warning_mode=self._warning_mode,
                    audit=warning_audit,
                ):
                    model = self._fit_dr_fallback(y=y, t=t, X=X)
        else:
            estimator_name = "causal_forest_dml"
            fitted_with_fallback = False
            try:
                with capture_stage_warnings(
                    stage=f"estimate.fit.{outcome}.causal_forest",
                    warning_mode=self._warning_mode,
                    audit=warning_audit,
                ):
                    model = self._fit_causal_forest_dml(y=y, t=t, X=X)
            except Exception as exc:
                fitted_with_fallback = True
                estimator_name = "linear_dml_fallback"
                fit_diagnostics["fallback_reason"] = str(exc)
                with capture_stage_warnings(
                    stage=f"estimate.fit.{outcome}.linear_dml_fallback",
                    warning_mode=self._warning_mode,
                    audit=warning_audit,
                ):
                    model = self._fit_linear_dml_fallback(y=y, t=t, X=X)

        fit_diagnostics["fit_success"] = True
        fit_diagnostics["fitted_with_fallback"] = fitted_with_fallback
        fit_diagnostics["warning_mode"] = self._warning_mode
        return _OutcomeFittedModel(
            outcome=outcome,
            estimator_name=estimator_name,
            fitted_with_fallback=fitted_with_fallback,
            model=model,
            is_binary_outcome=is_binary_outcome,
            x_names=x_names,
            y_mean=float(np.mean(y)),
            y_std=float(np.std(y)),
            t_median=t_median,
            t_q25=t_q25,
            t_q75=t_q75,
            label_type=label_type,
            label_confidence=label_conf,
            row_count=len(frame),
            warnings_total=warning_audit.total,
            warnings_by_stage=dict(warning_audit.by_stage),
            diagnostics=fit_diagnostics,
        )

    @staticmethod
    def _fit_forest_dr(*, y: np.ndarray, t: np.ndarray, X: np.ndarray) -> Any:
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
        return learner

    @staticmethod
    def _fit_dr_fallback(*, y: np.ndarray, t: np.ndarray, X: np.ndarray) -> Any:
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
        return learner

    @staticmethod
    def _fit_causal_forest_dml(*, y: np.ndarray, t: np.ndarray, X: np.ndarray) -> Any:
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
        return learner

    @staticmethod
    def _fit_linear_dml_fallback(*, y: np.ndarray, t: np.ndarray, X: np.ndarray) -> Any:
        from econml.dml import LinearDML
        from sklearn.ensemble import RandomForestRegressor

        learner = LinearDML(
            model_y=RandomForestRegressor(
                n_estimators=100,
                random_state=42,
                max_depth=8,
            ),
            model_t=RandomForestRegressor(
                n_estimators=100,
                random_state=42,
                max_depth=8,
            ),
            random_state=42,
        )
        learner.fit(y, t, X=X)
        return learner

    def _predict_effect_for_context(
        self,
        *,
        fitted: _OutcomeFittedModel,
        x_curr: np.ndarray,
        t_curr: float,
    ) -> float:
        if fitted.is_binary_outcome:
            raw_effect = float(np.squeeze(fitted.model.effect(x_curr)))
            spread = max(1e-4, fitted.t_q75 - fitted.t_q25)
            normalized = (t_curr - fitted.t_median) / spread
            return raw_effect * float(np.clip(normalized, -1.5, 1.5))

        return float(
            np.squeeze(
                fitted.model.effect(
                    x_curr,
                    T0=np.array([fitted.t_median]),
                    T1=np.array([t_curr]),
                ),
            )
        )

    @staticmethod
    def _compose_score(
        *,
        y_mean: float,
        y_std: float,
        effect_value: float,
        row_count: int,
    ) -> float:
        spread = max(0.05, y_std)
        amplitude_cap = max(0.06, min(0.35, 2.0 * y_std + min(row_count / 500.0, 0.1)))
        bounded_effect = float(np.tanh(effect_value / spread) * amplitude_cap)
        return float(np.clip(y_mean + bounded_effect, 0.0, 1.0))

    @staticmethod
    def _estimate_confidence(
        *,
        y_std: float,
        row_count: int,
        fitted_with_fallback: bool,
        warning_count: int,
    ) -> float:
        row_bonus = min(row_count / 400.0, 0.2)
        warning_penalty = min(warning_count / 200.0, 0.15)
        fallback_penalty = 0.1 if fitted_with_fallback else 0.0
        score = 1.0 - y_std + row_bonus - warning_penalty - fallback_penalty
        return float(np.clip(score, 0.05, 0.95))

    def _load_calibration_from_model(self, model: CausalModelRegistry) -> None:
        metrics = model.metrics_json if isinstance(model.metrics_json, dict) else {}
        calibration = metrics.get("calibration")
        if not isinstance(calibration, dict):
            self._calibration_version = "none"
            self._calibration_by_outcome = {}
            return

        outcomes_payload = calibration.get("outcomes")
        if not isinstance(outcomes_payload, dict):
            self._calibration_version = "none"
            self._calibration_by_outcome = {}
            return

        parsed: dict[str, dict[str, Any]] = {}
        for outcome, payload in outcomes_payload.items():
            if not isinstance(payload, dict):
                continue
            if str(payload.get("status") or "").strip().lower() != "ok":
                continue
            method = str(payload.get("method") or "").strip().lower()
            params = payload.get("parameters")
            if method not in {"isotonic", "linear"} or not isinstance(params, dict):
                continue
            parsed[str(outcome)] = {"method": method, "parameters": params}

        self._calibration_by_outcome = parsed
        version = calibration.get("version")
        self._calibration_version = str(version).strip() if version else model.model_version

    def _apply_outcome_calibration(
        self,
        *,
        outcome: str,
        raw_score: float,
    ) -> tuple[float, str, bool]:
        payload = self._calibration_by_outcome.get(outcome)
        if not isinstance(payload, dict):
            return float(np.clip(raw_score, 0.0, 1.0)), "none", False

        method = str(payload.get("method") or "").strip().lower()
        params = payload.get("parameters")
        if not isinstance(params, dict):
            return float(np.clip(raw_score, 0.0, 1.0)), "none", False

        try:
            if method == "isotonic":
                x_vals = np.asarray(params.get("x_thresholds", []), dtype=float)
                y_vals = np.asarray(params.get("y_thresholds", []), dtype=float)
                if len(x_vals) < 2 or len(y_vals) < 2:
                    return float(np.clip(raw_score, 0.0, 1.0)), "none", False
                clipped_raw = float(np.clip(raw_score, float(np.min(x_vals)), float(np.max(x_vals))))
                calibrated = float(np.interp(clipped_raw, x_vals, y_vals))
                return float(np.clip(calibrated, 0.0, 1.0)), "isotonic", True
            if method == "linear":
                a = float(params.get("a", 1.0))
                b = float(params.get("b", 0.0))
                calibrated = a * float(raw_score) + b
                return float(np.clip(calibrated, 0.0, 1.0)), "linear", True
        except Exception:
            logger.warning("Failed to apply calibration for outcome=%s", outcome, exc_info=True)
            return float(np.clip(raw_score, 0.0, 1.0)), "none", False

        return float(np.clip(raw_score, 0.0, 1.0)), "none", False

    def _ensure_cache_for_model(self, model_version: str) -> None:
        if self._cache_model_version == model_version:
            return
        self._cache_model_version = model_version
        self._rows_cache.clear()
        self._fitted_cache.clear()
        self._warmup_applied = False
        self._invalidate_process_cache_except(model_version)

    @classmethod
    def _process_cache_key(
        cls,
        *,
        model_version: str,
        outcome: str,
        lookback_days: int | None = None,
    ) -> str:
        days = int(lookback_days if lookback_days is not None else 365)
        return f"{model_version}:{outcome}:{days}"

    @classmethod
    def _invalidate_process_cache_except(cls, model_version: str) -> None:
        prefix = f"{model_version}:"
        stale_keys = [key for key in cls._PROCESS_FITTED_CACHE if not key.startswith(prefix)]
        for key in stale_keys:
            cls._PROCESS_FITTED_CACHE.pop(key, None)
            cls._PROCESS_FIT_LOCKS.pop(key, None)
        cls._PROCESS_WARMED_MODELS = {
            key
            for key in cls._PROCESS_WARMED_MODELS
            if str(key).startswith(prefix)
        }

    async def _load_training_rows(self, outcome: str) -> list[dict[str, float]]:
        cached = self._rows_cache.get(outcome)
        if cached is not None:
            return cached

        cutoff = datetime.now(UTC) - timedelta(days=self._lookback_days)

        snapshot_stmt = select(CausalFeatureSnapshot).where(
            CausalFeatureSnapshot.observed_at >= cutoff,
        )
        event_stmt = select(CausalOutcomeEvent).where(
            CausalOutcomeEvent.outcome_name == outcome,
            CausalOutcomeEvent.observed_at >= cutoff,
        )

        snapshot_rows = (await self._session.execute(snapshot_stmt)).scalars().all()
        event_rows = (await self._session.execute(event_stmt)).scalars().all()

        if not snapshot_rows or not event_rows:
            self._rows_cache[outcome] = []
            return []

        latest_events: dict[tuple[str, str], CausalOutcomeEvent] = {}
        for event in event_rows:
            key = (str(event.student_id), str(event.school_id))
            prev = latest_events.get(key)
            if prev is None or event.observed_at > prev.observed_at:
                latest_events[key] = event

        rows: list[dict[str, float]] = []
        for snap in snapshot_rows:
            key = (str(snap.student_id), str(snap.school_id))
            event = latest_events.get(key)
            if event is None:
                continue

            payload = snap.feature_payload or {}
            feats = {}
            feats.update(payload.get("student_features", {}))
            feats.update(payload.get("school_features", {}))
            feats.update(payload.get("interaction_features", {}))
            feats = {str(k): float(v) for k, v in feats.items() if isinstance(v, (int, float))}
            feats["t"] = float(feats.get("school_selectivity", 0.5))
            feats["y"] = float(event.outcome_value)
            feats["label_confidence"] = float(event.label_confidence)
            feats["label_type"] = event.label_type
            rows.append(feats)

        self._rows_cache[outcome] = rows
        return rows

    @staticmethod
    def _context_with_interventions(
        ctx: CausalRequestContext,
        interventions: dict[str, float],
    ) -> CausalRequestContext:
        student = dict(ctx.student_features)
        school = dict(ctx.school_features)
        interaction = dict(ctx.interaction_features)
        for key, value in interventions.items():
            if key in student:
                student[key] = float(value)
            elif key in school:
                school[key] = float(value)
            else:
                interaction[key] = float(value)

        return CausalRequestContext(
            request_id=ctx.request_id,
            context=ctx.context,
            student_id=ctx.student_id,
            school_id=ctx.school_id,
            offer_id=ctx.offer_id,
            student_features=student,
            school_features=school,
            interaction_features=interaction,
            metadata=dict(ctx.metadata),
        )

    @staticmethod
    def _ensure_dependencies() -> None:
        try:
            import dowhy  # noqa: F401
            import econml  # noqa: F401
        except Exception as exc:  # pragma: no cover - runtime guard
            raise PyWhyUnavailableError("DoWhy/EconML dependencies are missing") from exc
