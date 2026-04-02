"""Calibration helpers extracted from training pipeline."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

from scholarpath.causal_engine.warning_audit import (
    WarningAudit,
    capture_stage_warnings,
)


def fit_outcome_calibrators(
    *,
    frame: pd.DataFrame,
    enabled: bool,
    warning_mode: str,
    warning_audit: WarningAudit,
    outcome_names: tuple[str, ...],
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "version": None,
            "outcomes": {},
            "summary": {"fitted_count": 0, "improved_count": 0},
        }

    outcomes: dict[str, Any] = {}
    fitted_count = 0
    improved_count = 0
    for outcome in outcome_names:
        outcome_frame = frame[frame["outcome_name"] == outcome].copy()
        rows = int(len(outcome_frame))
        if rows < 30:
            outcomes[outcome] = {
                "status": "skipped",
                "reason": "insufficient_rows",
                "rows": rows,
            }
            continue
        if "school_selectivity" not in outcome_frame.columns:
            outcomes[outcome] = {
                "status": "skipped",
                "reason": "missing_treatment",
                "rows": rows,
            }
            continue

        y = np.asarray(outcome_frame["outcome_value"], dtype=float)
        t = np.asarray(outcome_frame["school_selectivity"], dtype=float)
        raw = compute_raw_scores_for_calibration(y=y, t=t)
        if len(raw) < 30:
            outcomes[outcome] = {
                "status": "skipped",
                "reason": "insufficient_raw_scores",
                "rows": rows,
            }
            continue

        indices = np.arange(len(raw))
        rng = np.random.default_rng(42)
        rng.shuffle(indices)
        split_at = max(10, int(round(len(indices) * 0.8)))
        split_at = min(split_at, len(indices) - 1)
        train_idx = indices[:split_at]
        hold_idx = indices[split_at:]
        if len(hold_idx) < 5:
            outcomes[outcome] = {
                "status": "skipped",
                "reason": "insufficient_holdout_rows",
                "rows": rows,
            }
            continue

        raw_train = raw[train_idx]
        y_train = y[train_idx]
        raw_hold = raw[hold_idx]
        y_hold = y[hold_idx]
        mae_before = float(np.mean(np.abs(raw_hold - y_hold)))

        try:
            if outcome in {"admission_probability", "phd_probability"}:
                outcome_payload = fit_isotonic_calibration(
                    outcome=outcome,
                    raw_train=raw_train,
                    y_train=y_train,
                    raw_hold=raw_hold,
                    y_hold=y_hold,
                    warning_mode=warning_mode,
                    warning_audit=warning_audit,
                )
            else:
                outcome_payload = fit_linear_calibration(
                    outcome=outcome,
                    raw_train=raw_train,
                    y_train=y_train,
                    raw_hold=raw_hold,
                    y_hold=y_hold,
                    warning_mode=warning_mode,
                    warning_audit=warning_audit,
                )
        except Exception as exc:
            outcomes[outcome] = {
                "status": "failed",
                "rows": rows,
                "error": str(exc),
            }
            continue

        fitted_count += 1
        if float(outcome_payload.get("mae_after", mae_before)) <= mae_before:
            improved_count += 1
        outcomes[outcome] = {
            "status": "ok",
            "rows": rows,
            "holdout_rows": int(len(hold_idx)),
            "mae_before": round(mae_before, 6),
            **outcome_payload,
        }

    return {
        "enabled": True,
        "version": datetime.now(UTC).strftime("%Y%m%d-%H%M%S"),
        "outcomes": outcomes,
        "summary": {
            "fitted_count": int(fitted_count),
            "improved_count": int(improved_count),
            "total_outcomes": len(outcome_names),
        },
    }


def compute_raw_scores_for_calibration(
    *,
    y: np.ndarray,
    t: np.ndarray,
) -> np.ndarray:
    y_mean = float(np.mean(y))
    y_std = float(np.std(y))
    t_median = float(np.median(t))
    t_q25 = float(np.quantile(t, 0.25))
    t_q75 = float(np.quantile(t, 0.75))
    spread = max(1e-4, t_q75 - t_q25)
    normalized = np.clip((t - t_median) / spread, -1.5, 1.5)
    return np.asarray(
        [
            compose_raw_score(
                y_mean=y_mean,
                y_std=y_std,
                effect_value=float(value),
                row_count=len(y),
            )
            for value in normalized
        ],
        dtype=float,
    )


def compose_raw_score(
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


def fit_isotonic_calibration(
    *,
    outcome: str,
    raw_train: np.ndarray,
    y_train: np.ndarray,
    raw_hold: np.ndarray,
    y_hold: np.ndarray,
    warning_mode: str,
    warning_audit: WarningAudit,
) -> dict[str, Any]:
    from sklearn.isotonic import IsotonicRegression

    with capture_stage_warnings(
        stage=f"training.calibration.fit.{outcome}.isotonic",
        warning_mode=warning_mode,
        audit=warning_audit,
    ):
        iso = IsotonicRegression(out_of_bounds="clip")
        iso.fit(raw_train, y_train)
    preds = np.asarray(iso.predict(raw_hold), dtype=float)
    mae_after = float(np.mean(np.abs(np.clip(preds, 0.0, 1.0) - y_hold)))
    x_thresholds = [round(float(v), 6) for v in np.asarray(iso.X_thresholds_, dtype=float)]
    y_thresholds = [round(float(v), 6) for v in np.asarray(iso.y_thresholds_, dtype=float)]
    return {
        "method": "isotonic",
        "mae_after": round(mae_after, 6),
        "parameters": {
            "x_thresholds": x_thresholds,
            "y_thresholds": y_thresholds,
        },
    }


def fit_linear_calibration(
    *,
    outcome: str,
    raw_train: np.ndarray,
    y_train: np.ndarray,
    raw_hold: np.ndarray,
    y_hold: np.ndarray,
    warning_mode: str,
    warning_audit: WarningAudit,
) -> dict[str, Any]:
    with capture_stage_warnings(
        stage=f"training.calibration.fit.{outcome}.linear",
        warning_mode=warning_mode,
        audit=warning_audit,
    ):
        design = np.column_stack([raw_train, np.ones(len(raw_train), dtype=float)])
        coeffs, *_ = np.linalg.lstsq(design, y_train, rcond=None)
    a = float(coeffs[0])
    b = float(coeffs[1])
    preds = np.clip(a * raw_hold + b, 0.0, 1.0)
    mae_after = float(np.mean(np.abs(preds - y_hold)))
    return {
        "method": "linear",
        "mae_after": round(mae_after, 6),
        "parameters": {
            "a": round(a, 8),
            "b": round(b, 8),
        },
    }

