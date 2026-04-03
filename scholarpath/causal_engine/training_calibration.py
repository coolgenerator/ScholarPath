"""Outcome-level post calibration utilities."""

from __future__ import annotations

from typing import Any


def fit_linear_calibration(
    *,
    y_pred: list[float],
    y_true: list[float],
) -> dict[str, Any]:
    """Fit y' = clip(a*y + b, 0, 1) with least squares."""
    if not y_pred or not y_true or len(y_pred) != len(y_true):
        return {"method": "none"}
    n = len(y_pred)
    mean_x = sum(y_pred) / n
    mean_y = sum(y_true) / n
    var_x = sum((x - mean_x) ** 2 for x in y_pred)
    if var_x <= 1e-12:
        return {"method": "none"}
    cov_xy = sum((x - mean_x) * (y - mean_y) for x, y in zip(y_pred, y_true))
    a = cov_xy / var_x
    b = mean_y - a * mean_x
    return {"method": "linear", "a": float(a), "b": float(b)}


def apply_calibration(value: float, cfg: dict[str, Any]) -> float:
    method = str(cfg.get("method") or "none").strip().lower()
    if method != "linear":
        return _clip01(value)
    a = float(cfg.get("a", 1.0))
    b = float(cfg.get("b", 0.0))
    return _clip01(a * value + b)


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))
