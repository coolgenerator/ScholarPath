"""Proxy outcome label builders for causal training."""

from __future__ import annotations

from typing import Any


def build_proxy_outcomes(*, school: Any) -> dict[str, float]:
    """Build bounded proxy outcomes from school-level public metrics."""
    grad = _clip01(getattr(school, "graduation_rate_4yr", None))
    earnings = _clip01((getattr(school, "avg_earnings_10yr", None) or 0.0) / 120000.0)
    affordability = 1.0 - _clip01((getattr(school, "avg_net_price", None) or 0.0) / 90000.0)
    selectivity = 1.0 - _clip01(getattr(school, "acceptance_rate", None))

    academic = _clip01(0.65 * grad + 0.35 * selectivity)
    career = _clip01(0.55 * earnings + 0.45 * grad)
    life = _clip01(0.5 * affordability + 0.3 * grad + 0.2 * _clip01(getattr(school, "student_faculty_ratio", None)))
    phd = _clip01(0.5 * academic + 0.3 * selectivity + 0.2 * career)
    admission = _clip01(1.0 - getattr(school, "acceptance_rate", 0.5))

    return {
        "admission_probability": admission,
        "academic_outcome": academic,
        "career_outcome": career,
        "life_satisfaction": life,
        "phd_probability": phd,
    }


def _clip01(value: float | None) -> float:
    if value is None:
        return 0.0
    return max(0.0, min(1.0, float(value)))
