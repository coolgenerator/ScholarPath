from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scholarpath.causal_engine.training import (
    _ensure_outcome_coverage,
    _fit_outcome_calibrators,
)
from scholarpath.causal_engine.warning_audit import WarningAudit


def _build_training_frame(rows_per_outcome: int) -> pd.DataFrame:
    outcomes = [
        "admission_probability",
        "academic_outcome",
        "career_outcome",
        "life_satisfaction",
        "phd_probability",
    ]
    rows = []
    rng = np.random.default_rng(42)
    for outcome in outcomes:
        for _ in range(rows_per_outcome):
            value = float(np.clip(rng.normal(0.65, 0.12), 0.0, 1.0))
            rows.append(
                {
                    "outcome_name": outcome,
                    "outcome_value": value,
                    "school_selectivity": float(np.clip(rng.normal(0.6, 0.15), 0.0, 1.0)),
                    "label_type": "true" if rng.random() > 0.3 else "proxy",
                }
            )
    return pd.DataFrame(rows)


def test_ensure_outcome_coverage_fails_precondition() -> None:
    frame = _build_training_frame(rows_per_outcome=20)
    with pytest.raises(ValueError, match="failed_precondition"):
        _ensure_outcome_coverage(frame=frame, min_rows_per_outcome=50)


def test_fit_outcome_calibrators_outputs_methods() -> None:
    frame = _build_training_frame(rows_per_outcome=60)
    payload = _fit_outcome_calibrators(
        frame=frame,
        enabled=True,
        warning_mode="count_silent",
        warning_audit=WarningAudit(),
    )
    assert payload["enabled"] is True
    outcomes = payload["outcomes"]
    assert outcomes["admission_probability"]["method"] == "isotonic"
    assert outcomes["phd_probability"]["method"] == "isotonic"
    assert outcomes["academic_outcome"]["method"] == "linear"
    assert outcomes["career_outcome"]["method"] == "linear"
    assert outcomes["life_satisfaction"]["method"] == "linear"

