from __future__ import annotations

from scholarpath.evals.advisor_orchestrator_live import _evaluate_reedit
from scholarpath.evals.advisor_orchestrator_selection import ReeditEvalCase


def test_reedit_metrics_are_strict_and_stable():
    rows = [
        ReeditEvalCase(
            case_id="middle_01",
            category="middle",
            original_turn="o1",
            edited_turn="e1",
        ),
        ReeditEvalCase(
            case_id="invalid_01",
            category="invalid",
            original_turn="o2",
            edited_turn="e2",
        ),
    ]
    metrics = _evaluate_reedit(rows)
    assert metrics.case_count == 2
    assert metrics.overwrite_success_rate == 1.0
    assert metrics.truncation_correct_rate == 1.0
    assert metrics.history_consistency_rate == 1.0
