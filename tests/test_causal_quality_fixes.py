from __future__ import annotations

from datetime import datetime, timezone

from scholarpath.causal_engine.training import _fit_calibration_from_alignment
from scholarpath.evals.causal_gold_live import _grade_status
from scholarpath.scripts.causal_staged_train import _stage_pass


def _build_rows() -> tuple[list[tuple], list[tuple]]:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot_rows: list[tuple] = []
    outcome_rows: list[tuple] = []
    for i in range(5):
        student_id = f"s{i}"
        school_id = "school-1"
        snapshot_rows.append(
            (
                student_id,
                school_id,
                ts,
                {
                    "idx": i,
                    "student_features": {},
                    "school_features": {},
                    "interaction_features": {"idx": i},
                },
            )
        )
    return snapshot_rows, outcome_rows


def test_alignment_calibration_fixes_admission_direction_and_phd_mae(monkeypatch) -> None:
    snapshot_rows, outcome_rows = _build_rows()
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    admission_pred = [0.20, 0.30, 0.40, 0.50, 0.60]
    admission_truth = [0.80, 0.70, 0.60, 0.50, 0.40]

    phd_pred = [
        0.4014424407738394,
        0.23866755335481027,
        0.40472642867003705,
        0.17373549243748376,
        0.2490035705446645,
    ]
    phd_truth = [
        0.9308861142650783,
        0.11234236537706255,
        0.4356099501975208,
        0.41870626709019687,
        0.23106240898706099,
    ]

    for i in range(5):
        student_id = f"s{i}"
        school_id = "school-1"
        outcome_rows.append(
            (student_id, school_id, "admission_probability", "true", admission_truth[i], ts)
        )
        outcome_rows.append((student_id, school_id, "phd_probability", "proxy", phd_truth[i], ts))

    def _fake_scores(view):
        idx = int(view.interaction_features.get("idx", 0))
        return {
            "admission_probability": admission_pred[idx],
            "academic_outcome": 0.5,
            "career_outcome": 0.5,
            "life_satisfaction": 0.5,
            "phd_probability": phd_pred[idx],
        }

    monkeypatch.setattr("scholarpath.causal_engine.training.compute_pywhy_raw_scores", _fake_scores)

    calibration, diagnostics = _fit_calibration_from_alignment(
        snapshot_rows=snapshot_rows,
        outcome_rows=outcome_rows,
    )

    assert calibration["admission_probability"]["method"] == "linear"
    assert diagnostics["admission_probability"]["spearman_calibrated"] > 0.0
    assert diagnostics["phd_probability"]["mae_calibrated"] <= diagnostics["phd_probability"]["mae_raw"]


def test_grade_status_uses_pywhy_judge_score() -> None:
    assert (
        _grade_status(
            judge_score_pywhy=82.0,
            pywhy_mae=0.10,
            legacy_mae=0.20,
            pywhy_field_pass_rate=0.65,
            rate_limit_error_count=0,
        )
        == "good"
    )
    assert (
        _grade_status(
            judge_score_pywhy=74.0,
            pywhy_mae=0.21,
            legacy_mae=0.20,
            pywhy_field_pass_rate=0.52,
            rate_limit_error_count=0,
        )
        == "watch"
    )


def test_stage_gate_uses_pywhy_score_over_legacy_overall() -> None:
    report = {
        "metrics": {
            "judge_overall_score": 40.0,
            "judge_score_pywhy": 82.0,
            "mae_overall_pywhy": 0.10,
            "mae_overall_legacy": 0.20,
            "rate_limit_error_count": 0,
        },
        "pywhy_pass": {
            "judge_field_pass_rate": 0.65,
            "fallback_rate": 0.0,
        },
    }
    passed, reasons = _stage_pass(4, report)
    assert passed is True
    assert reasons == []
