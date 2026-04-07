from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone

import pytest

from scholarpath.causal_engine.training import _fit_calibration_from_alignment
from scholarpath.scripts import causal_staged_train


def _alignment_rows_for_outcome(outcome_name: str) -> tuple[list[tuple], list[tuple]]:
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot_rows: list[tuple] = []
    outcome_rows: list[tuple] = []
    for idx in range(5):
        student_id = f"s{idx}"
        school_id = "school-1"
        snapshot_rows.append(
            (
                student_id,
                school_id,
                ts,
                {
                    "student_features": {},
                    "school_features": {},
                    "interaction_features": {"idx": idx},
                },
            )
        )
        outcome_rows.append(
            (
                student_id,
                school_id,
                outcome_name,
                "true",
                0.2 + idx * 0.15,
                ts,
            )
        )
    return snapshot_rows, outcome_rows


def test_non_admission_calibration_guard_falls_back_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot_rows, outcome_rows = _alignment_rows_for_outcome("life_satisfaction")
    pred_values = [0.2, 0.3, 0.4, 0.5, 0.6]

    def _fake_scores(view):
        idx = int(view.interaction_features.get("idx", 0))
        return {
            "admission_probability": 0.5,
            "academic_outcome": 0.5,
            "career_outcome": 0.5,
            "life_satisfaction": pred_values[idx],
            "phd_probability": 0.5,
        }

    def _fake_fit(*, y_pred, y_true):
        return {"method": "linear", "a": 0.1, "b": 0.8}

    monkeypatch.setattr("scholarpath.causal_engine.training.compute_pywhy_raw_scores", _fake_scores)
    monkeypatch.setattr("scholarpath.causal_engine.training.fit_linear_calibration", _fake_fit)

    calibration, diagnostics = _fit_calibration_from_alignment(
        snapshot_rows=snapshot_rows,
        outcome_rows=outcome_rows,
        active_outcomes=["life_satisfaction"],
        robust_non_admission_guard=True,
    )
    assert calibration["life_satisfaction"]["method"] == "none"
    assert diagnostics["life_satisfaction"]["guard_triggered"] is True
    assert "std_ratio<0.45" in diagnostics["life_satisfaction"]["guard_reasons"]
    assert "|slope|_outside_[0.25,1.75]" in diagnostics["life_satisfaction"]["guard_reasons"]


def test_stage1_profiles_are_diversified() -> None:
    profiles = causal_staged_train._candidate_train_profiles(1, 3)
    assert [profile.candidate_id for profile in profiles] == ["s1c1", "s1c2", "s1c3"]
    assert profiles[0].calibration_enabled is True
    assert profiles[0].calibration_disabled_outcomes == []
    assert profiles[1].calibration_enabled is True
    assert profiles[1].calibration_disabled_outcomes == ["life_satisfaction", "phd_probability"]
    assert profiles[2].calibration_enabled is False


def test_stage4_data_gate_admission_rows_override_one_off() -> None:
    coverage = {
        "snapshots": 20_000,
        "counts": {
            "admission_probability": 14_500,
            "academic_outcome": 15_500,
            "career_outcome": 15_500,
            "life_satisfaction": 15_500,
            "phd_probability": 15_500,
        },
        "true_counts": {
            "admission_probability": 3_100,
            "academic_outcome": 700,
            "career_outcome": 700,
            "life_satisfaction": 700,
            "phd_probability": 700,
        },
        "anchor_counts": {
            "admission_probability": 3_100,
            "academic_outcome": 400,
            "career_outcome": 400,
            "life_satisfaction": 400,
            "phd_probability": 400,
        },
    }

    passed_default, reasons_default = causal_staged_train._check_stage_data_gate(4, coverage)
    assert passed_default is False
    assert "admission_probability_rows<15000" in reasons_default

    thresholds = copy.deepcopy(causal_staged_train._STAGE_DATA_THRESHOLDS)
    thresholds[4]["admission_rows"] = 14_000
    passed_override, reasons_override = causal_staged_train._check_stage_data_gate(
        4,
        coverage,
        stage_data_thresholds=thresholds,
    )
    assert passed_override is True
    assert reasons_override == []


def test_stage4_override_applies_only_to_stage4_and_keeps_defaults() -> None:
    thresholds, overrides = causal_staged_train._resolve_stage_data_thresholds(
        stage4_min_admission_rows=14_000
    )
    assert overrides == {"stage4_min_admission_rows": 14_000}
    assert thresholds[4]["admission_rows"] == 14_000
    assert thresholds[4]["per_outcome"] == 15_000
    assert "admission_rows" not in thresholds[3]
    assert thresholds[1]["per_outcome"] == causal_staged_train._STAGE_DATA_THRESHOLDS[1]["per_outcome"]


def test_has_previous_stage4_pass_requires_previous_strict_row(tmp_path) -> None:
    history_csv = tmp_path / "history.csv"
    causal_staged_train._append_history(
        history_csv,
        run_id="run-1",
        stage=4,
        passed=True,
        champion_model_version="m1",
        strict_stage4_gate=True,
        stage4_min_admission_rows=None,
    )
    assert causal_staged_train._has_previous_stage4_pass(history_csv, require_strict=True) is False

    causal_staged_train._append_history(
        history_csv,
        run_id="run-2",
        stage=4,
        passed=True,
        champion_model_version="m2",
        strict_stage4_gate=False,
        stage4_min_admission_rows=14_000,
    )
    assert causal_staged_train._has_previous_stage4_pass(history_csv, require_strict=True) is True


@pytest.mark.asyncio
async def test_stage1_run_restores_previous_active_model(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    args = argparse.Namespace(
        stage="1",
        train_candidates_per_stage=3,
        max_rpm_total=180,
        judge_concurrency=2,
        promote_on_final_pass=True,
        output_dir=str(tmp_path),
    )

    async def _fake_run_stage(**kwargs):
        return {
            "stage": 1,
            "coverage": {},
            "data_gate_passed": True,
            "data_gate_reasons": [],
            "candidates": [],
            "champion": None,
            "passed": False,
        }

    versions = iter(["legacy-active", "candidate-active", "legacy-active"])

    async def _fake_get_active_model_version():
        return next(versions)

    promote_calls: list[str] = []

    async def _fake_promote_model(*, model_version: str):
        promote_calls.append(model_version)
        return {"status": "ok", "model_version": model_version}

    monkeypatch.setattr(causal_staged_train, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(causal_staged_train, "_get_active_model_version", _fake_get_active_model_version)
    monkeypatch.setattr(causal_staged_train, "promote_model", _fake_promote_model)

    payload = await causal_staged_train._run(args)
    assert payload["active_restore_attempted"] is True
    assert payload["active_restore_status"] == "ok"
    assert payload["active_restored_to"] == "legacy-active"
    assert payload["active_model_after"] == "legacy-active"
    assert promote_calls == ["legacy-active"]


@pytest.mark.asyncio
async def test_stage4_run_reports_effective_data_gate_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    args = argparse.Namespace(
        stage="4",
        train_candidates_per_stage=3,
        max_rpm_total=180,
        judge_concurrency=2,
        promote_on_final_pass=False,
        output_dir=str(tmp_path),
        stage4_min_admission_rows=14_000,
    )

    captured_thresholds: dict[int, dict[str, int]] = {}

    async def _fake_run_stage(**kwargs):
        nonlocal captured_thresholds
        captured_thresholds = kwargs.get("stage_data_thresholds") or {}
        return {
            "stage": 4,
            "coverage": {},
            "effective_data_thresholds": dict(captured_thresholds.get(4, {})),
            "data_gate_passed": True,
            "data_gate_reasons": [],
            "candidates": [],
            "champion": None,
            "passed": False,
        }

    async def _fake_get_active_model_version():
        return None

    monkeypatch.setattr(causal_staged_train, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(causal_staged_train, "_get_active_model_version", _fake_get_active_model_version)

    payload = await causal_staged_train._run(args)
    assert payload["config"]["stage4_min_admission_rows"] == 14_000
    assert payload["overrides_applied"] == {"stage4_min_admission_rows": 14_000}
    assert payload["effective_stage_data_thresholds"]["4"]["admission_rows"] == 14_000
    assert payload["effective_stage_data_thresholds"]["4"]["per_outcome"] == 15_000
    assert payload["gate_results"]["stage_4"]["effective_data_thresholds"]["admission_rows"] == 14_000


@pytest.mark.asyncio
async def test_stage1_run_clears_active_when_no_previous_model(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    args = argparse.Namespace(
        stage="1",
        train_candidates_per_stage=3,
        max_rpm_total=180,
        judge_concurrency=2,
        promote_on_final_pass=True,
        output_dir=str(tmp_path),
    )

    async def _fake_run_stage(**kwargs):
        return {
            "stage": 1,
            "coverage": {},
            "data_gate_passed": True,
            "data_gate_reasons": [],
            "candidates": [],
            "champion": None,
            "passed": False,
        }

    versions = iter([None, "candidate-active", None])

    async def _fake_get_active_model_version():
        return next(versions)

    promote_calls: list[str] = []
    clear_calls: list[bool] = []

    async def _fake_promote_model(*, model_version: str):
        promote_calls.append(model_version)
        return {"status": "ok", "model_version": model_version}

    async def _fake_clear_active_model():
        clear_calls.append(True)
        return {"status": "ok", "cleared": 1}

    monkeypatch.setattr(causal_staged_train, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(causal_staged_train, "_get_active_model_version", _fake_get_active_model_version)
    monkeypatch.setattr(causal_staged_train, "promote_model", _fake_promote_model)
    monkeypatch.setattr(causal_staged_train, "_clear_active_model", _fake_clear_active_model)

    payload = await causal_staged_train._run(args)
    assert payload["active_restore_attempted"] is True
    assert payload["active_restore_status"] == "ok"
    assert payload["active_restored_to"] is None
    assert payload["active_model_after"] is None
    assert clear_calls == [True]
    assert promote_calls == []


@pytest.mark.asyncio
async def test_stage4_override_pass_does_not_promote_even_with_promote_flag(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    args = argparse.Namespace(
        stage="4",
        train_candidates_per_stage=3,
        max_rpm_total=180,
        judge_concurrency=2,
        promote_on_final_pass=True,
        output_dir=str(tmp_path),
        stage4_min_admission_rows=14_000,
    )

    async def _fake_run_stage(**kwargs):
        return {
            "stage": 4,
            "coverage": {},
            "effective_data_thresholds": {
                "snapshots": 15_000,
                "per_outcome": 15_000,
                "admission_true": 3_000,
                "other_true_or_anchor": 1_000,
                "admission_rows": 14_000,
            },
            "data_gate_passed": True,
            "data_gate_reasons": [],
            "candidates": [],
            "champion": {"model_version": "model-stage4"},
            "passed": True,
        }

    active_calls = iter(["active-before", "active-before"])

    async def _fake_get_active_model_version():
        return next(active_calls)

    promote_calls: list[str] = []

    async def _fake_promote_model(*, model_version: str):
        promote_calls.append(model_version)
        return {"status": "ok", "model_version": model_version}

    monkeypatch.setattr(causal_staged_train, "_run_stage", _fake_run_stage)
    monkeypatch.setattr(causal_staged_train, "_get_active_model_version", _fake_get_active_model_version)
    monkeypatch.setattr(causal_staged_train, "promote_model", _fake_promote_model)

    payload = await causal_staged_train._run(args)
    assert payload["promotion_decision"]["attempted"] is False
    assert "stage4_current_pass_not_strict" in payload["promotion_decision"]["reasons"]
    assert promote_calls == []
