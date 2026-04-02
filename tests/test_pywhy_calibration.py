from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scholarpath.causal_engine.pywhy_engine import PyWhyCausalEngine, _OutcomeFittedModel
from scholarpath.causal_engine.types import CausalRequestContext


def test_apply_outcome_calibration_isotonic_and_linear() -> None:
    engine = PyWhyCausalEngine(session=SimpleNamespace())
    engine._calibration_by_outcome = {
        "admission_probability": {
            "method": "isotonic",
            "parameters": {
                "x_thresholds": [0.0, 0.5, 1.0],
                "y_thresholds": [0.1, 0.6, 0.9],
            },
        },
        "career_outcome": {
            "method": "linear",
            "parameters": {"a": 0.9, "b": 0.05},
        },
    }

    score_iso, method_iso, applied_iso = engine._apply_outcome_calibration(
        outcome="admission_probability",
        raw_score=0.5,
    )
    score_linear, method_linear, applied_linear = engine._apply_outcome_calibration(
        outcome="career_outcome",
        raw_score=0.8,
    )

    assert applied_iso is True
    assert method_iso == "isotonic"
    assert score_iso == pytest.approx(0.6, abs=1e-6)
    assert applied_linear is True
    assert method_linear == "linear"
    assert score_linear == pytest.approx(0.77, abs=1e-6)


@pytest.mark.asyncio
async def test_estimate_metadata_includes_calibration_keys(monkeypatch) -> None:
    engine = PyWhyCausalEngine(session=SimpleNamespace())
    model = SimpleNamespace(model_version="pywhy-test", status="active")

    monkeypatch.setattr(engine, "_ensure_dependencies", lambda: None)

    async def fake_resolve_model():
        return model

    async def fake_estimate_single_outcome(ctx, outcome):
        return (
            0.7,
            0.8,
            ("proxy", 0.75),
            {
                "estimator_name": "causal_forest_dml",
                "fitted_with_fallback": False,
                "row_count": 120,
                "warnings_total": 0,
                "warnings_by_stage": {},
                "diagnostics": {},
                "calibration_applied": True,
                "calibration_method": "linear",
            },
        )

    monkeypatch.setattr(engine, "_resolve_model", fake_resolve_model)
    monkeypatch.setattr(engine, "_ensure_cache_for_model", lambda _version: None)
    monkeypatch.setattr(engine, "_estimate_single_outcome", fake_estimate_single_outcome)
    engine._calibration_version = "20260402-010000"

    ctx = CausalRequestContext(
        request_id="req-1",
        context="test",
        student_id=uuid.uuid4(),
        school_id=uuid.uuid4(),
        offer_id=None,
        student_features={"student_gpa_norm": 0.8},
        school_features={"school_selectivity": 0.7},
        interaction_features={"academic_match": 0.75},
    )

    result = await engine.estimate(ctx, ["admission_probability"])
    assert result.metadata["calibration_applied"] is True
    assert result.metadata["calibration_method_by_outcome"]["admission_probability"] == "linear"
    assert result.metadata["calibration_version"] == "20260402-010000"
    assert "cache_hit_by_outcome" in result.metadata
    assert "warmup_applied" in result.metadata
    assert "fit_reused" in result.metadata


@pytest.mark.asyncio
async def test_process_fit_cache_reused_across_engine_instances(monkeypatch) -> None:
    PyWhyCausalEngine._PROCESS_FITTED_CACHE.clear()
    PyWhyCausalEngine._PROCESS_FIT_LOCKS.clear()
    PyWhyCausalEngine._PROCESS_WARMED_MODELS.clear()

    dummy_fitted = _OutcomeFittedModel(
        outcome="admission_probability",
        estimator_name="causal_forest_dml",
        fitted_with_fallback=False,
        model=SimpleNamespace(),
        is_binary_outcome=False,
        x_names=["x1"],
        y_mean=0.5,
        y_std=0.1,
        t_median=0.5,
        t_q25=0.4,
        t_q75=0.6,
        label_type="proxy",
        label_confidence=0.8,
        row_count=100,
        warnings_total=0,
        warnings_by_stage={},
        diagnostics={},
    )

    engine1 = PyWhyCausalEngine(session=SimpleNamespace(), lookback_days=540)
    engine1._active_model_version = "m1"
    engine1._ensure_cache_for_model("m1")
    monkeypatch.setattr(engine1, "_fit_outcome_model", AsyncMock(return_value=dummy_fitted))

    fitted1, cache_hit1 = await engine1._get_or_fit_outcome_model("admission_probability")
    assert cache_hit1 is False
    assert fitted1 is dummy_fitted

    engine2 = PyWhyCausalEngine(session=SimpleNamespace(), lookback_days=540)
    engine2._active_model_version = "m1"
    engine2._ensure_cache_for_model("m1")
    fit_mock = AsyncMock(return_value=dummy_fitted)
    monkeypatch.setattr(engine2, "_fit_outcome_model", fit_mock)

    fitted2, cache_hit2 = await engine2._get_or_fit_outcome_model("admission_probability")
    assert cache_hit2 is True
    assert fitted2 is dummy_fitted
    fit_mock.assert_not_called()


@pytest.mark.asyncio
async def test_process_fit_cache_invalidates_on_model_switch(monkeypatch) -> None:
    PyWhyCausalEngine._PROCESS_FITTED_CACHE.clear()
    PyWhyCausalEngine._PROCESS_FIT_LOCKS.clear()
    PyWhyCausalEngine._PROCESS_WARMED_MODELS.clear()

    dummy_fitted = _OutcomeFittedModel(
        outcome="admission_probability",
        estimator_name="causal_forest_dml",
        fitted_with_fallback=False,
        model=SimpleNamespace(),
        is_binary_outcome=False,
        x_names=["x1"],
        y_mean=0.5,
        y_std=0.1,
        t_median=0.5,
        t_q25=0.4,
        t_q75=0.6,
        label_type="proxy",
        label_confidence=0.8,
        row_count=100,
        warnings_total=0,
        warnings_by_stage={},
        diagnostics={},
    )

    engine1 = PyWhyCausalEngine(session=SimpleNamespace(), lookback_days=540)
    engine1._active_model_version = "m1"
    engine1._ensure_cache_for_model("m1")
    monkeypatch.setattr(engine1, "_fit_outcome_model", AsyncMock(return_value=dummy_fitted))
    _, first_hit = await engine1._get_or_fit_outcome_model("admission_probability")
    assert first_hit is False

    # Switching model should invalidate stale process cache entries.
    engine_switch = PyWhyCausalEngine(session=SimpleNamespace(), lookback_days=540)
    engine_switch._active_model_version = "m2"
    engine_switch._ensure_cache_for_model("m2")

    engine2 = PyWhyCausalEngine(session=SimpleNamespace(), lookback_days=540)
    engine2._active_model_version = "m1"
    engine2._ensure_cache_for_model("m1")
    fit_mock = AsyncMock(return_value=dummy_fitted)
    monkeypatch.setattr(engine2, "_fit_outcome_model", fit_mock)
    _, second_hit = await engine2._get_or_fit_outcome_model("admission_probability")
    assert second_hit is False
    fit_mock.assert_called_once()


@pytest.mark.asyncio
async def test_warmup_marks_first_use_and_skips_after_warmed(monkeypatch) -> None:
    PyWhyCausalEngine._PROCESS_FITTED_CACHE.clear()
    PyWhyCausalEngine._PROCESS_FIT_LOCKS.clear()
    PyWhyCausalEngine._PROCESS_WARMED_MODELS.clear()

    engine = PyWhyCausalEngine(session=SimpleNamespace(), lookback_days=540)
    model = SimpleNamespace(model_version="m1", status="active")

    monkeypatch.setattr(engine, "_ensure_dependencies", lambda: None)

    async def fake_resolve_model():
        return model

    async def fake_get_or_fit(_outcome: str):
        return (
            _OutcomeFittedModel(
                outcome="admission_probability",
                estimator_name="causal_forest_dml",
                fitted_with_fallback=False,
                model=SimpleNamespace(),
                is_binary_outcome=False,
                x_names=["x1"],
                y_mean=0.5,
                y_std=0.1,
                t_median=0.5,
                t_q25=0.4,
                t_q75=0.6,
                label_type="proxy",
                label_confidence=0.8,
                row_count=100,
                warnings_total=0,
                warnings_by_stage={},
                diagnostics={},
            ),
            False,
        )

    monkeypatch.setattr(engine, "_resolve_model", fake_resolve_model)
    monkeypatch.setattr(engine, "_get_or_fit_outcome_model", fake_get_or_fit)

    await engine.warmup(["admission_probability"])
    assert engine._warmup_applied is True

    await engine.warmup(["admission_probability"])
    assert engine._warmup_applied is False
