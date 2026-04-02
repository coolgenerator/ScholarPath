from __future__ import annotations

from types import SimpleNamespace

from scholarpath.evals.causal_rollout_quality import (
    RolloutGateMetrics,
    RolloutGateThresholds,
    _build_contexts,
    _build_metrics,
    evaluate_rollout_gate,
)


def test_evaluate_rollout_gate_good() -> None:
    metrics = RolloutGateMetrics(
        sample_rows=200,
        pywhy_primary_rows=100,
        legacy_primary_rows=100,
        pywhy_primary_ratio=0.5,
        fallback_rate=0.0,
        mae_all=0.3,
        mae_pywhy_primary=0.31,
        mae_legacy_primary=0.3,
        mae_gap_pywhy_minus_legacy=0.01,
        dual_arm_available=True,
        abs_diff_p95=0.6,
        abs_diff_max=0.9,
        by_outcome_mae={"admission_probability": 0.12},
    )
    decision = evaluate_rollout_gate(
        metrics=metrics,
        target_percent=50,
        thresholds=RolloutGateThresholds(),
    )
    assert decision.passed is True
    assert decision.status == "good"
    assert decision.reasons == []


def test_evaluate_rollout_gate_bad_on_ratio_and_fallback() -> None:
    metrics = RolloutGateMetrics(
        sample_rows=120,
        pywhy_primary_rows=20,
        legacy_primary_rows=100,
        pywhy_primary_ratio=0.1667,
        fallback_rate=0.05,
        mae_all=0.3,
        mae_pywhy_primary=0.34,
        mae_legacy_primary=0.3,
        mae_gap_pywhy_minus_legacy=0.04,
        dual_arm_available=True,
        abs_diff_p95=0.65,
        abs_diff_max=0.95,
        by_outcome_mae={"admission_probability": 0.14},
    )
    decision = evaluate_rollout_gate(
        metrics=metrics,
        target_percent=50,
        thresholds=RolloutGateThresholds(),
    )
    assert decision.passed is False
    assert decision.status == "bad"
    assert len(decision.reasons) >= 2


def test_build_metrics_counts_modes_and_mae() -> None:
    rows = [
        SimpleNamespace(
            engine_mode="shadow_pywhy",
            fallback_used=False,
            diff_scores={"admission_probability": 0.1, "career_outcome": -0.2},
        ),
        SimpleNamespace(
            engine_mode="shadow_legacy",
            fallback_used=True,
            diff_scores={"admission_probability": -0.3, "career_outcome": 0.2},
        ),
        SimpleNamespace(
            engine_mode="shadow",
            fallback_used=False,
            diff_scores={"admission_probability": 0.0},
        ),
    ]
    metrics = _build_metrics(rows)  # type: ignore[arg-type]
    assert metrics.sample_rows == 3
    assert metrics.pywhy_primary_rows == 1
    assert metrics.legacy_primary_rows == 2
    assert metrics.dual_arm_available is True
    assert metrics.fallback_rate == round(1 / 3, 6)
    assert metrics.abs_diff_max == 0.3
    assert "admission_probability" in metrics.by_outcome_mae


def test_build_contexts_respects_varchar_limit() -> None:
    contexts = _build_contexts(
        context_prefix="rollout50_qgate_very_long_prefix_need_trim_for_db",
        context_count=3,
    )
    assert len(contexts) == 3
    assert len(set(contexts)) == 3
    assert all(len(item) <= 40 for item in contexts)


def test_evaluate_rollout_gate_single_arm_watch_not_bad() -> None:
    metrics = RolloutGateMetrics(
        sample_rows=128,
        pywhy_primary_rows=128,
        legacy_primary_rows=0,
        pywhy_primary_ratio=1.0,
        fallback_rate=0.0,
        mae_all=0.37,
        mae_pywhy_primary=0.37,
        mae_legacy_primary=0.0,
        mae_gap_pywhy_minus_legacy=0.0,
        dual_arm_available=False,
        abs_diff_p95=0.7,
        abs_diff_max=0.85,
        by_outcome_mae={},
    )
    decision = evaluate_rollout_gate(
        metrics=metrics,
        target_percent=100,
        thresholds=RolloutGateThresholds(),
    )
    assert decision.passed is True
    assert decision.status == "watch"


def test_evaluate_rollout_gate_trend_worsening_turns_bad() -> None:
    metrics = RolloutGateMetrics(
        sample_rows=160,
        pywhy_primary_rows=160,
        legacy_primary_rows=0,
        pywhy_primary_ratio=1.0,
        fallback_rate=0.015,
        mae_all=0.34,
        mae_pywhy_primary=0.34,
        mae_legacy_primary=0.0,
        mae_gap_pywhy_minus_legacy=0.0,
        dual_arm_available=False,
        abs_diff_p95=0.72,
        abs_diff_max=0.9,
        by_outcome_mae={"admission_probability": 0.19},
    )
    decision = evaluate_rollout_gate(
        metrics=metrics,
        target_percent=100,
        thresholds=RolloutGateThresholds(),
        trend={
            "fallback_rate_delta": 0.015,
            "abs_diff_p95_delta": 0.05,
            "by_outcome_mae_worsening": ["admission_probability"],
        },
    )
    assert decision.passed is False
    assert decision.status == "bad"
    assert any("trend" in reason for reason in decision.reasons)
