from __future__ import annotations

import pytest

pytest.importorskip("celery")
from scholarpath.tasks.celery_app import celery_app


def test_celery_routes_include_causal_eval_tasks() -> None:
    routes = celery_app.conf.task_routes or {}
    assert "scholarpath.tasks.causal_model.causal_rollout_quality_gate" in routes
    assert "scholarpath.tasks.causal_model.causal_daily_gold_eval" in routes


def test_celery_beat_includes_rollout_and_gold_eval() -> None:
    beat = celery_app.conf.beat_schedule or {}
    assert "causal-rollout-quality-hourly" in beat
    assert "causal-gold-eval-daily" in beat
    assert beat["causal-rollout-quality-hourly"]["kwargs"]["emit_alert"] is True
    assert beat["causal-gold-eval-daily"]["kwargs"]["max_rpm_total"] == 180
