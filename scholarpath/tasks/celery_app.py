"""Celery application configuration with Redis broker."""

from __future__ import annotations

from celery import Celery

from scholarpath.config import settings

celery_app = Celery(
    "scholarpath",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    # Route long-running tasks to a dedicated queue.
    task_routes={
        "scholarpath.tasks.deep_search.run_deep_search": {"queue": "deep_search"},
        "scholarpath.tasks.conflict_pipeline.run_conflict_detection": {"queue": "conflict"},
        "scholarpath.tasks.causal_model.causal_train_full_graph": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_promote_model": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_shadow_audit": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_rollout_quality_gate": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_daily_gold_eval": {"queue": "causal_train"},
        "scholarpath.tasks.advisor_memory.advisor_memory_ingest": {"queue": "advisor_memory"},
        "scholarpath.tasks.advisor_memory.advisor_memory_ingest_message": {"queue": "advisor_memory"},
        "scholarpath.tasks.advisor_memory.advisor_memory_cleanup": {"queue": "advisor_memory"},
    },
    beat_schedule={
        "causal-train-daily": {
            "task": "scholarpath.tasks.causal_model.causal_train_full_graph",
            "schedule": 24 * 60 * 60,
            "kwargs": {
                "profile": "high_quality",
                "bootstrap_iters": 300,
                "stability_threshold": 0.75,
                "lookback_days": 540,
                "bootstrap_parallelism": 4,
                "checkpoint_interval": 25,
                "resume_from_checkpoint": False,
                "early_stop_patience": 40,
                "discovery_sample_rows": 500,
                "discovery_max_features": 12,
                "min_rows_per_outcome": 200,
                "calibration_enabled": True,
                "warning_mode": "count_silent",
            },
        },
        "causal-rollout-quality-hourly": {
            "task": "scholarpath.tasks.causal_model.causal_rollout_quality_gate",
            "schedule": 60 * 60,
            "kwargs": {
                "sample_schools": 20,
                "contexts": 2,
                "history_window_runs": 24,
                "emit_alert": True,
            },
        },
        "causal-gold-eval-daily": {
            "task": "scholarpath.tasks.causal_model.causal_daily_gold_eval",
            "schedule": 24 * 60 * 60,
            "kwargs": {
                "judge_enabled": True,
                "judge_concurrency": 2,
                "max_rpm_total": 180,
                "sample_size": 40,
            },
        },
        "advisor-memory-cleanup-daily": {
            "task": "scholarpath.tasks.advisor_memory.advisor_memory_cleanup",
            "schedule": 24 * 60 * 60,
            "kwargs": {"batch_size": 2000},
        }
    },
)
