"""Celery application configuration with Redis broker."""

from __future__ import annotations

from celery import Celery
from celery.schedules import crontab
from celery.signals import worker_process_init

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
    imports=(
        "scholarpath.tasks.deep_search",
        "scholarpath.tasks.conflict_pipeline",
        "scholarpath.tasks.causal_model",
    ),
    task_routes={
        "scholarpath.tasks.deep_search.run_deep_search": {"queue": "deep_search"},
        "scholarpath.tasks.conflict_pipeline.run_conflict_detection": {"queue": "conflict"},
        "scholarpath.tasks.causal_model.causal_ingest_official_facts": {"queue": "celery"},
        "scholarpath.tasks.causal_model.causal_ingest_ipeds_college_navigator": {"queue": "celery"},
        "scholarpath.tasks.causal_model.causal_ingest_ipeds_program_facts": {"queue": "celery"},
        "scholarpath.tasks.causal_model.causal_ingest_common_app_trends": {"queue": "celery"},
        "scholarpath.tasks.causal_model.causal_ingest_admission_events": {"queue": "celery"},
        "scholarpath.tasks.causal_model.causal_clean_and_judge_facts": {"queue": "celery"},
        "scholarpath.tasks.causal_model.causal_build_dataset_version": {"queue": "celery"},
        "scholarpath.tasks.causal_model.causal_dataset_quality_gate": {"queue": "celery"},
        "scholarpath.tasks.causal_model.causal_train_from_dataset": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_train_full_graph": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_promote_model": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_shadow_audit": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_rollout_quality_gate": {"queue": "causal_train"},
        "scholarpath.tasks.causal_model.causal_daily_gold_eval": {"queue": "causal_train"},
    },
    beat_schedule={
        "causal-rollout-quality-hourly": {
            "task": "scholarpath.tasks.causal_model.causal_rollout_quality_gate",
            "schedule": crontab(minute=0),
            "args": (24, 100, 3),
            "options": {"queue": "causal_train"},
        },
        "causal-daily-gold-eval": {
            "task": "scholarpath.tasks.causal_model.causal_daily_gold_eval",
            "schedule": crontab(hour=3, minute=0),
            "kwargs": {"sample_size": 40, "judge_enabled": True},
            "options": {"queue": "causal_train"},
        },
    },
)


@worker_process_init.connect
def _reset_db_pools_after_fork(**_: object) -> None:
    """Avoid inheriting parent asyncpg pool state in prefork workers."""
    try:
        from scholarpath.db.session import engine

        engine.sync_engine.dispose(close=False)
    except Exception:
        # Best-effort guard; worker can still function with default behavior.
        pass

    try:
        from scholarpath.tasks.deep_search import _reset_worker_event_loop

        _reset_worker_event_loop()
    except Exception:
        # Best-effort guard; deep-search task can still start its own loop lazily.
        return
