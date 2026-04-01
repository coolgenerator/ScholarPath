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
    },
)
