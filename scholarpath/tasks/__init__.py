"""ScholarPath background tasks (Celery)."""

from scholarpath.tasks.celery_app import celery_app
from scholarpath.tasks.conflict_pipeline import run_conflict_detection
from scholarpath.tasks.deep_search import run_deep_search

__all__ = [
    "celery_app",
    "run_deep_search",
    "run_conflict_detection",
]
