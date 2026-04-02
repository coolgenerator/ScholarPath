"""ScholarPath background tasks (Celery)."""

from scholarpath.tasks.celery_app import celery_app
from scholarpath.tasks.conflict_pipeline import run_conflict_detection
from scholarpath.tasks.causal_model import (
    causal_daily_gold_eval,
    causal_promote_model,
    causal_rollout_quality_gate,
    causal_shadow_audit,
    causal_train_full_graph,
)
from scholarpath.tasks.deep_search import run_deep_search
from scholarpath.tasks.advisor_memory import (
    advisor_memory_cleanup,
    advisor_memory_ingest,
    advisor_memory_ingest_message,
)

__all__ = [
    "celery_app",
    "run_deep_search",
    "run_conflict_detection",
    "causal_train_full_graph",
    "causal_promote_model",
    "causal_shadow_audit",
    "causal_rollout_quality_gate",
    "causal_daily_gold_eval",
    "advisor_memory_ingest",
    "advisor_memory_ingest_message",
    "advisor_memory_cleanup",
]
