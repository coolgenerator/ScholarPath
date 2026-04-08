"""ScholarPath background tasks (Celery)."""

from scholarpath.tasks.celery_app import celery_app
from scholarpath.tasks.causal_model import (
    causal_build_dataset_version,
    causal_clean_and_judge_facts,
    causal_dataset_quality_gate,
    causal_daily_gold_eval,
    causal_ingest_admission_events,
    causal_ingest_common_app_trends,
    causal_ingest_ipeds_college_navigator,
    causal_ingest_ipeds_program_facts,
    causal_ingest_official_facts,
    causal_promote_model,
    causal_rollout_quality_gate,
    causal_shadow_audit,
    causal_train_from_dataset,
    causal_train_full_graph,
)
from scholarpath.tasks.conflict_pipeline import run_conflict_detection
from scholarpath.tasks.deep_search import run_deep_search

__all__ = [
    "celery_app",
    "run_deep_search",
    "run_conflict_detection",
    "causal_ingest_official_facts",
    "causal_ingest_ipeds_college_navigator",
    "causal_ingest_ipeds_program_facts",
    "causal_ingest_common_app_trends",
    "causal_ingest_admission_events",
    "causal_clean_and_judge_facts",
    "causal_build_dataset_version",
    "causal_dataset_quality_gate",
    "causal_train_from_dataset",
    "causal_train_full_graph",
    "causal_promote_model",
    "causal_shadow_audit",
    "causal_rollout_quality_gate",
    "causal_daily_gold_eval",
]
