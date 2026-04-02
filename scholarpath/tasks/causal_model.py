"""Celery tasks for PyWhy causal model lifecycle."""

from __future__ import annotations

import logging
from typing import Any

from scholarpath.evals.causal_gold_live import run_causal_gold_eval
from scholarpath.evals.causal_rollout_quality import run_causal_rollout_quality_gate
from scholarpath.causal_engine.training import (
    promote_model,
    shadow_audit,
    train_full_graph_model,
)
from scholarpath.tasks.async_runtime import run_async
from scholarpath.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="scholarpath.tasks.causal_model.causal_train_full_graph",
    bind=True,
    max_retries=1,
    default_retry_delay=60,
)
def causal_train_full_graph(
    self: Any,
    model_version: str | None = None,
    profile: str = "high_quality",
    bootstrap_iters: int = 100,
    stability_threshold: float = 0.7,
    lookback_days: int = 365,
    bootstrap_parallelism: int = 1,
    checkpoint_interval: int = 25,
    resume_from_checkpoint: bool = False,
    early_stop_patience: int = 0,
    discovery_sample_rows: int = 300,
    discovery_max_features: int = 12,
    min_rows_per_outcome: int = 200,
    calibration_enabled: bool = True,
    warning_mode: str = "count_silent",
) -> dict[str, Any]:
    """Train full-graph causal model with PC+GES consensus."""
    try:
        return run_async(
            _train_async(
                model_version=model_version,
                profile=profile,
                bootstrap_iters=bootstrap_iters,
                stability_threshold=stability_threshold,
                lookback_days=lookback_days,
                bootstrap_parallelism=bootstrap_parallelism,
                checkpoint_interval=checkpoint_interval,
                resume_from_checkpoint=resume_from_checkpoint,
                early_stop_patience=early_stop_patience,
                discovery_sample_rows=discovery_sample_rows,
                discovery_max_features=discovery_max_features,
                min_rows_per_outcome=min_rows_per_outcome,
                calibration_enabled=calibration_enabled,
                warning_mode=warning_mode,
            )
        )
    except Exception as exc:
        if isinstance(exc, ValueError) and "failed_precondition:" in str(exc):
            logger.error("Causal training failed precondition: %s", exc)
            raise
        logger.exception("Causal full-graph training failed")
        raise self.retry(exc=exc)


async def _train_async(
    *,
    model_version: str | None,
    profile: str,
    bootstrap_iters: int,
    stability_threshold: float,
    lookback_days: int,
    bootstrap_parallelism: int,
    checkpoint_interval: int,
    resume_from_checkpoint: bool,
    early_stop_patience: int,
    discovery_sample_rows: int,
    discovery_max_features: int,
    min_rows_per_outcome: int,
    calibration_enabled: bool,
    warning_mode: str,
) -> dict[str, Any]:
    from scholarpath.db.session import async_session_factory

    async with async_session_factory() as session:
        result = await train_full_graph_model(
            session,
            model_version=model_version,
            profile=profile,
            bootstrap_iters=bootstrap_iters,
            stability_threshold=stability_threshold,
            lookback_days=lookback_days,
            bootstrap_parallelism=bootstrap_parallelism,
            checkpoint_interval=checkpoint_interval,
            resume_from_checkpoint=resume_from_checkpoint,
            early_stop_patience=early_stop_patience,
            discovery_sample_rows=discovery_sample_rows,
            discovery_max_features=discovery_max_features,
            min_rows_per_outcome=min_rows_per_outcome,
            calibration_enabled=calibration_enabled,
            warning_mode=warning_mode,
        )
        await session.commit()

    return {
        "model_version": result.model_version,
        "metrics": result.metrics,
        "refuters": result.refuters,
        "artifact_uri": result.artifact_uri,
        "graph_nodes": len(result.graph_json.get("nodes", [])),
        "graph_edges": len(result.graph_json.get("edges", [])),
    }


@celery_app.task(
    name="scholarpath.tasks.causal_model.causal_promote_model",
    bind=True,
    max_retries=1,
    default_retry_delay=30,
)
def causal_promote_model(self: Any, model_version: str) -> dict[str, Any]:
    """Promote a trained model to active."""
    try:
        return run_async(_promote_async(model_version))
    except Exception as exc:
        logger.exception("Causal model promotion failed")
        raise self.retry(exc=exc)


async def _promote_async(model_version: str) -> dict[str, Any]:
    from scholarpath.db.session import async_session_factory

    async with async_session_factory() as session:
        result = await promote_model(session, model_version=model_version)
        await session.commit()
        return result


@celery_app.task(
    name="scholarpath.tasks.causal_model.causal_shadow_audit",
    bind=True,
    max_retries=0,
)
def causal_shadow_audit(self: Any, active_only: bool = True) -> dict[str, Any]:
    """Summarize shadow-run comparison quality."""
    return run_async(_shadow_audit_async(active_only=active_only))


async def _shadow_audit_async(*, active_only: bool) -> dict[str, Any]:
    from scholarpath.db.session import async_session_factory

    async with async_session_factory() as session:
        result = await shadow_audit(session, active_only=active_only)
        await session.commit()
        return result


@celery_app.task(
    name="scholarpath.tasks.causal_model.causal_rollout_quality_gate",
    bind=True,
    max_retries=0,
)
def causal_rollout_quality_gate(
    self: Any,
    target_percent: int | None = None,
    sample_schools: int = 20,
    contexts: int = 2,
    history_window_runs: int = 24,
    emit_alert: bool = True,
) -> dict[str, Any]:
    """Run rollout quality gate in report mode (alert-only on threshold breach)."""
    return run_async(
        _rollout_quality_async(
            target_percent=target_percent,
            sample_schools=sample_schools,
            contexts=contexts,
            history_window_runs=history_window_runs,
            emit_alert=emit_alert,
        )
    )


async def _rollout_quality_async(
    *,
    target_percent: int | None,
    sample_schools: int,
    contexts: int,
    history_window_runs: int,
    emit_alert: bool,
) -> dict[str, Any]:
    report = await run_causal_rollout_quality_gate(
        target_percent=target_percent,
        sample_schools=sample_schools,
        contexts=contexts,
        history_window_runs=history_window_runs,
        emit_alert=emit_alert,
    )
    return report.to_dict()


@celery_app.task(
    name="scholarpath.tasks.causal_model.causal_daily_gold_eval",
    bind=True,
    max_retries=0,
)
def causal_daily_gold_eval(
    self: Any,
    judge_enabled: bool = True,
    judge_concurrency: int = 2,
    max_rpm_total: int = 180,
    sample_size: int = 40,
) -> dict[str, Any]:
    """Run daily causal gold eval (report mode, no release gating)."""
    return run_async(
        _daily_gold_eval_async(
            judge_enabled=judge_enabled,
            judge_concurrency=judge_concurrency,
            max_rpm_total=max_rpm_total,
            sample_size=sample_size,
        )
    )


async def _daily_gold_eval_async(
    *,
    judge_enabled: bool,
    judge_concurrency: int,
    max_rpm_total: int,
    sample_size: int,
) -> dict[str, Any]:
    report = await run_causal_gold_eval(
        judge_enabled=judge_enabled,
        judge_concurrency=judge_concurrency,
        max_rpm_total=max_rpm_total,
        sample_size=sample_size,
        sample_strategy="full",
    )
    return report.to_dict()
