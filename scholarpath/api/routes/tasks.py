"""Task polling routes -- check Celery task status and results."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

router = APIRouter(prefix="/tasks", tags=["tasks"])


def _get_celery_app():
    """Lazily import the Celery app so the module loads without Celery."""
    try:
        from scholarpath.tasks import celery_app  # type: ignore[import-untyped]

        return celery_app
    except ImportError:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Task queue not configured",
        )


@router.post("/causal/train")
async def enqueue_causal_train(
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
) -> dict:
    """Enqueue full-graph causal model training."""
    app = _get_celery_app()
    result = app.send_task(
        "scholarpath.tasks.causal_model.causal_train_full_graph",
        kwargs={
            "profile": profile,
            "bootstrap_iters": bootstrap_iters,
            "stability_threshold": stability_threshold,
            "lookback_days": lookback_days,
            "bootstrap_parallelism": bootstrap_parallelism,
            "checkpoint_interval": checkpoint_interval,
            "resume_from_checkpoint": resume_from_checkpoint,
            "early_stop_patience": early_stop_patience,
            "discovery_sample_rows": discovery_sample_rows,
            "discovery_max_features": discovery_max_features,
            "min_rows_per_outcome": min_rows_per_outcome,
            "calibration_enabled": calibration_enabled,
            "warning_mode": warning_mode,
        },
    )
    return {"task_id": result.id, "status": "PENDING"}


@router.post("/causal/promote/{model_version}")
async def enqueue_causal_promote(model_version: str) -> dict:
    """Enqueue causal model promotion to active."""
    app = _get_celery_app()
    result = app.send_task(
        "scholarpath.tasks.causal_model.causal_promote_model",
        kwargs={"model_version": model_version},
    )
    return {"task_id": result.id, "status": "PENDING"}


@router.post("/causal/shadow-audit")
async def enqueue_causal_shadow_audit(active_only: bool = True) -> dict:
    """Enqueue shadow-run quality audit."""
    app = _get_celery_app()
    result = app.send_task(
        "scholarpath.tasks.causal_model.causal_shadow_audit",
        kwargs={"active_only": active_only},
    )
    return {"task_id": result.id, "status": "PENDING"}


@router.get("/{task_id}")
async def get_task_status(task_id: str) -> dict:
    """Poll the status of a Celery task.

    Returns one of: PENDING, STARTED, SUCCESS, FAILURE.
    """
    app = _get_celery_app()
    result = app.AsyncResult(task_id)

    response: dict = {
        "task_id": task_id,
        "status": result.status,
    }

    if result.status == "FAILURE":
        response["error"] = str(result.result)
    elif result.status == "SUCCESS":
        response["result_ready"] = True

    return response


@router.get("/{task_id}/result")
async def get_task_result(task_id: str) -> dict:
    """Get the result of a completed Celery task."""
    app = _get_celery_app()
    result = app.AsyncResult(task_id)

    if result.status == "PENDING":
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail="Task is still pending",
        )
    if result.status == "STARTED":
        raise HTTPException(
            status_code=status.HTTP_202_ACCEPTED,
            detail="Task is still running",
        )
    if result.status == "FAILURE":
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Task failed: {result.result}",
        )

    return {"task_id": task_id, "status": "SUCCESS", "result": result.result}
