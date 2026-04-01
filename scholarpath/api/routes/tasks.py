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


@router.get("/tasks/{task_id}")
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


@router.get("/tasks/{task_id}/result")
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
