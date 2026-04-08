"""Tests for DeepSearch Celery task preflight behavior."""

from __future__ import annotations

import uuid

import pytest

pytest.importorskip("celery")

import scholarpath.tasks.deep_search as deep_search_task
from scholarpath.config import settings


def test_require_scorecard_api_key_rejects_empty() -> None:
    with pytest.raises(ValueError, match="SCORECARD_API_KEY"):
        deep_search_task._require_scorecard_api_key("")


def test_run_deep_search_fails_fast_when_scorecard_key_missing(monkeypatch) -> None:
    monkeypatch.setattr(settings, "SCORECARD_API_KEY", "")

    with pytest.raises(ValueError, match="SCORECARD_API_KEY"):
        deep_search_task.run_deep_search.run(  # type: ignore[misc]
            student_id=str(uuid.uuid4()),
            school_names=["MIT"],
            required_fields=["acceptance_rate"],
        )


def test_run_deep_search_passes_configured_scorecard_key(monkeypatch) -> None:
    monkeypatch.setattr(settings, "SCORECARD_API_KEY", "scorecard-key")

    observed: dict[str, str | None] = {}

    async def _fake_run_async(*args, **kwargs):  # type: ignore[no-untyped-def]
        observed["scorecard_api_key"] = kwargs.get("scorecard_api_key")
        return {"ok": True}

    monkeypatch.setattr(deep_search_task, "_run_deep_search_async", _fake_run_async)

    payload = deep_search_task.run_deep_search.run(  # type: ignore[misc]
        student_id=str(uuid.uuid4()),
        school_names=["MIT"],
        required_fields=["acceptance_rate"],
    )
    assert payload == {"ok": True}
    assert observed["scorecard_api_key"] == "scorecard-key"


def test_run_deep_search_returns_non_retryable_for_missing_student(monkeypatch) -> None:
    monkeypatch.setattr(settings, "SCORECARD_API_KEY", "scorecard-key")

    async def _missing_student(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise deep_search_task.StudentNotFoundError("Student 000 not found")

    monkeypatch.setattr(deep_search_task, "_run_deep_search_async", _missing_student)

    payload = deep_search_task.run_deep_search.run(  # type: ignore[misc]
        student_id=str(uuid.uuid4()),
        school_names=["MIT"],
        required_fields=["acceptance_rate"],
    )
    assert payload["non_retryable_error"] == "student_not_found"
