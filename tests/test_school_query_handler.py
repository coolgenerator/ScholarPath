"""Regression tests for school_query heuristic-first extraction flow."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from scholarpath.chat.handlers.school_query import (
    _maybe_refresh_with_internal_deepsearch,
    _missing_critical_fields,
    handle_school_query,
)
from scholarpath.chat.memory import ChatMemory
from tests.fake_redis import FakeRedis


@dataclass
class _DummySchool:
    id: uuid.UUID
    name: str
    name_cn: str = ""
    city: str = "City"
    state: str = "State"
    school_type: str = "private"
    us_news_rank: int | None = 1
    acceptance_rate: float | None = 0.05
    sat_25: int | None = 1450
    sat_75: int | None = 1560
    tuition_oos: float | None = 65000
    avg_net_price: float | None = 35000
    student_faculty_ratio: str | None = "5:1"
    graduation_rate_4yr: float | None = 0.89
    intl_student_pct: float | None = 0.22
    campus_setting: str | None = "urban"


@dataclass
class _DummyDataPoint:
    variable_name: str
    crawled_at: datetime


class _StubLLM:
    def __init__(self, *, extracted_school: str | None, answer_text: str = "ok") -> None:
        self._extracted_school = extracted_school
        self._answer_text = answer_text
        self.complete_json_calls = 0
        self.complete_calls = 0

    async def complete_json(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.complete_json_calls += 1
        return {"school_name": self._extracted_school}

    async def complete(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.complete_calls += 1
        return self._answer_text


@pytest.mark.asyncio
async def test_school_query_uses_context_heuristic_without_extractor_llm(monkeypatch) -> None:
    school = _DummySchool(id=uuid.uuid4(), name="Stanford University")

    async def _search(*args, **kwargs):  # noqa: ANN002, ANN003
        return [school]

    async def _detail(*args, **kwargs):  # noqa: ANN002, ANN003
        return {"programs": [], "data_points": [], "conflicts": []}

    llm = _StubLLM(extracted_school=None, answer_text="斯坦福的CS很强。")

    async def _extract_should_not_run(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("LLM extractor should not run when heuristic hits")

    monkeypatch.setattr("scholarpath.chat.handlers.school_query.search_schools", _search)
    monkeypatch.setattr("scholarpath.chat.handlers.school_query.get_school_detail", _detail)
    monkeypatch.setattr("scholarpath.chat.handlers.school_query._extract_school_name", _extract_should_not_run)

    memory = ChatMemory(FakeRedis())
    await memory.save_context(
        "sess-heuristic",
        "current_school_name",
        "Stanford University",
        domain="undergrad",
    )

    result = await handle_school_query(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        memory=memory,
        session_id="sess-heuristic",
        student_id=uuid.uuid4(),
        message="这所学校的CS课程强吗？",
    )

    assert result.school_name == "Stanford University"
    assert result.extraction_source == "context"
    assert result.llm_calls == 1
    assert llm.complete_json_calls == 0
    assert llm.complete_calls == 1


@pytest.mark.asyncio
async def test_school_query_falls_back_to_extractor_llm_when_heuristic_misses(monkeypatch) -> None:
    school = _DummySchool(id=uuid.uuid4(), name="Massachusetts Institute of Technology")

    async def _search(*args, **kwargs):  # noqa: ANN002, ANN003
        return [school]

    async def _detail(*args, **kwargs):  # noqa: ANN002, ANN003
        return {"programs": [], "data_points": [], "conflicts": []}

    monkeypatch.setattr("scholarpath.chat.handlers.school_query.search_schools", _search)
    monkeypatch.setattr("scholarpath.chat.handlers.school_query.get_school_detail", _detail)

    llm = _StubLLM(extracted_school="MIT", answer_text="MIT has strong engineering and CS.")
    memory = ChatMemory(FakeRedis())

    result = await handle_school_query(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        memory=memory,
        session_id="sess-llm",
        student_id=uuid.uuid4(),
        message="Can you tell me more about this university's curriculum?",
    )

    assert result.extraction_source == "llm"
    assert result.school_name == "Massachusetts Institute of Technology"
    assert result.llm_calls == 2
    assert llm.complete_json_calls == 1
    assert llm.complete_calls == 1


@pytest.mark.asyncio
async def test_school_query_alias_does_not_misread_mit_as_context_pronoun(monkeypatch) -> None:
    school = _DummySchool(id=uuid.uuid4(), name="Massachusetts Institute of Technology")

    async def _search(*args, **kwargs):  # noqa: ANN002, ANN003
        return [school]

    async def _detail(*args, **kwargs):  # noqa: ANN002, ANN003
        return {"programs": [], "data_points": [], "conflicts": []}

    llm = _StubLLM(extracted_school=None, answer_text="MIT details.")

    async def _extract_should_not_run(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("LLM extractor should not run when alias heuristic hits")

    monkeypatch.setattr("scholarpath.chat.handlers.school_query.search_schools", _search)
    monkeypatch.setattr("scholarpath.chat.handlers.school_query.get_school_detail", _detail)
    monkeypatch.setattr("scholarpath.chat.handlers.school_query._extract_school_name", _extract_should_not_run)

    memory = ChatMemory(FakeRedis())
    await memory.save_context(
        "sess-alias",
        "current_school_name",
        "Stanford University",
        domain="undergrad",
    )

    result = await handle_school_query(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        memory=memory,
        session_id="sess-alias",
        student_id=uuid.uuid4(),
        message="Tell me about MIT curriculum and labs.",
    )

    assert result.extraction_source == "alias"
    assert result.school_name == "Massachusetts Institute of Technology"
    assert result.llm_calls == 1
    assert llm.complete_json_calls == 0
    assert llm.complete_calls == 1


@pytest.mark.asyncio
async def test_school_query_returns_clarify_when_no_school_identified(monkeypatch) -> None:
    async def _search(*args, **kwargs):  # noqa: ANN002, ANN003
        return []

    monkeypatch.setattr("scholarpath.chat.handlers.school_query.search_schools", _search)

    llm = _StubLLM(extracted_school=None)
    memory = ChatMemory(FakeRedis())

    result = await handle_school_query(
        llm=llm,  # type: ignore[arg-type]
        session=MagicMock(),
        memory=memory,
        session_id="sess-none",
        student_id=uuid.uuid4(),
        message="Can you compare the vibe and classes?",
    )

    assert result.school_name is None
    assert result.extraction_source == "llm"
    assert result.llm_calls == 1
    assert llm.complete_json_calls == 1
    assert llm.complete_calls == 0
    assert "which school" in result.text.lower()


@pytest.mark.asyncio
async def test_internal_deepsearch_refresh_triggers_for_missing_fields(monkeypatch) -> None:
    school = _DummySchool(id=uuid.uuid4(), name="Stanford University")
    called: dict[str, object] = {}

    async def _fake_run_internal_deepsearch(  # noqa: ANN202
        *,
        session,
        student_id: uuid.UUID,
        school_name: str,
        required_fields: list[str] | None = None,
        eval_run_id: str | None = None,
    ):
        called["session"] = session
        called["student_id"] = student_id
        called["school_name"] = school_name
        called["required_fields"] = required_fields or []
        called["eval_run_id"] = eval_run_id
        return {"errors": [], "schools_returned": 1}

    monkeypatch.setattr(
        "scholarpath.chat.handlers.school_query._run_internal_deepsearch",
        _fake_run_internal_deepsearch,
    )
    monkeypatch.setattr(
        "scholarpath.chat.handlers.school_query.settings.ADVISOR_INTERNAL_DEEPSEARCH_ENABLED",
        True,
    )
    monkeypatch.setattr(
        "scholarpath.chat.handlers.school_query.settings.ADVISOR_INTERNAL_DEEPSEARCH_FRESHNESS_DAYS",
        90,
    )
    monkeypatch.setattr(
        "scholarpath.chat.handlers.school_query.settings.ADVISOR_INTERNAL_DEEPSEARCH_MAX_INTERNAL_WEBSEARCH_PER_SCHOOL",
        1,
    )
    monkeypatch.setattr(
        "scholarpath.chat.handlers.school_query.settings.ADVISOR_INTERNAL_DEEPSEARCH_BUDGET_MODE",
        "balanced",
    )

    meta = await _maybe_refresh_with_internal_deepsearch(
        session=MagicMock(),
        student_id=uuid.uuid4(),
        school=school,  # type: ignore[arg-type]
        data_points=[],
    )

    assert meta["triggered"] is True
    assert isinstance(meta.get("missing_fields_before"), list)
    assert called["school_name"] == "Stanford University"
    assert isinstance(called.get("required_fields"), list)
    assert str(called.get("eval_run_id", "")).startswith("advisor-school-query-")


def test_missing_critical_fields_respects_90_day_window() -> None:
    now = datetime.now(UTC)
    data_points = [
        _DummyDataPoint(variable_name="acceptance_rate", crawled_at=now - timedelta(days=1)),
        _DummyDataPoint(variable_name="tuition_out_of_state", crawled_at=now - timedelta(days=120)),
    ]
    missing = _missing_critical_fields(data_points, freshness_days=90)
    assert "acceptance_rate" not in missing
    assert "tuition_out_of_state" in missing
