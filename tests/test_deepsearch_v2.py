"""Unit tests for DeepSearch V2 orchestration primitives."""

from __future__ import annotations

import pytest

from scholarpath.search.canonical_merge import CanonicalMergeService
from scholarpath.search.db_coverage import SchoolCoverageSnapshot
from scholarpath.search.entity_aligner import AlignedEntity
from scholarpath.search.field_planner import FieldCoveragePlanner
from scholarpath.search.orchestrator import DeepSearchOrchestrator
from scholarpath.search.source_value import SourceValueInput, SourceValueScorer
from scholarpath.search.sources.base import BaseSource, SearchResult


class _FakeSource(BaseSource):
    name = "fake"
    source_type = "proxy"

    def __init__(self, return_values: dict[str, str]) -> None:
        self._return_values = return_values
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        requested = tuple(sorted(fields or []))
        self.calls.append((school_name, requested))
        results: list[SearchResult] = []
        for field in requested:
            if field not in self._return_values:
                continue
            results.append(
                SearchResult(
                    source_name=self.name,
                    source_type=self.source_type,
                    source_url="https://example.com",
                    variable_name=field,
                    value_text=self._return_values[field],
                    value_numeric=None,
                    confidence=0.6,
                    raw_data={"queried_school": school_name},
                )
            )
        return results


def test_canonical_merge_dedupes_same_fact_across_sources() -> None:
    merger = CanonicalMergeService()
    rows = [
        SearchResult(
            source_name="source_a",
            source_type="official",
            source_url="",
            variable_name="acceptance rate",
            value_text="5%",
            value_numeric=5.0,
            confidence=0.4,
            raw_data={"queried_school": "MIT"},
        ),
        SearchResult(
            source_name="source_b",
            source_type="proxy",
            source_url="",
            variable_name="acceptance_rate",
            value_text="5%",
            value_numeric=5.0,
            confidence=0.8,
            raw_data={"queried_school": "MIT"},
        ),
    ]

    merged = merger.merge(rows)
    assert len(merged) == 1
    assert merged[0].variable_name == "acceptance_rate"
    assert set(merged[0].raw_data.get("deduped_sources", [])) == {"source_a", "source_b"}


def test_field_planner_routes_missing_fields_to_cheap_sources() -> None:
    planner = FieldCoveragePlanner()
    coverage = {
        "MIT": SchoolCoverageSnapshot(
            target_school="MIT",
            missing_fields=[
                "acceptance_rate",
                "overall_grade",
                "admission_experience",
                "campus_setting",
            ],
        )
    }
    plans = planner.plan_wave_b(
        coverage=coverage,
        required_fields=coverage["MIT"].missing_fields,
        available_sources={"college_scorecard", "niche", "ugc", "web_search"},
    )
    by_source = {plan.source_name: set(plan.fields) for plan in plans["MIT"]}
    assert by_source["college_scorecard"] == {"acceptance_rate"}
    assert by_source["niche"] == {"campus_setting", "overall_grade"}
    assert by_source["ugc"] == {"admission_experience"}


def test_source_value_scorer_prefers_higher_coverage_and_efficiency() -> None:
    scorer = SourceValueScorer()
    strong = scorer.score(
        payload=SourceValueInput(
            calls=4,
            failures=0,
            raw_facts=12,
            kept_facts=10,
            unique_fields=8,
            conflicting_facts=1,
            estimated_tokens=2500,
            avg_latency_ms=900,
        ),
        required_field_count=12,
    )
    weak = scorer.score(
        payload=SourceValueInput(
            calls=4,
            failures=2,
            raw_facts=10,
            kept_facts=3,
            unique_fields=2,
            conflicting_facts=3,
            estimated_tokens=18000,
            avg_latency_ms=5200,
        ),
        required_field_count=12,
    )
    assert strong > weak


def test_source_policy_fuses_low_value_source() -> None:
    scorecard = _FakeSource({})
    scorecard.name = "college_scorecard"
    niche = _FakeSource({})
    niche.name = "niche"

    orchestrator = DeepSearchOrchestrator(
        llm=None,
        sources={
            "college_scorecard": scorecard,
            "niche": niche,
        },
    )
    orchestrator._source_policy_state = {}
    update = orchestrator._update_source_policy(
        source_scores={
            "college_scorecard": 0.72,
            "niche": 0.12,
        },
        source_metrics={
            "college_scorecard": {"calls": 5, "failures": 0},
            "niche": {"calls": 6, "failures": 5},
        },
    )
    snapshot = update["policy_snapshot"]
    assert snapshot["niche"]["status"] == "fused"
    assert snapshot["college_scorecard"]["status"] in {"normal", "downweighted"}
    assert "niche" in update["changed_sources"]


def test_orchestrator_requires_scorecard_key_for_auto_sources() -> None:
    with pytest.raises(ValueError, match="SCORECARD_API_KEY"):
        DeepSearchOrchestrator(
            llm=object(),  # type: ignore[arg-type]
            sources=None,
            scorecard_api_key="",
        )


@pytest.mark.asyncio
async def test_orchestrator_triggers_internal_websearch_on_remaining_gap(monkeypatch) -> None:
    scorecard = _FakeSource({"acceptance_rate": "5%"})
    scorecard.name = "college_scorecard"
    niche = _FakeSource({})
    niche.name = "niche"
    ugc = _FakeSource({})
    ugc.name = "ugc"
    web = _FakeSource({})
    web.name = "web_search"

    orchestrator = DeepSearchOrchestrator(
        llm=None,
        sources={
            "college_scorecard": scorecard,
            "niche": niche,
            "ugc": ugc,
            "web_search": web,
        },
        school_concurrency=2,
        source_http_concurrency=4,
        self_extract_concurrency=2,
        internal_websearch_concurrency=2,
    )

    async def fake_load(**_: object) -> dict[str, SchoolCoverageSnapshot]:
        return {
            "MIT": SchoolCoverageSnapshot(
                target_school="MIT",
                existing_results=[],
                covered_fields=set(),
                missing_fields=["acceptance_rate", "tuition_out_of_state"],
            )
        }

    async def fake_align(results: list[SearchResult]) -> list[AlignedEntity]:
        return [AlignedEntity(canonical_name="MIT", aliases=[], data_points=results)]

    async def fake_detect(_: list[AlignedEntity]) -> list:
        return []

    async def fake_persist(**_: object) -> dict[str, int]:
        return {"persisted_data_points": 0, "persisted_conflicts": 0, "persisted_schools": 0}

    internal = _FakeSource({"tuition_out_of_state": "$60000"})
    internal.name = "internal_web_search"

    monkeypatch.setattr(orchestrator._db_loader, "load", fake_load)
    monkeypatch.setattr(orchestrator._aligner, "align", fake_align)
    monkeypatch.setattr(orchestrator._detector, "detect", fake_detect)
    monkeypatch.setattr(orchestrator, "_persist_results", fake_persist)
    monkeypatch.setattr(orchestrator, "_internal_web_source", internal)

    result = await orchestrator.search(
        student_profile={"gpa": 3.9},
        target_schools=["MIT"],
        required_fields=["acceptance_rate", "tuition_out_of_state"],
        freshness_days=90,
        max_internal_websearch_calls_per_school=1,
        budget_mode="low_cost",
    )

    assert result.search_metadata["self_source_calls"] >= 1
    assert result.search_metadata["internal_websearch_calls"] == 1
    assert result.search_metadata["fallback_trigger_rate"] == 1.0
    assert "source_value_scores" in result.search_metadata
    assert "source_runtime_metrics" in result.search_metadata


@pytest.mark.asyncio
async def test_orchestrator_skips_internal_websearch_for_noncritical_gap(monkeypatch) -> None:
    scorecard = _FakeSource({})
    scorecard.name = "college_scorecard"
    niche = _FakeSource({})
    niche.name = "niche"
    ugc = _FakeSource({})
    ugc.name = "ugc"
    web = _FakeSource({})
    web.name = "web_search"

    orchestrator = DeepSearchOrchestrator(
        llm=None,
        sources={
            "college_scorecard": scorecard,
            "niche": niche,
            "ugc": ugc,
            "web_search": web,
        },
        school_concurrency=2,
        source_http_concurrency=4,
        self_extract_concurrency=2,
        internal_websearch_concurrency=2,
    )

    async def fake_load(**_: object) -> dict[str, SchoolCoverageSnapshot]:
        return {
            "MIT": SchoolCoverageSnapshot(
                target_school="MIT",
                existing_results=[],
                covered_fields=set(),
                missing_fields=["student_reviews"],
            )
        }

    async def fake_align(results: list[SearchResult]) -> list[AlignedEntity]:
        return [AlignedEntity(canonical_name="MIT", aliases=[], data_points=results)]

    async def fake_detect(_: list[AlignedEntity]) -> list:
        return []

    async def fake_persist(**_: object) -> dict[str, int]:
        return {"persisted_data_points": 0, "persisted_conflicts": 0, "persisted_schools": 0}

    internal = _FakeSource({"student_reviews": "great campus life"})
    internal.name = "internal_web_search"

    monkeypatch.setattr(orchestrator._db_loader, "load", fake_load)
    monkeypatch.setattr(orchestrator._aligner, "align", fake_align)
    monkeypatch.setattr(orchestrator._detector, "detect", fake_detect)
    monkeypatch.setattr(orchestrator, "_persist_results", fake_persist)
    monkeypatch.setattr(orchestrator, "_internal_web_source", internal)

    result = await orchestrator.search(
        student_profile={"gpa": 3.9},
        target_schools=["MIT"],
        required_fields=["student_reviews"],
        freshness_days=90,
        max_internal_websearch_calls_per_school=1,
        budget_mode="low_cost",
    )

    assert result.search_metadata["internal_websearch_calls"] == 0
    assert result.search_metadata["fallback_trigger_rate"] == 0.0
