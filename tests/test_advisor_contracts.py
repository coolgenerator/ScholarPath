"""Contract tests for advisor v1 request/response models."""

from __future__ import annotations

from scholarpath.advisor.contracts import (
    AdvisorAction,
    AdvisorHistoryEntry,
    AdvisorRequest,
    AdvisorResponse,
    AdvisorRouteMeta,
    DoneStep,
    GuidedIntakeArtifact,
    GuidedQuestion,
    OfferComparisonArtifact,
    MemoryIngestEvent,
    MemoryItem,
    PendingStep,
    RecommendationData,
    RecommendedSchool,
    SchoolRecommendationArtifact,
    StrategyPlanArtifact,
    WhatIfResultArtifact,
)


def test_advisor_request_defaults() -> None:
    req = AdvisorRequest(session_id="s-1", message="帮我推荐学校")
    assert req.turn_id is None
    assert req.student_id is None
    assert req.domain_hint is None
    assert req.edit is None


def test_advisor_request_edit_payload() -> None:
    req = AdvisorRequest(
        session_id="s-1",
        message="把这条改掉",
        edit={"target_turn_id": "turn-1", "mode": "overwrite"},
    )
    assert req.edit is not None
    assert req.edit.target_turn_id == "turn-1"
    assert req.edit.mode == "overwrite"


def test_advisor_response_artifact_union_roundtrip() -> None:
    guided = GuidedIntakeArtifact(
        questions=[
            GuidedQuestion(
                id="major",
                title="What major?",
                options=[],
                allow_custom=True,
                custom_placeholder="Type here",
            )
        ]
    )
    recommendation = SchoolRecommendationArtifact(
        data=RecommendationData(
            narrative="Top picks for you.",
            schools=[
                RecommendedSchool(
                    school_name="Stanford University",
                    tier="reach",
                    overall_score=0.91,
                    admission_probability=0.18,
                    sub_scores={"academic": 0.95, "financial": 0.7},
                )
            ],
            ed_recommendation="Stanford University",
            ea_recommendations=["MIT"],
            strategy_summary="ED Stanford; EA MIT.",
        )
    )
    offer = OfferComparisonArtifact(
        offers=[{"school": "A"}],
        comparison_matrix={"net_cost": {"A": 30000}},
        recommendation="Choose A.",
    )
    strategy = StrategyPlanArtifact(strategy={"ed_recommendation": {"school": "A"}})
    what_if = WhatIfResultArtifact(
        interventions={"student_ability": 0.9},
        deltas={"admission_probability": 0.12},
        explanation="Probability improves.",
    )

    response = AdvisorResponse(
        turn_id="t-1",
        domain="undergrad",
        capability="undergrad.school.recommend",
        assistant_text="Done",
        artifacts=[guided, recommendation, offer, strategy, what_if],
        actions=[AdvisorAction(action_id="legacy.action", label="Legacy", payload={})],
        done=[
            DoneStep(
                capability="undergrad.school.recommend",
                status="succeeded",
                message="recommend completed",
                retry_count=0,
            )
        ],
        pending=[
            PendingStep(
                capability="undergrad.strategy.plan",
                reason="over_limit",
                message="queued",
            )
        ],
        next_actions=[
            AdvisorAction(
                action_id="queue.run_pending",
                label="继续 undergrad.strategy.plan",
                payload={"capability_hint": "undergrad.strategy.plan"},
            )
        ],
        route_meta=AdvisorRouteMeta(
            domain_confidence=0.92,
            capability_confidence=0.88,
            router_model="gpt-5.4-mini",
            latency_ms=120,
            fallback_used=False,
        ),
    )

    dumped = response.model_dump(mode="json")
    restored = AdvisorResponse.model_validate(dumped)

    assert restored.turn_id == "t-1"
    assert restored.domain == "undergrad"
    assert len(restored.artifacts) == 5
    assert len(restored.done) == 1
    assert len(restored.pending) == 1
    assert len(restored.next_actions) == 1
    assert restored.artifacts[0].type == "guided_intake"
    assert restored.artifacts[1].type == "school_recommendation"
    assert restored.artifacts[2].type == "offer_comparison"
    assert restored.artifacts[3].type == "strategy_plan"
    assert restored.artifacts[4].type == "what_if_result"


def test_advisor_response_defaults_include_coordination_fields() -> None:
    response = AdvisorResponse(
        turn_id="t-2",
        domain="common",
        capability="common.clarify",
        assistant_text="clarify",
        route_meta=AdvisorRouteMeta(
            domain_confidence=0.0,
            capability_confidence=0.0,
            router_model="gpt-5.4-mini",
            latency_ms=1,
            fallback_used=True,
        ),
    )

    dumped = response.model_dump(mode="json")
    assert dumped["done"] == []
    assert dumped["pending"] == []
    assert dumped["next_actions"] == []


def test_route_meta_optional_memory_observability_fields() -> None:
    meta = AdvisorRouteMeta(
        domain_confidence=0.9,
        capability_confidence=0.8,
        router_model="gpt-5.4-mini",
        latency_ms=60,
        fallback_used=False,
        context_tokens=780,
        memory_hits=5,
        rag_hits=3,
        rag_latency_ms=42,
        memory_degraded=False,
        guard_result="clarify",
        guard_reason="low_confidence",
        primary_capability="undergrad.school.recommend",
        executed_count=1,
        pending_count=2,
    )
    payload = meta.model_dump(mode="json")
    assert payload["context_tokens"] == 780
    assert payload["memory_hits"] == 5
    assert payload["rag_hits"] == 3
    assert payload["rag_latency_ms"] == 42
    assert payload["memory_degraded"] is False
    assert payload["guard_result"] == "clarify"
    assert payload["guard_reason"] == "low_confidence"
    assert payload["primary_capability"] == "undergrad.school.recommend"
    assert payload["executed_count"] == 1
    assert payload["pending_count"] == 2


def test_internal_memory_contracts_validate() -> None:
    event = MemoryIngestEvent(
        turn_id="t-3",
        session_id="s-1",
        student_id=None,
        domain="undergrad",
        capability="undergrad.school.recommend",
        role="assistant",
        content="summary",
        artifacts=[{"type": "info_card", "title": "x", "summary": "y"}],
        done=[
            DoneStep(
                capability="undergrad.school.recommend",
                status="succeeded",
                message="done",
                retry_count=0,
            )
        ],
        pending=[],
        next_actions=[AdvisorAction(action_id="queue.run_pending", label="继续", payload={})],
    )
    item = MemoryItem(
        scope="session",
        type="decision",
        key="done:undergrad.school.recommend",
        value={"status": "succeeded"},
        confidence=0.9,
        session_id="s-1",
        source_turn_id="t-3",
        expires_at=None,
    )
    assert event.role == "assistant"
    assert item.status == "active"


def test_advisor_history_entry_extended_fields() -> None:
    entry = {
        "role": "user",
        "content": "hello",
        "message_id": "mid-1",
        "turn_id": "turn-1",
        "created_at": "2026-04-02T01:00:00+00:00",
        "editable": True,
        "edited": True,
    }
    model = AdvisorHistoryEntry.model_validate(entry)
    assert model.editable is True
    assert model.edited is True
