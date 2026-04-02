"""Utility helpers for advisor orchestration logic."""

from __future__ import annotations

import uuid
from typing import Any

from scholarpath.advisor.contracts import (
    AdvisorAction,
    AdvisorCapability,
    AdvisorDomain,
    DoneStep,
    PendingStep,
)
from scholarpath.advisor.memory_context import ContextMetrics
from scholarpath.advisor.router_policy import (
    CONFLICT_GAP_THRESHOLD,
    CONFLICT_GROUP_MAP,
    CONFLICT_SECONDARY_MIN_CONFIDENCE,
    capability_priority as policy_capability_priority,
    contains_ambiguous_expression as policy_contains_ambiguous_expression,
    contains_offer_signal as policy_contains_offer_signal,
    contains_portfolio_signal as policy_contains_portfolio_signal,
    contains_school_or_offer_signal as policy_contains_school_or_offer_signal,
    contains_smalltalk_signal as policy_contains_smalltalk_signal,
    contains_undergrad_signal as policy_contains_undergrad_signal,
    fallback_common_capability as policy_fallback_common_capability,
    is_emotional_message as policy_is_emotional_message,
    signal_domain_from_message as policy_signal_domain_from_message,
)

from .types import IntentCandidate


def bound_confidence(value: Any) -> float:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, num))


def compact(text: str, max_len: int = 160) -> str:
    stripped = " ".join(text.split())
    if len(stripped) <= max_len:
        return stripped
    return stripped[: max_len - 3] + "..."


def conflict_group(capability: AdvisorCapability) -> str:
    return CONFLICT_GROUP_MAP.get(capability, "default")


def pair_conflict(a: IntentCandidate, b: IntentCandidate) -> bool:
    return a.conflict_group == b.conflict_group and a.capability != b.capability


def has_unresolved_conflict(
    candidates: list[IntentCandidate],
    *,
    primary: IntentCandidate | None = None,
) -> bool:
    selected = primary or select_primary(candidates)
    if selected is None:
        return False
    conflicting = [
        row
        for row in candidates
        if row.capability != selected.capability and pair_conflict(selected, row)
    ]
    if not conflicting:
        return False
    second = sorted(conflicting, key=lambda x: x.confidence, reverse=True)[0]
    gap = abs(selected.confidence - second.confidence)
    return gap <= CONFLICT_GAP_THRESHOLD and second.confidence >= CONFLICT_SECONDARY_MIN_CONFIDENCE


def sort_and_dedupe_candidates(candidates: list[IntentCandidate]) -> list[IntentCandidate]:
    seen: set[str] = set()
    out: list[IntentCandidate] = []
    for row in sorted(candidates, key=lambda x: x.confidence, reverse=True):
        if row.capability in seen:
            continue
        seen.add(row.capability)
        out.append(row)
    return out


def capability_priority(capability: AdvisorCapability) -> int:
    return policy_capability_priority(capability)


def select_primary(candidates: list[IntentCandidate]) -> IntentCandidate | None:
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda x: (capability_priority(x.capability), -x.confidence),
    )[0]


def lead_confidence(candidates: list[IntentCandidate], *, primary: IntentCandidate | None) -> float:
    if primary is None:
        return 0.0
    competitor_confidences = [
        row.confidence
        for row in candidates
        if row.capability != primary.capability
    ]
    if not competitor_confidences:
        return primary.confidence
    return max(primary.confidence - max(competitor_confidences), 0.0)


def contains_ambiguous_expression(message: str) -> bool:
    return policy_contains_ambiguous_expression(message)


def compute_intent_clarity(
    *,
    candidates: list[IntentCandidate],
    ambiguous_expression: bool,
    has_explicit_target: bool,
    model_intent_clarity: float | None,
) -> float:
    if not candidates:
        heuristic = 0.0
    else:
        top = candidates[0].confidence
        second = candidates[1].confidence if len(candidates) > 1 else 0.0
        margin = max(top - second, 0.0)
        heuristic = bound_confidence((0.65 * top) + (0.35 * margin))

    if ambiguous_expression and not has_explicit_target:
        heuristic = min(heuristic, 0.45)
    if has_explicit_target:
        heuristic = max(heuristic, 0.80)

    if model_intent_clarity is None:
        return heuristic
    blended = bound_confidence((heuristic + model_intent_clarity) / 2)
    if ambiguous_expression and not has_explicit_target:
        return min(blended, 0.45)
    return blended


def is_emotional_message(message: str) -> bool:
    return policy_is_emotional_message(message)


def contains_school_or_offer_signal(message: str) -> bool:
    return policy_contains_school_or_offer_signal(message)


def contains_portfolio_signal(message: str) -> bool:
    return policy_contains_portfolio_signal(message)


def contains_smalltalk_signal(message: str) -> bool:
    return policy_contains_smalltalk_signal(message)


def contains_undergrad_signal(message: str) -> bool:
    return policy_contains_undergrad_signal(message)


def contains_offer_signal(message: str) -> bool:
    return policy_contains_offer_signal(message)


def signal_domain_from_message(message: str) -> AdvisorDomain | None:
    return policy_signal_domain_from_message(message)


def fallback_common_capability(message: str) -> tuple[AdvisorCapability, float]:
    return policy_fallback_common_capability(message)


def student_id_validation_error(raw: str | None) -> str | None:
    if raw is None:
        return None
    try:
        uuid.UUID(raw)
        return None
    except (TypeError, ValueError):
        return "student_id must be a valid UUID."


def parse_pending_steps(raw: Any) -> list[PendingStep]:
    if not isinstance(raw, list):
        return []
    out: list[PendingStep] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            out.append(PendingStep.model_validate(item))
        except Exception:
            continue
    return out


def dedupe_pending(pending: list[PendingStep], *, executed: list[AdvisorCapability]) -> list[PendingStep]:
    seen: set[str] = set()
    out: list[PendingStep] = []
    executed_set = {str(c) for c in executed}
    for row in pending:
        cap = str(row.capability)
        if cap in executed_set:
            continue
        if cap in seen:
            continue
        seen.add(cap)
        out.append(row)
    return out


def build_next_actions(
    pending: list[PendingStep],
    done: list[DoneStep],
    *,
    force_clarify: bool = False,
) -> list[AdvisorAction]:
    actions: list[AdvisorAction] = []
    for step in pending:
        actions.append(
            AdvisorAction(
                action_id="queue.run_pending",
                label=f"继续：{step.capability}",
                payload={
                    "capability_hint": step.capability,
                    "client_context": {"trigger": "queue.run_pending"},
                },
            )
        )
    for step in done:
        if step.status == "failed":
            actions.append(
                AdvisorAction(
                    action_id="step.retry",
                    label=f"重试：{step.capability}",
                    payload={
                        "capability_hint": step.capability,
                        "client_context": {"trigger": "step.retry"},
                    },
                )
            )
    if pending or force_clarify:
        actions.append(
            AdvisorAction(
                action_id="route.clarify",
                label="先澄清优先级",
                payload={"client_context": {"trigger": "route.clarify"}},
            )
        )
    return dedupe_actions(actions)


def build_summary_text(
    *,
    step_outputs: list[str],
    done: list[DoneStep],
    pending: list[PendingStep],
    next_actions: list[AdvisorAction],
) -> str:
    done_lines = ["done:"]
    if done:
        done_lines.extend(
            f"- {s.capability} [{s.status}]" + (f" ({s.message})" if s.message else "")
            for s in done
        )
    else:
        done_lines.append("- (none)")

    pending_lines = ["pending:"]
    if pending:
        pending_lines.extend(
            f"- {p.capability} ({p.reason})" + (f" ({p.message})" if p.message else "")
            for p in pending
        )
    else:
        pending_lines.append("- (none)")

    action_lines = ["next_actions:"]
    if next_actions:
        action_lines.extend(f"- {a.label} [{a.action_id}]" for a in next_actions)
    else:
        action_lines.append("- (none)")

    body: list[str] = []
    if step_outputs:
        body.append("execution_results:")
        body.extend(f"- {line}" for line in step_outputs)
    body.extend(done_lines)
    body.extend(pending_lines)
    body.extend(action_lines)
    return "\n".join(body)


def dedupe_actions(actions: list[AdvisorAction]) -> list[AdvisorAction]:
    seen: set[tuple[str, str]] = set()
    out: list[AdvisorAction] = []
    for row in actions:
        cap = str(row.payload.get("capability_hint", ""))
        key = (row.action_id, cap)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def parse_uuid_or_none(raw: str | None) -> uuid.UUID | None:
    if not raw:
        return None
    try:
        return uuid.UUID(raw)
    except (TypeError, ValueError):
        return None


def safe_capability(raw: str | None, *, fallback: AdvisorCapability) -> AdvisorCapability:
    if not raw:
        return fallback
    valid: set[str] = {
        "undergrad.profile.intake",
        "undergrad.school.recommend",
        "undergrad.school.query",
        "undergrad.strategy.plan",
        "offer.compare",
        "offer.decision",
        "offer.what_if",
        "graduate.program.recommend",
        "summer.program.recommend",
        "common.general",
        "common.emotional_support",
        "common.clarify",
    }
    return raw if raw in valid else fallback


def merge_context_metrics(base: ContextMetrics, incoming: ContextMetrics) -> ContextMetrics:
    return ContextMetrics(
        context_tokens=max(base.context_tokens, incoming.context_tokens),
        memory_hits=max(base.memory_hits, incoming.memory_hits),
        rag_hits=max(base.rag_hits, incoming.rag_hits),
        rag_latency_ms=max(base.rag_latency_ms, incoming.rag_latency_ms),
        memory_degraded=base.memory_degraded or incoming.memory_degraded,
        memory_conflicts=max(base.memory_conflicts, incoming.memory_conflicts),
    )
