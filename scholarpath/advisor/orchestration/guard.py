"""Clarify guard component for advisor orchestration."""

from __future__ import annotations

from scholarpath.advisor.contracts import AdvisorRequest

from .constants import CAPABILITY_THRESHOLD_DEFAULT, TRIGGER_ACTIONS
from .registry import CapabilityRegistry
from .types import GuardDecision, RouteDecision
from .utils import lead_confidence, student_id_validation_error


class ClarifyGate:
    """Guard: validates input and decides pass/clarify/invalid_input."""

    def __init__(
        self,
        *,
        registry: CapabilityRegistry,
        domain_threshold: float,
        intent_clarity_threshold: float,
        capability_threshold_map: dict[str, float],
    ) -> None:
        self._registry = registry
        self._domain_threshold = domain_threshold
        self._intent_clarity_threshold = intent_clarity_threshold
        self._capability_threshold_map = capability_threshold_map

    def evaluate(self, *, request: AdvisorRequest, decision: RouteDecision) -> GuardDecision:
        student_error = student_id_validation_error(request.student_id)
        if student_error is not None:
            return GuardDecision(
                result="invalid_input",
                reason="invalid_input",
                message=student_error,
            )

        trigger = str((request.client_context or {}).get("trigger", "")).strip()
        explicit_trigger = trigger in TRIGGER_ACTIONS
        if explicit_trigger:
            if not request.capability_hint:
                return GuardDecision(
                    result="invalid_input",
                    reason="trigger_invalid",
                    message=f"capability_hint is required for trigger: {trigger}",
                )
            requested = self._registry.get(str(request.capability_hint))
            if requested is None:
                return GuardDecision(
                    result="invalid_input",
                    reason="trigger_invalid",
                    message=f"Unknown capability_hint: {request.capability_hint}",
                )
            return GuardDecision(
                result="pass",
                reason="none",
                requested_definition=requested,
            )

        primary = decision.primary
        if primary is None:
            return GuardDecision(result="clarify", reason="low_confidence")

        lead = lead_confidence(decision.candidates, primary=primary)
        is_offer_high_confidence_direct = (
            primary.capability in {"offer.compare", "offer.decision"}
            and decision.capability_confidence >= 0.85
            and lead >= 0.15
        )

        if decision.unresolved_conflict and not is_offer_high_confidence_direct:
            return GuardDecision(result="clarify", reason="conflict")

        if primary.capability in {"common.general", "common.emotional_support"}:
            if (
                decision.ambiguous_expression
                and not request.capability_hint
                and not request.domain_hint
            ):
                return GuardDecision(result="clarify", reason="low_confidence")
            return GuardDecision(result="pass", reason="none")

        capability_threshold = self._capability_threshold_map.get(
            primary.capability,
            self._capability_threshold_map.get("default", CAPABILITY_THRESHOLD_DEFAULT),
        )
        hard_low_confidence = (
            decision.domain_confidence < self._domain_threshold
            or decision.capability_confidence < capability_threshold
        )
        clarity_is_low = decision.intent_clarity < self._intent_clarity_threshold
        clarity_supports_clarify = clarity_is_low and decision.capability_confidence < max(
            capability_threshold,
            0.82,
        )
        ambiguous_without_target = (
            decision.ambiguous_expression
            and not request.capability_hint
            and not request.domain_hint
            and decision.capability_confidence < max(capability_threshold, 0.82)
        )

        should_clarify = hard_low_confidence or clarity_supports_clarify or ambiguous_without_target
        if should_clarify and not is_offer_high_confidence_direct:
            return GuardDecision(result="clarify", reason="low_confidence")

        return GuardDecision(result="pass", reason="none")
