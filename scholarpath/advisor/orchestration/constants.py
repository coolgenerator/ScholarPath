"""Constants shared across advisor orchestration components."""

from __future__ import annotations

from scholarpath.advisor.contracts import AdvisorDomain

ROUTABLE_DOMAINS: tuple[AdvisorDomain, ...] = ("undergrad", "offer")
CLASSIFIABLE_DOMAINS: tuple[AdvisorDomain, ...] = ("undergrad", "offer", "common")
ALLOWED_DOMAINS: set[AdvisorDomain] = {"undergrad", "offer", "graduate", "summer", "common"}

EXECUTION_LIMIT = 2
PENDING_QUEUE_KEY = "advisor_pending_queue"
FAILED_STEPS_KEY = "advisor_failed_steps"

DOMAIN_THRESHOLD_DEFAULT = 0.60
CAPABILITY_THRESHOLD_DEFAULT = 0.70
INTENT_CLARITY_THRESHOLD_DEFAULT = 0.65

CAPABILITY_THRESHOLD_MAP_DEFAULT: dict[str, float] = {
    "default": 0.70,
    "undergrad.strategy.plan": 0.75,
    "offer.decision": 0.75,
    "common.general": 0.55,
    "common.emotional_support": 0.55,
}

TRIGGER_ACTIONS: set[str] = {"queue.run_pending", "step.retry"}
