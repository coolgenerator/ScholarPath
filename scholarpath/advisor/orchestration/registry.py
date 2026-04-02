"""Capability registry for advisor orchestration."""

from __future__ import annotations

from scholarpath.advisor.contracts import AdvisorCapability, AdvisorDomain

from .types import CapabilityDefinition


class CapabilityRegistry:
    """In-memory capability registry."""

    def __init__(self) -> None:
        self._by_id: dict[str, CapabilityDefinition] = {}
        self._by_domain: dict[AdvisorDomain, list[CapabilityDefinition]] = {
            "undergrad": [],
            "offer": [],
            "graduate": [],
            "summer": [],
            "common": [],
        }

    def register(self, definition: CapabilityDefinition) -> None:
        self._by_id[definition.capability_id] = definition
        self._by_domain[definition.domain].append(definition)

    def get(self, capability_id: str) -> CapabilityDefinition | None:
        return self._by_id.get(capability_id)

    def list_by_domain(self, domain: AdvisorDomain) -> list[CapabilityDefinition]:
        return list(self._by_domain[domain])

    def list_capability_ids(self) -> list[AdvisorCapability]:
        return [
            capability_id  # type: ignore[list-item]
            for capability_id in sorted(self._by_id.keys())
        ]
