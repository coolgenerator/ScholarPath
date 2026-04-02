"""Execution queue coordinator for advisor orchestration."""

from __future__ import annotations

from scholarpath.advisor.contracts import PendingStep

from .constants import EXECUTION_LIMIT
from .types import IntentCandidate
from .utils import pair_conflict, select_primary


class Coordinator:
    """Coordinator: build execution queue under strict per-turn limit."""

    @staticmethod
    def build_execution_queue(
        candidates: list[IntentCandidate],
    ) -> tuple[list[IntentCandidate], list[PendingStep]]:
        if not candidates:
            return [], []

        execution: list[IntentCandidate] = []
        pending: list[PendingStep] = []

        primary = select_primary(candidates)
        if primary is None:
            return [], []

        execution.append(primary)
        for candidate in candidates:
            if candidate.capability == primary.capability:
                continue
            if len(execution) >= EXECUTION_LIMIT:
                pending.append(
                    PendingStep(
                        capability=candidate.capability,
                        reason="over_limit",
                        message="Exceeded per-turn execution limit.",
                    )
                )
                continue
            if pair_conflict(primary, candidate):
                pending.append(
                    PendingStep(
                        capability=candidate.capability,
                        reason="conflict",
                        message=f"Conflicts with primary {primary.capability}.",
                    )
                )
                continue
            execution.append(candidate)

        return execution, pending
