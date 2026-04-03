"""Deterministic case selection utilities for advisor orchestrator eval."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class AdvisorEvalCase:
    case_id: str
    category: str
    prompt: str
    expected_capability: str


@dataclass(slots=True)
class ReeditEvalCase:
    case_id: str
    category: str
    original_turn: str
    edited_turn: str


ORCHESTRATOR_MINI_QUOTA = {
    "single_intent": 3,
    "multi_over_limit": 2,
    "conflict_clarify": 1,
    "low_confidence": 1,
    "explicit_recovery": 1,
    "input_error": 1,
    "memory_degraded": 1,
}

REEDIT_MINI_QUOTA = {
    "middle": 1,
    "edge": 1,
    "tail": 1,
    "invalid": 2,
    "history": 1,
}


def select_orchestrator_cases(
    cases: list[AdvisorEvalCase],
    *,
    sample_size: int | None,
    case_ids: list[str] | None = None,
) -> list[AdvisorEvalCase]:
    if case_ids:
        ids = [item.strip() for item in case_ids if item.strip()]
        mapping = {case.case_id: case for case in cases}
        unknown = [item for item in ids if item not in mapping]
        if unknown:
            raise ValueError(f"Unknown orchestrator case ids: {unknown}")
        return [mapping[item] for item in ids]

    ordered = sorted(cases, key=lambda item: item.case_id)
    if sample_size is None or sample_size >= len(ordered):
        return ordered
    if sample_size == 10:
        return _select_by_quota(ordered, ORCHESTRATOR_MINI_QUOTA, sample_size)
    return ordered[: max(1, sample_size)]


def select_reedit_cases(
    cases: list[ReeditEvalCase],
    *,
    sample_size: int | None,
    case_ids: list[str] | None = None,
) -> list[ReeditEvalCase]:
    if case_ids:
        ids = [item.strip() for item in case_ids if item.strip()]
        mapping = {case.case_id: case for case in cases}
        unknown = [item for item in ids if item not in mapping]
        if unknown:
            raise ValueError(f"Unknown reedit case ids: {unknown}")
        return [mapping[item] for item in ids]

    ordered = sorted(cases, key=lambda item: item.case_id)
    if sample_size is None:
        return ordered
    if sample_size == 6:
        return _select_by_quota(ordered, REEDIT_MINI_QUOTA, sample_size)
    return ordered[: max(1, sample_size)]


def _select_by_quota(cases: list, quota: dict[str, int], sample_size: int) -> list:
    selected: list = []
    by_category: dict[str, list] = {}
    for case in cases:
        by_category.setdefault(case.category, []).append(case)
    for category, need in quota.items():
        picks = by_category.get(category, [])
        selected.extend(picks[:need])
    if len(selected) < sample_size:
        selected_ids = {case.case_id for case in selected}
        for case in cases:
            if case.case_id in selected_ids:
                continue
            selected.append(case)
            if len(selected) >= sample_size:
                break
    return selected[:sample_size]
