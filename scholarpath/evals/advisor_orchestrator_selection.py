"""Case selection helpers for advisor orchestrator eval datasets."""

from __future__ import annotations

from typing import Any


def select_eval_cases(
    ordered_cases: list[Any],
    *,
    sample_size: int,
    case_ids: list[str] | None,
    mini_quotas: dict[str, int],
) -> list[Any]:
    if case_ids:
        index = {case.case_id: case for case in ordered_cases}
        missing = [case_id for case_id in case_ids if case_id not in index]
        if missing:
            raise ValueError(f"Unknown case_ids: {missing}")
        return [index[case_id] for case_id in case_ids]

    if sample_size <= 0:
        raise ValueError("sample_size must be > 0")
    if sample_size > len(ordered_cases):
        raise ValueError(
            f"sample_size={sample_size} exceeds dataset size={len(ordered_cases)}",
        )

    mini_size = sum(mini_quotas.values())
    if sample_size == mini_size:
        return select_stratified_cases(
            ordered_cases,
            quotas=mini_quotas,
            sample_size=mini_size,
            label="orchestrator",
        )

    return ordered_cases[:sample_size]


def select_reedit_cases(
    ordered_cases: list[Any],
    *,
    sample_size: int | None,
    case_ids: list[str] | None,
    mini_quotas: dict[str, int],
) -> list[Any]:
    if case_ids:
        index = {case.case_id: case for case in ordered_cases}
        missing = [case_id for case_id in case_ids if case_id not in index]
        if missing:
            raise ValueError(f"Unknown reedit_case_ids: {missing}")
        return [index[case_id] for case_id in case_ids]

    if sample_size is None:
        return list(ordered_cases)
    if sample_size <= 0:
        raise ValueError("reedit_sample_size must be > 0")
    if sample_size > len(ordered_cases):
        raise ValueError(
            f"reedit_sample_size={sample_size} exceeds dataset size={len(ordered_cases)}",
        )

    mini_size = sum(mini_quotas.values())
    if sample_size == mini_size:
        return select_stratified_cases(
            ordered_cases,
            quotas=mini_quotas,
            sample_size=mini_size,
            label="reedit",
        )

    return ordered_cases[:sample_size]


def select_stratified_cases(
    ordered_cases: list[Any],
    *,
    quotas: dict[str, int],
    sample_size: int,
    label: str,
) -> list[Any]:
    by_category: dict[str, list[Any]] = {key: [] for key in quotas}
    for case in ordered_cases:
        category = next((tag for tag in case.tags if tag in quotas), None)
        if category is not None:
            by_category[category].append(case)

    selected: list[Any] = []
    for category, take in quotas.items():
        bucket = by_category.get(category, [])
        if len(bucket) < take:
            raise ValueError(
                f"{label} mini sampling requires {take} cases for {category}, got {len(bucket)}",
            )
        selected.extend(bucket[:take])

    if len(selected) != sample_size:
        raise ValueError(
            f"{label} mini sampling expected {sample_size} cases, got {len(selected)}",
        )
    return selected

