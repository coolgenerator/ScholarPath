"""Dataset and artifact IO helpers for advisor orchestrator evaluation."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scholarpath.evals.advisor_orchestrator_selection import (
    AdvisorEvalCase,
    ReeditEvalCase,
)

DEFAULT_OUTPUT_DIR = Path(".benchmarks/advisor_orchestrator")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slug_prefix(name: str) -> str:
    return name.strip().lower().replace(" ", "-")


def generate_default_orchestrator_cases() -> list[AdvisorEvalCase]:
    cases: list[AdvisorEvalCase] = []
    # Keep deterministic IDs and category distribution used by mini sampling quota.
    categories = {
        "single_intent": 10,
        "multi_over_limit": 6,
        "conflict_clarify": 4,
        "low_confidence": 4,
        "explicit_recovery": 4,
        "input_error": 6,
        "memory_degraded": 6,
    }
    capability_by_category = {
        "single_intent": "undergrad.school.recommend",
        "multi_over_limit": "offer.compare",
        "conflict_clarify": "common.clarify",
        "low_confidence": "common.clarify",
        "explicit_recovery": "undergrad.strategy.plan",
        "input_error": "common.general",
        "memory_degraded": "undergrad.school.query",
    }
    for category, count in categories.items():
        prefix = _slug_prefix(category)
        for idx in range(1, count + 1):
            case_id = f"{prefix}_{idx:02d}"
            capability = capability_by_category[category]
            prompt = (
                f"[{category}] request {idx}: "
                f"Please handle capability {capability} with concise structured output."
            )
            cases.append(
                AdvisorEvalCase(
                    case_id=case_id,
                    category=category,
                    prompt=prompt,
                    expected_capability=capability,
                )
            )
    return sorted(cases, key=lambda item: item.case_id)


def generate_default_reedit_cases() -> list[ReeditEvalCase]:
    rows: list[tuple[str, int]] = [
        ("middle", 3),
        ("edge", 2),
        ("tail", 2),
        ("invalid", 3),
        ("history", 2),
    ]
    out: list[ReeditEvalCase] = []
    for category, count in rows:
        prefix = _slug_prefix(category)
        for idx in range(1, count + 1):
            case_id = f"{prefix}_{idx:02d}"
            out.append(
                ReeditEvalCase(
                    case_id=case_id,
                    category=category,
                    original_turn=f"Original user turn for {case_id}",
                    edited_turn=f"Edited overwrite turn for {case_id}",
                )
            )
    return sorted(out, key=lambda item: item.case_id)


def serialize_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def serialize_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, default=_json_default))
            fp.write("\n")


def append_history(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def write_summary(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def asdict_safe(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    if isinstance(obj, dict):
        return dict(obj)
    raise TypeError(f"Unsupported asdict source: {type(obj)}")


def _json_default(value: Any) -> Any:
    if isinstance(value, (set, frozenset)):
        return sorted(list(value))
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    return str(value)
