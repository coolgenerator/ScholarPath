"""Warning capture helpers for causal training/eval pipelines."""

from __future__ import annotations

import warnings
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

VALID_WARNING_MODES = {"count_silent", "silent", "verbose"}


def normalize_warning_mode(raw: str | None) -> str:
    mode = str(raw or "count_silent").strip().lower()
    if mode not in VALID_WARNING_MODES:
        raise ValueError(
            "warning_mode must be one of: count_silent, silent, verbose",
        )
    return mode


def _detect_warning_family(item: warnings.WarningMessage) -> str:
    text = str(item.message).lower()
    category_module = getattr(item.category, "__module__", "") or ""
    filename = (item.filename or "").lower()
    if "econml" in text or "econml" in category_module or "econml" in filename:
        return "econml"
    if "sklearn" in text or "sklearn" in category_module or "sklearn" in filename:
        return "sklearn"
    if "pandas" in text or "pandas" in category_module or "pandas" in filename:
        return "pandas"
    return "other"


@dataclass(slots=True)
class WarningAudit:
    """Collect warning counts across stages."""

    by_stage: dict[str, int] = field(default_factory=dict)
    by_family: dict[str, int] = field(default_factory=dict)
    total: int = 0

    def add(self, *, stage: str, captured: list[warnings.WarningMessage]) -> None:
        if not captured:
            return
        count = len(captured)
        self.total += count
        self.by_stage[stage] = self.by_stage.get(stage, 0) + count

        family_counter: dict[str, int] = defaultdict(int)
        for item in captured:
            family_counter[_detect_warning_family(item)] += 1
        for family, family_count in family_counter.items():
            self.by_family[family] = self.by_family.get(family, 0) + int(family_count)

    def snapshot(self) -> dict[str, Any]:
        return {
            "warnings_total": int(self.total),
            "warnings_by_stage": dict(sorted(self.by_stage.items())),
            "warnings_by_family": dict(sorted(self.by_family.items())),
        }


@contextmanager
def capture_stage_warnings(
    *,
    stage: str,
    warning_mode: str,
    audit: WarningAudit | None,
):
    """Capture warnings for a stage according to mode."""
    mode = normalize_warning_mode(warning_mode)
    if mode == "verbose":
        yield
        return

    with warnings.catch_warnings(record=True) as captured:
        if mode == "count_silent":
            warnings.simplefilter("always")
        else:
            warnings.simplefilter("ignore")
        yield
    if mode == "count_silent" and audit is not None:
        audit.add(stage=stage, captured=captured)
