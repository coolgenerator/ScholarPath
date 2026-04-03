"""Warning capture helpers for training/eval stages."""

from __future__ import annotations

import warnings
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field


@dataclass(slots=True)
class WarningAudit:
    mode: str = "count_silent"
    counter: Counter[str] = field(default_factory=Counter)

    def observe(self, category_name: str) -> None:
        self.counter[category_name] += 1

    def summary(self) -> dict[str, int]:
        return dict(self.counter)


@contextmanager
def capture_warnings(audit: WarningAudit):
    """Capture warnings according to audit mode.

    Modes:
    - count_silent: suppress terminal spam, keep counts.
    - silent: suppress and do not count.
    - verbose: pass through.
    """
    mode = (audit.mode or "count_silent").strip().lower()
    if mode == "verbose":
        yield
        return

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        yield
        if mode == "count_silent":
            for item in captured:
                category = getattr(item.category, "__name__", "Warning")
                audit.observe(str(category))
