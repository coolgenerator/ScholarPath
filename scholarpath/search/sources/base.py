"""Abstract base source and shared data structures."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """A single data point returned by a source."""

    source_name: str
    source_type: str  # "official" | "proxy" | "ugc"
    source_url: str
    variable_name: str
    value_text: str
    value_numeric: float | None = None
    confidence: float = 0.5
    sample_size: int | None = None
    temporal_range: str | None = None
    raw_data: dict[str, Any] | None = field(default=None, repr=False)


class BaseSource(ABC):
    """Interface that every search source must implement."""

    name: str = "base"
    source_type: str = "unknown"

    @abstractmethod
    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        """Query the source for *school_name* and return structured results.

        If *fields* is provided, the source should attempt to limit its
        response to those variable names.
        """

    async def health_check(self) -> bool:
        """Return ``True`` if the source is reachable and ready."""
        return True
