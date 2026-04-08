"""Open DeepSearch engine for ScholarPath.

Handles multi-source data acquisition, entity alignment, conflict
detection, canonical merge, and source-value orchestration.
"""

from scholarpath.search.orchestrator import DeepSearchOrchestrator, DeepSearchResult

__all__ = [
    "DeepSearchOrchestrator",
    "DeepSearchResult",
]
