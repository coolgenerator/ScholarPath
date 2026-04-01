"""Open DeepSearch engine for ScholarPath.

Handles multi-source data acquisition, entity alignment, conflict
detection, and recursive refinement for college admissions research.
"""

from scholarpath.search.orchestrator import DeepSearchOrchestrator, DeepSearchResult

__all__ = [
    "DeepSearchOrchestrator",
    "DeepSearchResult",
]
