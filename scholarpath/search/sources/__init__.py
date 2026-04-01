"""Data sources for Open DeepSearch."""

from scholarpath.search.sources.base import BaseSource, SearchResult
from scholarpath.search.sources.college_scorecard import CollegeScorecardSource
from scholarpath.search.sources.niche import NicheSource
from scholarpath.search.sources.ugc import UGCSource
from scholarpath.search.sources.web_search import WebSearchSource

__all__ = [
    "BaseSource",
    "SearchResult",
    "CollegeScorecardSource",
    "NicheSource",
    "UGCSource",
    "WebSearchSource",
]
