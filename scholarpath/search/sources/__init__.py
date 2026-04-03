"""Data sources for Open DeepSearch."""

from scholarpath.search.sources.base import BaseSource, SearchResult
from scholarpath.search.sources.college_scorecard import CollegeScorecardSource
from scholarpath.search.sources.cds_parser import CommonDataSetSource
from scholarpath.search.sources.ipeds_college_navigator import IPEDSCollegeNavigatorSource
from scholarpath.search.sources.internal_web_search import InternalWebSearchSource
from scholarpath.search.sources.niche import NicheSource
from scholarpath.search.sources.school_official_profile import SchoolOfficialProfileSource
from scholarpath.search.sources.ugc import UGCSource
from scholarpath.search.sources.web_search import WebSearchSource

__all__ = [
    "BaseSource",
    "SearchResult",
    "CollegeScorecardSource",
    "CommonDataSetSource",
    "IPEDSCollegeNavigatorSource",
    "InternalWebSearchSource",
    "NicheSource",
    "SchoolOfficialProfileSource",
    "UGCSource",
    "WebSearchSource",
]
