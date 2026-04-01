"""Niche.com simulated data source."""

from __future__ import annotations

import logging
from typing import Any

from scholarpath.search.sources.base import BaseSource, SearchResult

logger = logging.getLogger(__name__)

# Simulated Niche data for well-known schools.
_NICHE_DATA: dict[str, dict[str, Any]] = {
    "MIT": {
        "overall_grade": "A+",
        "academics_grade": "A+",
        "campus_grade": "A",
        "safety_grade": "B+",
        "student_reviews": "Extremely rigorous academics with collaborative culture.",
        "niche_url": "https://www.niche.com/colleges/massachusetts-institute-of-technology/",
    },
    "Stanford University": {
        "overall_grade": "A+",
        "academics_grade": "A+",
        "campus_grade": "A+",
        "safety_grade": "A-",
        "student_reviews": "Beautiful campus, world-class research opportunities.",
        "niche_url": "https://www.niche.com/colleges/stanford-university/",
    },
    "UC Berkeley": {
        "overall_grade": "A+",
        "academics_grade": "A+",
        "campus_grade": "B+",
        "safety_grade": "B",
        "student_reviews": "Top public university with vibrant campus life.",
        "niche_url": "https://www.niche.com/colleges/university-of-california---berkeley/",
    },
    "UC Santa Barbara": {
        "overall_grade": "A",
        "academics_grade": "A",
        "campus_grade": "A+",
        "safety_grade": "B+",
        "student_reviews": "Beach campus with strong STEM programs.",
        "niche_url": "https://www.niche.com/colleges/university-of-california---santa-barbara/",
    },
    "UCLA": {
        "overall_grade": "A+",
        "academics_grade": "A+",
        "campus_grade": "A",
        "safety_grade": "B+",
        "student_reviews": "Great social scene and strong athletics alongside academics.",
        "niche_url": "https://www.niche.com/colleges/university-of-california---los-angeles/",
    },
    "Harvard University": {
        "overall_grade": "A+",
        "academics_grade": "A+",
        "campus_grade": "A",
        "safety_grade": "A-",
        "student_reviews": "Unmatched network and resources, intense academic environment.",
        "niche_url": "https://www.niche.com/colleges/harvard-university/",
    },
    "Carnegie Mellon University": {
        "overall_grade": "A+",
        "academics_grade": "A+",
        "campus_grade": "B+",
        "safety_grade": "B+",
        "student_reviews": "Top CS and engineering programs, heavy workload.",
        "niche_url": "https://www.niche.com/colleges/carnegie-mellon-university/",
    },
    "University of Michigan": {
        "overall_grade": "A+",
        "academics_grade": "A+",
        "campus_grade": "A",
        "safety_grade": "B+",
        "student_reviews": "Big Ten experience with excellent academics.",
        "niche_url": "https://www.niche.com/colleges/university-of-michigan---ann-arbor/",
    },
}

# Simple aliases for name normalisation before lookup.
_ALIASES: dict[str, str] = {
    "Massachusetts Institute of Technology": "MIT",
    "UCSB": "UC Santa Barbara",
    "University of California, Santa Barbara": "UC Santa Barbara",
    "加州大学圣塔芭芭拉分校": "UC Santa Barbara",
    "UCB": "UC Berkeley",
    "University of California, Berkeley": "UC Berkeley",
    "加州大学伯克利分校": "UC Berkeley",
    "University of California, Los Angeles": "UCLA",
    "加州大学洛杉矶分校": "UCLA",
    "哈佛大学": "Harvard University",
    "Harvard": "Harvard University",
    "斯坦福大学": "Stanford University",
    "Stanford": "Stanford University",
    "CMU": "Carnegie Mellon University",
    "卡内基梅隆大学": "Carnegie Mellon University",
    "UMich": "University of Michigan",
    "密歇根大学": "University of Michigan",
}

_GRADE_FIELDS = [
    "overall_grade",
    "academics_grade",
    "campus_grade",
    "safety_grade",
    "student_reviews",
]


class NicheSource(BaseSource):
    """Niche.com simulated scraper source."""

    name = "niche"
    source_type = "proxy"

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        canonical = _ALIASES.get(school_name, school_name)
        school_data = _NICHE_DATA.get(canonical)
        if school_data is None:
            logger.debug("Niche: no simulated data for '%s'", school_name)
            return []

        target_fields = fields if fields else _GRADE_FIELDS
        niche_url = school_data.get("niche_url", "https://www.niche.com")
        results: list[SearchResult] = []

        for field_name in target_fields:
            value = school_data.get(field_name)
            if value is None:
                continue
            results.append(
                SearchResult(
                    source_name=self.name,
                    source_type=self.source_type,
                    source_url=niche_url,
                    variable_name=field_name,
                    value_text=str(value),
                    value_numeric=None,
                    confidence=0.65,
                    sample_size=None,
                    temporal_range=None,
                    raw_data={"canonical_name": canonical},
                )
            )
        return results
