"""User-generated content source (simulated)."""

from __future__ import annotations

import logging
from typing import Any

from scholarpath.search.sources.base import BaseSource, SearchResult

logger = logging.getLogger(__name__)

# Simulated UGC data modelling posts from platforms like 小红书 and 一亩三分地.
_UGC_DATA: dict[str, list[dict[str, Any]]] = {
    "MIT": [
        {
            "variable_name": "admission_experience",
            "value_text": (
                "Early Action admit. GPA 3.97, SAT 1580, strong math competition "
                "background. Two research papers. Interview was conversational."
            ),
            "platform": "一亩三分地",
            "sample_size": 1,
        },
        {
            "variable_name": "campus_life_review",
            "value_text": "课业压力很大但peer support非常好，UROP本科研究机会丰富。",
            "platform": "小红书",
            "sample_size": 1,
        },
    ],
    "Stanford University": [
        {
            "variable_name": "admission_experience",
            "value_text": (
                "REA admit. Perfect GPA, SAT 1560, founded a non-profit. "
                "Essays were the most important part, focus on storytelling."
            ),
            "platform": "一亩三分地",
            "sample_size": 1,
        },
        {
            "variable_name": "campus_life_review",
            "value_text": "阳光充足，创业氛围浓厚，CS课程质量顶级但选课竞争激烈。",
            "platform": "小红书",
            "sample_size": 1,
        },
    ],
    "UC Berkeley": [
        {
            "variable_name": "admission_experience",
            "value_text": (
                "Admitted to L&S CS. In-state, GPA 4.3W, SAT 1520. "
                "Strong extracurriculars in robotics. No interview."
            ),
            "platform": "一亩三分地",
            "sample_size": 1,
        },
        {
            "variable_name": "campus_life_review",
            "value_text": "学术资源丰富但竞争激烈，周边生活便利，Cal Day很精彩。",
            "platform": "小红书",
            "sample_size": 1,
        },
    ],
    "Harvard University": [
        {
            "variable_name": "admission_experience",
            "value_text": (
                "REA admit, legacy. GPA 3.95, SAT 1570. Debate captain, "
                "community service leadership. Alumni interview was important."
            ),
            "platform": "一亩三分地",
            "sample_size": 1,
        },
    ],
    "Carnegie Mellon University": [
        {
            "variable_name": "admission_experience",
            "value_text": (
                "SCS admit. GPA 3.92, SAT 1550, multiple CS competition awards. "
                "Supplemental essays about why CMU were critical."
            ),
            "platform": "一亩三分地",
            "sample_size": 1,
        },
        {
            "variable_name": "campus_life_review",
            "value_text": "课业量惊人但同学互助氛围好，匹兹堡冬天较冷但生活成本低。",
            "platform": "小红书",
            "sample_size": 1,
        },
    ],
}

_ALIASES: dict[str, str] = {
    "Massachusetts Institute of Technology": "MIT",
    "UCSB": "UC Santa Barbara",
    "UCB": "UC Berkeley",
    "Harvard": "Harvard University",
    "Stanford": "Stanford University",
    "CMU": "Carnegie Mellon University",
    "哈佛大学": "Harvard University",
    "斯坦福大学": "Stanford University",
    "卡内基梅隆大学": "Carnegie Mellon University",
    "加州大学伯克利分校": "UC Berkeley",
}


class UGCSource(BaseSource):
    """Simulated user-generated content from Chinese student platforms."""

    name = "ugc"
    source_type = "ugc"

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        canonical = _ALIASES.get(school_name, school_name)
        entries = _UGC_DATA.get(canonical, [])
        if not entries:
            logger.debug("UGC: no simulated data for '%s'", school_name)
            return []

        results: list[SearchResult] = []
        for entry in entries:
            var_name = entry["variable_name"]
            if fields and var_name not in fields:
                continue
            platform = entry.get("platform", "unknown")
            results.append(
                SearchResult(
                    source_name=self.name,
                    source_type=self.source_type,
                    source_url=f"https://{platform}.example.com",
                    variable_name=var_name,
                    value_text=entry["value_text"],
                    value_numeric=None,
                    confidence=0.35,
                    sample_size=entry.get("sample_size", 1),
                    temporal_range=None,
                    raw_data={"platform": platform, "canonical_name": canonical},
                )
            )
        return results
