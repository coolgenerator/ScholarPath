"""Cross-source entity alignment."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from scholarpath.llm import LLMClient
from scholarpath.search.sources.base import SearchResult

logger = logging.getLogger(__name__)

# Static alias table for fast deterministic matching.
_KNOWN_ALIASES: dict[str, str] = {
    # English variants
    "MIT": "Massachusetts Institute of Technology",
    "Stanford": "Stanford University",
    "Harvard": "Harvard University",
    "UCB": "University of California, Berkeley",
    "UC Berkeley": "University of California, Berkeley",
    "UCSB": "University of California, Santa Barbara",
    "UC Santa Barbara": "University of California, Santa Barbara",
    "UCLA": "University of California, Los Angeles",
    "UC Los Angeles": "University of California, Los Angeles",
    "CMU": "Carnegie Mellon University",
    "UMich": "University of Michigan",
    "University of Michigan - Ann Arbor": "University of Michigan",
    # Chinese name variants
    "麻省理工学院": "Massachusetts Institute of Technology",
    "斯坦福大学": "Stanford University",
    "哈佛大学": "Harvard University",
    "加州大学伯克利分校": "University of California, Berkeley",
    "加州大学圣塔芭芭拉分校": "University of California, Santa Barbara",
    "加州大学洛杉矶分校": "University of California, Los Angeles",
    "卡内基梅隆大学": "Carnegie Mellon University",
    "密歇根大学": "University of Michigan",
}

_ALIGNMENT_SYSTEM_PROMPT = """\
You are a university name alignment assistant. Given a list of university
name strings, group them so that each group refers to the same institution.

Return JSON: {"groups": [{"canonical": "<full official name>", "aliases": ["<name1>", ...]}]}

Only group names that truly refer to the same school. If unsure, keep them
separate. Include ALL input names in exactly one group.
"""


@dataclass
class AlignedEntity:
    """A school entity with all its name variants and collected data points."""

    canonical_name: str
    aliases: list[str] = field(default_factory=list)
    data_points: list[SearchResult] = field(default_factory=list)


class EntityAligner:
    """Groups search results by school, resolving name variations."""

    def __init__(self, llm: LLMClient | None = None) -> None:
        self._llm = llm

    async def align(self, results: list[SearchResult]) -> list[AlignedEntity]:
        """Group *results* by school entity."""
        # Step 1: Collect all unique "school names" mentioned.
        # We infer school names from the query context; since SearchResult
        # doesn't carry the queried school name directly, we group by
        # source_url + raw_data hints.  In practice the orchestrator passes
        # results already tagged per-school, so we rely on a school_name
        # field in raw_data when available.
        name_to_results: dict[str, list[SearchResult]] = {}
        for r in results:
            school_name = self._extract_school_name(r)
            canonical = self._static_canonicalise(school_name)
            name_to_results.setdefault(canonical, []).append(r)

        # Step 2: Check for remaining ambiguities via LLM fuzzy matching.
        canonical_names = list(name_to_results.keys())
        if self._llm and len(canonical_names) > 1:
            merged_map = await self._llm_merge(canonical_names)
            name_to_results = self._apply_merge(name_to_results, merged_map)

        # Step 3: Build AlignedEntity list.
        entities: list[AlignedEntity] = []
        for canonical, data_points in name_to_results.items():
            aliases = list(
                {self._extract_school_name(r) for r in data_points} - {canonical}
            )
            entities.append(
                AlignedEntity(
                    canonical_name=canonical,
                    aliases=sorted(aliases),
                    data_points=data_points,
                )
            )
        return entities

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_school_name(result: SearchResult) -> str:
        """Best-effort extraction of school name from a SearchResult."""
        if result.raw_data:
            for key in ("canonical_name", "school_name", "queried_school"):
                val = result.raw_data.get(key)
                if val:
                    return str(val)
        return "unknown"

    @staticmethod
    def _static_canonicalise(name: str) -> str:
        """Use the static alias table for fast resolution."""
        return _KNOWN_ALIASES.get(name, name)

    async def _llm_merge(
        self, names: list[str],
    ) -> dict[str, str]:
        """Ask LLM to merge names that refer to the same school.

        Returns a mapping from input name -> canonical name.
        """
        assert self._llm is not None
        messages = [
            {"role": "system", "content": _ALIGNMENT_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(names, ensure_ascii=False)},
        ]
        try:
            result = await self._llm.complete_json(
                messages,
                temperature=0.0,
                caller="search.entity_align",
            )
        except Exception:
            logger.exception("LLM entity alignment failed; skipping merge")
            return {n: n for n in names}

        mapping: dict[str, str] = {}
        for group in result.get("groups", []):
            canonical = group.get("canonical", "")
            for alias in group.get("aliases", []):
                mapping[alias] = canonical
            mapping[canonical] = canonical
        # Ensure every input name is accounted for.
        for name in names:
            mapping.setdefault(name, name)
        return mapping

    @staticmethod
    def _apply_merge(
        name_to_results: dict[str, list[SearchResult]],
        merged_map: dict[str, str],
    ) -> dict[str, list[SearchResult]]:
        """Re-key *name_to_results* according to *merged_map*."""
        merged: dict[str, list[SearchResult]] = {}
        for old_name, data_points in name_to_results.items():
            new_name = merged_map.get(old_name, old_name)
            merged.setdefault(new_name, []).extend(data_points)
        return merged
