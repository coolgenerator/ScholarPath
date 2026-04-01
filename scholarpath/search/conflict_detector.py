"""Cross-source conflict detection."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field

from scholarpath.llm import LLMClient
from scholarpath.search.entity_aligner import AlignedEntity

logger = logging.getLogger(__name__)

_NUMERIC_CONFLICT_THRESHOLD = 0.15  # 15% relative difference

_CONFLICT_SYSTEM_PROMPT = """\
You are a data conflict assessor for college admissions research.
Given two or more differing values for the same variable from different
sources, assess the severity and recommend a resolution.

Return JSON:
{
  "severity": "low" | "medium" | "high",
  "recommended_resolution": "<brief explanation of which value to trust and why>"
}
"""


@dataclass
class ConflictRecord:
    """A detected conflict between sources for a single variable."""

    school: str
    variable: str
    sources: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    severity: str = "low"  # "low" | "medium" | "high"
    recommended_resolution: str = ""


class ConflictDetector:
    """Detects and assesses conflicts across data sources."""

    def __init__(self, llm: LLMClient | None = None) -> None:
        self._llm = llm

    async def detect(
        self,
        aligned_entities: list[AlignedEntity],
    ) -> list[ConflictRecord]:
        """Scan aligned entities for cross-source conflicts."""
        conflicts: list[ConflictRecord] = []

        for entity in aligned_entities:
            # Group data points by variable_name.
            var_groups: dict[str, list] = defaultdict(list)
            for dp in entity.data_points:
                var_groups[dp.variable_name].append(dp)

            for variable, data_points in var_groups.items():
                if len(data_points) < 2:
                    continue

                # Check if all values agree.
                unique_sources = [dp.source_name for dp in data_points]
                unique_values = [dp.value_text for dp in data_points]

                # Attempt numeric comparison first.
                numerics = [dp.value_numeric for dp in data_points if dp.value_numeric is not None]
                if len(numerics) >= 2:
                    conflict = self._check_numeric_conflict(
                        entity.canonical_name, variable, data_points,
                    )
                    if conflict:
                        conflicts.append(conflict)
                        continue

                # Non-numeric: check text equality.
                distinct_values = set(unique_values)
                if len(distinct_values) > 1:
                    conflict = await self._assess_text_conflict(
                        entity.canonical_name,
                        variable,
                        unique_sources,
                        unique_values,
                    )
                    conflicts.append(conflict)

        return conflicts

    # ------------------------------------------------------------------
    # Numeric conflict
    # ------------------------------------------------------------------

    @staticmethod
    def _check_numeric_conflict(
        school: str,
        variable: str,
        data_points: list,
    ) -> ConflictRecord | None:
        """Return a ConflictRecord if numeric values diverge beyond threshold."""
        numerics = [
            (dp.source_name, dp.value_numeric)
            for dp in data_points
            if dp.value_numeric is not None
        ]
        if len(numerics) < 2:
            return None

        values = [v for _, v in numerics]
        max_val = max(abs(v) for v in values) or 1.0
        spread = max(values) - min(values)
        relative_diff = spread / max_val

        if relative_diff <= _NUMERIC_CONFLICT_THRESHOLD:
            return None

        # Determine severity.
        if relative_diff > 0.50:
            severity = "high"
        elif relative_diff > 0.25:
            severity = "medium"
        else:
            severity = "low"

        # Recommend the highest-confidence source.
        best = max(data_points, key=lambda dp: dp.confidence)
        resolution = (
            f"Trust '{best.source_name}' (confidence {best.confidence:.2f}). "
            f"Relative difference: {relative_diff:.1%}."
        )

        return ConflictRecord(
            school=school,
            variable=variable,
            sources=[s for s, _ in numerics],
            values=[str(v) for _, v in numerics],
            severity=severity,
            recommended_resolution=resolution,
        )

    # ------------------------------------------------------------------
    # Text conflict (LLM-assisted)
    # ------------------------------------------------------------------

    async def _assess_text_conflict(
        self,
        school: str,
        variable: str,
        sources: list[str],
        values: list[str],
    ) -> ConflictRecord:
        """Assess a non-numeric conflict, optionally using LLM."""
        severity = "low"
        resolution = "Values differ but may represent different facets of the data."

        if self._llm:
            try:
                messages = [
                    {"role": "system", "content": _CONFLICT_SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "school": school,
                                "variable": variable,
                                "source_values": [
                                    {"source": s, "value": v}
                                    for s, v in zip(sources, values)
                                ],
                            },
                            ensure_ascii=False,
                        ),
                    },
                ]
                result = await self._llm.complete_json(messages, temperature=0.1)
                severity = result.get("severity", "low")
                resolution = result.get("recommended_resolution", resolution)
            except Exception:
                logger.exception("LLM conflict assessment failed")

        return ConflictRecord(
            school=school,
            variable=variable,
            sources=sources,
            values=values,
            severity=severity,
            recommended_resolution=resolution,
        )
