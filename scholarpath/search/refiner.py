"""Recursive refinement for Open DeepSearch."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from scholarpath.search.entity_aligner import AlignedEntity
from scholarpath.search.sources.base import BaseSource, SearchResult

logger = logging.getLogger(__name__)

# Fields considered critical for a complete school profile.
_DEFAULT_CRITICAL_FIELDS = [
    "acceptance_rate",
    "sat_math_mid",
    "sat_reading_mid",
    "tuition_out_of_state",
    "median_earnings_10yr",
    "graduation_rate",
    "overall_grade",
]

_COVERAGE_TARGET = 0.80


@dataclass
class CoverageReport:
    """Coverage analysis for a single school."""

    school: str
    present_fields: list[str]
    missing_fields: list[str]
    coverage: float


class SearchRefiner:
    """Iteratively fills data gaps by issuing follow-up queries."""

    def __init__(self, sources: dict[str, BaseSource]) -> None:
        self._sources = sources

    async def refine(
        self,
        current_results: list[AlignedEntity],
        student_profile: dict,
        required_fields: list[str] | None = None,
        max_iterations: int = 3,
    ) -> list[AlignedEntity]:
        """Refine results until coverage target is met or iterations exhausted."""
        coverage_fields = required_fields or _DEFAULT_CRITICAL_FIELDS

        for iteration in range(1, max_iterations + 1):
            reports = self._analyse_coverage(current_results, coverage_fields)
            overall_coverage = self._overall_coverage(reports)

            logger.info(
                "Refine iteration %d/%d  coverage=%.1f%%",
                iteration,
                max_iterations,
                overall_coverage * 100,
            )

            if overall_coverage >= _COVERAGE_TARGET:
                logger.info("Coverage target met (%.1f%%); stopping refinement.", overall_coverage * 100)
                break

            # Identify which schools need what data.
            gap_queries = self._plan_gap_queries(reports)
            if not gap_queries:
                logger.info("No actionable gaps remain; stopping refinement.")
                break

            # Execute follow-up queries.
            new_results: list[SearchResult] = []
            for school, missing, source_name in gap_queries:
                source = self._sources.get(source_name)
                if source is None:
                    continue
                try:
                    results = await source.search(school, fields=missing)
                    # Tag results with school name for alignment.
                    for r in results:
                        if r.raw_data is None:
                            r.raw_data = {}
                        r.raw_data["queried_school"] = school
                    new_results.extend(results)
                except Exception:
                    logger.exception(
                        "Refinement query failed: school=%s source=%s", school, source_name,
                    )

            if not new_results:
                logger.info("No new data obtained; stopping refinement.")
                break

            # Merge new results into existing entities.
            current_results = self._merge(current_results, new_results)

        return current_results

    # ------------------------------------------------------------------
    # Coverage analysis
    # ------------------------------------------------------------------

    def _analyse_coverage(
        self,
        entities: list[AlignedEntity],
        required_fields: list[str],
    ) -> list[CoverageReport]:
        """Compute per-school coverage of required fields."""
        reports: list[CoverageReport] = []
        fields = required_fields or _DEFAULT_CRITICAL_FIELDS
        for entity in entities:
            present = {dp.variable_name for dp in entity.data_points}
            present_critical = [f for f in fields if f in present]
            missing_critical = [f for f in fields if f not in present]
            coverage = len(present_critical) / len(fields) if fields else 1.0
            reports.append(
                CoverageReport(
                    school=entity.canonical_name,
                    present_fields=present_critical,
                    missing_fields=missing_critical,
                    coverage=coverage,
                )
            )
        return reports

    @staticmethod
    def _overall_coverage(reports: list[CoverageReport]) -> float:
        if not reports:
            return 1.0
        return sum(r.coverage for r in reports) / len(reports)

    def _plan_gap_queries(
        self,
        reports: list[CoverageReport],
    ) -> list[tuple[str, list[str], str]]:
        """Return (school, missing_fields, source_name) triples for follow-up."""
        queries: list[tuple[str, list[str], str]] = []
        for report in reports:
            if not report.missing_fields:
                continue
            # Route numeric/official fields to college_scorecard, others elsewhere.
            official_fields = [
                f for f in report.missing_fields
                if f in {"acceptance_rate", "sat_math_mid", "sat_reading_mid",
                          "tuition_out_of_state", "median_earnings_10yr",
                          "graduation_rate", "retention_rate", "median_debt"}
            ]
            grade_fields = [
                f for f in report.missing_fields
                if f in {"overall_grade", "academics_grade", "campus_grade", "safety_grade"}
            ]
            if official_fields and "college_scorecard" in self._sources:
                queries.append((report.school, official_fields, "college_scorecard"))
            if grade_fields and "niche" in self._sources:
                queries.append((report.school, grade_fields, "niche"))
            # Fall back to web_search for anything remaining.
            remaining = [
                f for f in report.missing_fields
                if f not in official_fields and f not in grade_fields
            ]
            if remaining:
                if "web_search" in self._sources:
                    queries.append((report.school, remaining, "web_search"))
                else:
                    # If web search is unavailable, try all other available sources
                    # so we still maximize field coverage.
                    for source_name in ("college_scorecard", "niche", "ugc"):
                        if source_name in self._sources:
                            queries.append((report.school, remaining, source_name))

        deduped: list[tuple[str, list[str], str]] = []
        seen: set[tuple[str, tuple[str, ...], str]] = set()
        for school, fields, source_name in queries:
            norm = tuple(sorted(set(fields)))
            key = (school, norm, source_name)
            if key in seen:
                continue
            seen.add(key)
            deduped.append((school, list(norm), source_name))

        return deduped

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------

    @staticmethod
    def _merge(
        entities: list[AlignedEntity],
        new_results: list[SearchResult],
    ) -> list[AlignedEntity]:
        """Merge *new_results* into existing *entities* by school name."""
        entity_map: dict[str, AlignedEntity] = {e.canonical_name: e for e in entities}

        for result in new_results:
            school = "unknown"
            if result.raw_data:
                school = result.raw_data.get("queried_school", result.raw_data.get("canonical_name", "unknown"))

            if school in entity_map:
                entity_map[school].data_points.append(result)
            else:
                entity_map[school] = AlignedEntity(
                    canonical_name=school,
                    aliases=[],
                    data_points=[result],
                )

        return list(entity_map.values())
