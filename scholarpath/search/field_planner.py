"""Source routing planner for DeepSearch V2."""

from __future__ import annotations

from dataclasses import dataclass

from scholarpath.search.canonical_merge import normalise_variable_name
from scholarpath.search.db_coverage import SchoolCoverageSnapshot

_OFFICIAL_FIELDS = {
    "acceptance_rate",
    "sat_math_mid",
    "sat_reading_mid",
    "tuition_out_of_state",
    "avg_net_price",
    "median_earnings_10yr",
    "graduation_rate",
    "intl_student_pct",
    "city",
    "state",
}

_GRADE_FIELDS = {
    "overall_grade",
    "academics_grade",
    "campus_grade",
    "safety_grade",
    "campus_setting",
}

_UGC_FIELDS = {
    "admission_experience",
    "campus_life_review",
}


@dataclass
class SourcePlan:
    school_name: str
    source_name: str
    fields: list[str]


class FieldCoveragePlanner:
    """Route missing fields to the cheapest available sources first."""

    def plan_wave_b(
        self,
        *,
        coverage: dict[str, SchoolCoverageSnapshot],
        required_fields: list[str],
        available_sources: set[str],
        source_priority: dict[str, float] | None = None,
    ) -> dict[str, list[SourcePlan]]:
        required = {normalise_variable_name(field) for field in required_fields}
        plans: dict[str, list[SourcePlan]] = {}
        for school, snapshot in coverage.items():
            missing = set(snapshot.missing_fields) & required
            if not missing:
                plans[school] = []
                continue

            school_plans: list[SourcePlan] = []
            assigned: set[str] = set()

            official = sorted(missing & _OFFICIAL_FIELDS)
            if official and "college_scorecard" in available_sources:
                school_plans.append(
                    SourcePlan(school_name=school, source_name="college_scorecard", fields=official)
                )
                assigned.update(official)

            grades = sorted((missing - assigned) & _GRADE_FIELDS)
            if grades and "niche" in available_sources:
                school_plans.append(
                    SourcePlan(school_name=school, source_name="niche", fields=grades)
                )
                assigned.update(grades)

            ugc_fields = sorted((missing - assigned) & _UGC_FIELDS)
            if ugc_fields and "ugc" in available_sources:
                school_plans.append(
                    SourcePlan(school_name=school, source_name="ugc", fields=ugc_fields)
                )
                assigned.update(ugc_fields)

            remaining = sorted(missing - assigned)
            if remaining:
                if "web_search" in available_sources:
                    school_plans.append(
                        SourcePlan(school_name=school, source_name="web_search", fields=remaining)
                    )
                else:
                    fallback = self._pick_best_source(
                        candidates=("college_scorecard", "niche", "ugc"),
                        available_sources=available_sources,
                        source_priority=source_priority,
                    )
                    if fallback is not None:
                        school_plans.append(
                            SourcePlan(
                                school_name=school,
                                source_name=fallback,
                                fields=remaining,
                            )
                        )

            plans[school] = self._dedupe_by_source(school_plans)

        return plans

    @staticmethod
    def _dedupe_by_source(plans: list[SourcePlan]) -> list[SourcePlan]:
        fields_by_source: dict[str, set[str]] = {}
        school_name = plans[0].school_name if plans else ""
        for plan in plans:
            bucket = fields_by_source.setdefault(plan.source_name, set())
            bucket.update(normalise_variable_name(field) for field in plan.fields)
        merged: list[SourcePlan] = []
        for source_name, fields in fields_by_source.items():
            merged.append(
                SourcePlan(
                    school_name=school_name,
                    source_name=source_name,
                    fields=sorted(fields),
                )
            )
        return merged

    @staticmethod
    def _pick_best_source(
        *,
        candidates: tuple[str, ...],
        available_sources: set[str],
        source_priority: dict[str, float] | None,
    ) -> str | None:
        ranked = [source for source in candidates if source in available_sources]
        if not ranked:
            return None
        if not source_priority:
            return ranked[0]
        ranked.sort(key=lambda source: (-source_priority.get(source, 0.0), source))
        return ranked[0]
