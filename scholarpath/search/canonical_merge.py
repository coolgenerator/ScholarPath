"""Canonicalisation and deduplication helpers for DeepSearch V2."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from scholarpath.search.sources.base import SearchResult

# PRD-expanded critical fields used by default in DeepSearch V2.
PRD_EXPANDED_CRITICAL_FIELDS: list[str] = [
    "acceptance_rate",
    "sat_math_mid",
    "sat_reading_mid",
    "sat_25",
    "sat_75",
    "act_25",
    "act_75",
    "tuition_out_of_state",
    "avg_net_price",
    "graduation_rate_4yr",
    "median_earnings_10yr",
    "graduation_rate",
    "overall_grade",
    "academics_grade",
    "campus_grade",
    "safety_grade",
    "student_faculty_ratio",
    "endowment_per_student",
    "intl_student_pct",
    "campus_setting",
    "city",
    "state",
    "website_url",
]

_RATE_FIELDS = {
    "acceptance_rate",
    "graduation_rate",
    "graduation_rate_4yr",
    "retention_rate",
    "doctoral_completions_share",
    "intl_student_pct",
}

_FIELD_ALIAS_MAP: dict[str, str] = {
    "acceptance_rate": "acceptance_rate",
    "acceptance": "acceptance_rate",
    "admission_rate": "acceptance_rate",
    "admit_rate": "acceptance_rate",
    "tuition": "tuition_out_of_state",
    "tuition_oos": "tuition_out_of_state",
    "out_of_state_tuition": "tuition_out_of_state",
    "tuition_out_of_state": "tuition_out_of_state",
    "net_price": "avg_net_price",
    "average_net_price": "avg_net_price",
    "avg_net_price": "avg_net_price",
    "website_url": "website_url",
    "school_url": "website_url",
    "sat_math_mid": "sat_math_mid",
    "sat_reading_mid": "sat_reading_mid",
    "sat_25": "sat_25",
    "sat_75": "sat_75",
    "sat_verbal_mid": "sat_reading_mid",
    "overall_grade": "overall_grade",
    "academics_grade": "academics_grade",
    "academic_grade": "academics_grade",
    "campus_grade": "campus_grade",
    "safety_grade": "safety_grade",
    "student_reviews": "student_reviews",
    "student_faculty_ratio": "student_faculty_ratio",
    "endowment_per_student": "endowment_per_student",
    "international_student_pct": "intl_student_pct",
    "international_students_pct": "intl_student_pct",
    "intl_student_pct": "intl_student_pct",
    "city": "city",
    "state": "state",
    "campus_setting": "campus_setting",
    "median_earnings_10yr": "median_earnings_10yr",
    "doctoral_completions_share": "doctoral_completions_share",
    "phd_share": "doctoral_completions_share",
    "graduation_rate": "graduation_rate",
    "graduation_rate_4yr": "graduation_rate_4yr",
    "act_25": "act_25",
    "act_75": "act_75",
    "act_mid": "act_mid",
    "applicants_total": "applicants_total",
    "applications_total": "applicants_total",
    "admitted_total": "admitted_total",
    "enrolled_total": "enrolled_total",
    "yield_rate": "yield_rate",
    "application_deadline": "application_deadline",
    "application_fee": "application_fee",
}

_NUMERIC_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?")
_WHITESPACE_RE = re.compile(r"\s+")


def normalise_variable_name(name: str | None) -> str:
    raw = (name or "").strip().lower()
    if not raw:
        return "unknown"
    normalised = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    direct = _FIELD_ALIAS_MAP.get(normalised)
    if direct is not None:
        return direct

    # Heuristic mapping for noisy model outputs, e.g. "MIT acceptance rate".
    if "acceptance" in normalised and "rate" in normalised:
        return "acceptance_rate"
    if "tuition" in normalised and ("out_of_state" in normalised or normalised == "tuition"):
        return "tuition_out_of_state"
    if "net_price" in normalised:
        return "avg_net_price"
    if "intl" in normalised and "pct" in normalised:
        return "intl_student_pct"
    if "international" in normalised and "student" in normalised:
        return "intl_student_pct"
    if "graduation" in normalised and "rate" in normalised:
        return "graduation_rate"
    if "sat" in normalised and "math" in normalised and "mid" in normalised:
        return "sat_math_mid"
    if "sat" in normalised and (
        "reading" in normalised
        or "verbal" in normalised
        or "critical_reading" in normalised
    ) and "mid" in normalised:
        return "sat_reading_mid"

    return normalised


def extract_school_key(result: SearchResult) -> str:
    if result.raw_data:
        for key in ("queried_school", "canonical_name", "school_name"):
            value = result.raw_data.get(key)
            if value:
                return str(value).strip()
    return "unknown"


def coerce_numeric(value_text: str | None, *, variable_name: str) -> float | None:
    text = (value_text or "").strip()
    if not text:
        return None
    match = _NUMERIC_RE.search(text)
    if match is None:
        return None
    try:
        value = float(match.group().replace(",", ""))
    except ValueError:
        return None
    return normalise_numeric(value, variable_name=variable_name, value_text=text)


def normalise_numeric(
    value: float | None,
    *,
    variable_name: str,
    value_text: str | None = None,
) -> float | None:
    if value is None:
        return None
    out = float(value)
    if variable_name in _RATE_FIELDS:
        has_pct_marker = "%" in (value_text or "")
        if not has_pct_marker and 0 <= out <= 1:
            out *= 100
    return round(out, 6)


def normalise_text_value(value: str | None) -> str:
    return _WHITESPACE_RE.sub(" ", (value or "").strip()).lower()


def fingerprint_value(*, value_text: str | None, value_numeric: float | None) -> str:
    if value_numeric is not None:
        return f"num:{value_numeric:.6f}"
    return f"text:{normalise_text_value(value_text)}"


@dataclass
class CoverageStats:
    required_slots: int
    covered_slots: int

    @property
    def ratio(self) -> float:
        if self.required_slots <= 0:
            return 1.0
        return self.covered_slots / self.required_slots


class CanonicalMergeService:
    """Normalise fields and remove duplicate facts across sources."""

    def merge(self, results: list[SearchResult]) -> list[SearchResult]:
        best_by_key: dict[tuple[str, str, str], SearchResult] = {}
        for item in results:
            canonical = self._canonicalise(item)
            school_key = extract_school_key(canonical).lower()
            value_fp = fingerprint_value(
                value_text=canonical.value_text,
                value_numeric=canonical.value_numeric,
            )
            dedupe_key = (
                school_key,
                canonical.variable_name,
                value_fp,
            )
            current = best_by_key.get(dedupe_key)
            known_sources: set[str] = {canonical.source_name}
            if current is not None:
                known_sources.add(current.source_name)
                if current.raw_data:
                    known_sources.update(current.raw_data.get("deduped_sources", []))

            if current is None or canonical.confidence > current.confidence:
                if canonical.raw_data is None:
                    canonical.raw_data = {}
                canonical.raw_data["deduped_sources"] = sorted(known_sources)
                best_by_key[dedupe_key] = canonical
            else:
                if current.raw_data is None:
                    current.raw_data = {}
                existing = set(current.raw_data.get("deduped_sources", []))
                existing.update(known_sources)
                current.raw_data["deduped_sources"] = sorted(existing)
        return list(best_by_key.values())

    def coverage_by_school(
        self,
        results: list[SearchResult],
        required_fields: list[str],
    ) -> dict[str, set[str]]:
        required = {normalise_variable_name(field) for field in required_fields}
        covered: dict[str, set[str]] = defaultdict(set)
        for item in results:
            school = extract_school_key(item)
            variable = normalise_variable_name(item.variable_name)
            if variable in required:
                covered[school].add(variable)
        return covered

    def coverage_stats(
        self,
        *,
        school_names: list[str],
        coverage_by_school: dict[str, set[str]],
        required_fields: list[str],
    ) -> CoverageStats:
        required = {normalise_variable_name(field) for field in required_fields}
        required_slots = len(required) * len(school_names)
        covered_slots = 0
        for school in school_names:
            covered_slots += len(coverage_by_school.get(school, set()) & required)
        return CoverageStats(required_slots=required_slots, covered_slots=covered_slots)

    @staticmethod
    def _canonicalise(item: SearchResult) -> SearchResult:
        variable = normalise_variable_name(item.variable_name)
        numeric = item.value_numeric
        if numeric is None:
            numeric = coerce_numeric(item.value_text, variable_name=variable)
        else:
            numeric = normalise_numeric(
                numeric,
                variable_name=variable,
                value_text=item.value_text,
            )

        raw_data: dict[str, Any] = dict(item.raw_data or {})
        raw_data["canonical_variable"] = variable
        if numeric is not None:
            raw_data["normalised_numeric"] = numeric

        return SearchResult(
            source_name=item.source_name,
            source_type=item.source_type,
            source_url=item.source_url,
            variable_name=variable,
            value_text=item.value_text,
            value_numeric=numeric,
            confidence=item.confidence,
            sample_size=item.sample_size,
            temporal_range=item.temporal_range,
            raw_data=raw_data,
        )
