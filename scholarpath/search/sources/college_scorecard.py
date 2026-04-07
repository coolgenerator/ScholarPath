"""College Scorecard API source (api.data.gov)."""

from __future__ import annotations

import logging
import re
from typing import Any

import httpx

from scholarpath.search.sources.base import BaseSource, SearchResult

logger = logging.getLogger(__name__)

# Mapping from College Scorecard API field paths to our canonical variable names.
_FIELD_MAP: dict[str, str] = {
    "latest.admissions.admission_rate.overall": "acceptance_rate",
    "latest.admissions.sat_scores.25th_percentile.critical_reading": "sat_reading_25",
    "latest.admissions.sat_scores.75th_percentile.critical_reading": "sat_reading_75",
    "latest.admissions.sat_scores.25th_percentile.math": "sat_math_25",
    "latest.admissions.sat_scores.75th_percentile.math": "sat_math_75",
    "latest.admissions.sat_scores.midpoint.critical_reading": "sat_reading_mid",
    "latest.admissions.sat_scores.midpoint.math": "sat_math_mid",
    "latest.cost.tuition.in_state": "tuition_in_state",
    "latest.cost.tuition.out_of_state": "tuition_out_of_state",
    "latest.cost.avg_net_price.overall": "avg_net_price",
    "latest.cost.attendance.academic_year": "cost_of_attendance",
    "latest.student.size": "enrollment",
    "latest.student.demographics.student_faculty_ratio": "student_faculty_ratio",
    "latest.student.demographics.race_ethnicity.white": "pct_white",
    "latest.student.retention_rate.four_year.full_time": "retention_rate",
    "latest.completion.rate_suppressed.overall": "graduation_rate_4yr",
    "latest.earnings.10_yrs_after_entry.median": "median_earnings_10yr",
    "latest.aid.median_debt.completers.overall": "median_debt",
    "latest.school.endowment.end": "endowment_total",
    "school.city": "city",
    "school.state": "state",
    "school.school_url": "website_url",
}

# Reverse lookup: our variable name -> API field
_REVERSE_MAP: dict[str, str] = {v: k for k, v in _FIELD_MAP.items()}

_API_BASE = "https://api.data.gov/ed/collegescorecard/v1/schools.json"
_PERCENT_FIELDS = {"acceptance_rate", "retention_rate", "graduation_rate", "pct_white"}
_NON_WORD_RE = re.compile(r"[^a-z0-9]+")


def _school_name_candidates(name: str) -> list[str]:
    raw = str(name or "").strip()
    if not raw:
        return []
    candidates = [raw]
    no_comma = re.sub(r"\s*,\s*", " ", raw).strip()
    if no_comma and no_comma not in candidates:
        candidates.append(no_comma)
    normalized = re.sub(r"\s+", " ", no_comma or raw).strip()
    if normalized and normalized not in candidates:
        candidates.append(normalized)
    return candidates


class CollegeScorecardSource(BaseSource):
    """Official U.S. Department of Education College Scorecard data."""

    name = "college_scorecard"
    source_type = "official"

    def __init__(self, api_key: str) -> None:
        key = (api_key or "").strip()
        if not key:
            raise ValueError("College Scorecard API key is required")
        self._api_key = key

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        requested_fields = {str(field).strip() for field in (fields or []) if str(field).strip()}
        if not requested_fields:
            requested_fields = set(_FIELD_MAP.values()) | {
                "sat_25",
                "sat_75",
                "endowment_per_student",
            }

        # Determine which API fields to request.
        api_fields: set[str] = set()
        for requested in requested_fields:
            if requested in _REVERSE_MAP:
                api_fields.add(_REVERSE_MAP[requested])
        # Always include dependency fields for derived official values.
        api_fields.update(
            {
                "latest.school.endowment.end",
                "latest.student.size",
                "latest.cost.avg_net_price.overall",
                "latest.student.demographics.student_faculty_ratio",
                "latest.completion.rate_suppressed.overall",
                "school.school_url",
            }
        )
        # SAT percentile aggregates are derived from the component score fields.
        if "sat_25" in requested_fields or "sat_75" in requested_fields:
            api_fields.update(
                {
                    "latest.admissions.sat_scores.25th_percentile.critical_reading",
                    "latest.admissions.sat_scores.25th_percentile.math",
                    "latest.admissions.sat_scores.75th_percentile.critical_reading",
                    "latest.admissions.sat_scores.75th_percentile.math",
                }
            )

        # Always include the school name for identification.
        api_fields_csv = ",".join(["school.name", "id"] + sorted(api_fields))

        results: list[SearchResult] = []
        records: list[dict[str, Any]] = []
        chosen_query = school_name
        candidates = _school_name_candidates(school_name)
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                for candidate in candidates:
                    params: dict[str, Any] = {
                        "school.name": candidate,
                        "fields": api_fields_csv,
                        "api_key": self._api_key,
                        "per_page": 5,
                    }
                    try:
                        resp = await client.get(_API_BASE, params=params)
                        resp.raise_for_status()
                    except httpx.HTTPError as exc:
                        logger.warning("College Scorecard request failed: %s", exc)
                        continue
                    data = resp.json()
                    records = _select_best_records(
                        data.get("results", []),
                        school_name=candidate,
                        api_fields=api_fields,
                    )
                    if records:
                        chosen_query = candidate
                        break
        except httpx.HTTPError as exc:
            logger.warning("College Scorecard request failed: %s", exc)
            return results
        if not records:
            return results

        for record in records:
            school_id = record.get("id", "")
            source_url = f"https://collegescorecard.ed.gov/school/?{school_id}"
            for api_field, var_name in _FIELD_MAP.items():
                if var_name not in requested_fields or api_field not in api_fields:
                    continue
                value = _deep_get(record, api_field)
                if value is None:
                    continue
                numeric = float(value) if _is_numeric(value) else None
                value_text = str(value)
                if numeric is not None and var_name in _PERCENT_FIELDS:
                    if 0 <= numeric <= 1:
                        numeric *= 100
                    value_text = f"{numeric:.4f}%"
                results.append(
                    SearchResult(
                        source_name=self.name,
                        source_type=self.source_type,
                        source_url=source_url,
                        variable_name=var_name,
                        value_text=value_text,
                        value_numeric=numeric,
                        confidence=0.90,
                        sample_size=None,
                        temporal_range="latest",
                        raw_data={
                            "api_field": api_field,
                            "record_id": school_id,
                            "queried_school_name": chosen_query,
                        },
                    )
                )

            # Derived school profile fields.
            endowment_total = _deep_get(record, "latest.school.endowment.end")
            enrollment = _deep_get(record, "latest.student.size")
            if "endowment_per_student" in requested_fields and _is_numeric(endowment_total) and _is_numeric(enrollment):
                total = float(endowment_total)
                size = max(float(enrollment), 1.0)
                per_student = total / size
                results.append(
                    SearchResult(
                        source_name=self.name,
                        source_type=self.source_type,
                        source_url=source_url,
                        variable_name="endowment_per_student",
                        value_text=f"{per_student:.2f}",
                        value_numeric=per_student,
                        confidence=0.88,
                        sample_size=None,
                        temporal_range="latest",
                        raw_data={
                            "api_field": "derived:endowment_per_student",
                            "record_id": school_id,
                            "derived_from": ["latest.school.endowment.end", "latest.student.size"],
                            "endowment_total": float(endowment_total),
                            "enrollment": float(enrollment),
                        },
                    )
                )

            if "sat_25" in requested_fields:
                sat_25 = _mean_numeric(
                    _deep_get(record, "latest.admissions.sat_scores.25th_percentile.critical_reading"),
                    _deep_get(record, "latest.admissions.sat_scores.25th_percentile.math"),
                )
                if sat_25 is not None:
                    results.append(
                        SearchResult(
                            source_name=self.name,
                            source_type=self.source_type,
                            source_url=source_url,
                            variable_name="sat_25",
                            value_text=str(int(round(sat_25))),
                            value_numeric=sat_25,
                            confidence=0.88,
                            sample_size=None,
                            temporal_range="latest",
                            raw_data={
                                "api_field": "derived:sat_25",
                                "record_id": school_id,
                                "derived_from": [
                                    "latest.admissions.sat_scores.25th_percentile.critical_reading",
                                    "latest.admissions.sat_scores.25th_percentile.math",
                                ],
                            },
                        )
                    )
            if "sat_75" in requested_fields:
                sat_75 = _mean_numeric(
                    _deep_get(record, "latest.admissions.sat_scores.75th_percentile.critical_reading"),
                    _deep_get(record, "latest.admissions.sat_scores.75th_percentile.math"),
                )
                if sat_75 is not None:
                    results.append(
                        SearchResult(
                            source_name=self.name,
                            source_type=self.source_type,
                            source_url=source_url,
                            variable_name="sat_75",
                            value_text=str(int(round(sat_75))),
                            value_numeric=sat_75,
                            confidence=0.88,
                            sample_size=None,
                            temporal_range="latest",
                            raw_data={
                                "api_field": "derived:sat_75",
                                "record_id": school_id,
                                "derived_from": [
                                    "latest.admissions.sat_scores.75th_percentile.critical_reading",
                                    "latest.admissions.sat_scores.75th_percentile.math",
                                ],
                            },
                        )
                    )
        return results

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    _API_BASE,
                    params={"api_key": self._api_key, "per_page": 1, "fields": "school.name"},
                )
                return resp.status_code == 200
        except httpx.HTTPError:
            return False


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _deep_get(data: dict, dotted_key: str) -> Any:
    """Navigate nested dicts using a dot-separated key path."""
    # College Scorecard often returns flat keys that include dots directly.
    if dotted_key in data:
        return data.get(dotted_key)

    keys = dotted_key.split(".")
    current: Any = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current


def _select_best_records(
    records: list[dict[str, Any]],
    *,
    school_name: str,
    api_fields: list[str],
) -> list[dict[str, Any]]:
    if not records:
        return []
    target = _normalise_school_name(school_name)
    best_score = -1
    best_record: dict[str, Any] | None = None
    for record in records:
        rec_name_raw = _deep_get(record, "school.name")
        rec_name = _normalise_school_name(str(rec_name_raw or ""))
        exact = int(rec_name == target)
        contains = int(target and target in rec_name)
        non_null = sum(1 for key in api_fields if _deep_get(record, key) is not None)
        score = exact * 10_000 + contains * 1_000 + non_null
        if score > best_score:
            best_score = score
            best_record = record
    return [best_record] if best_record is not None else []


def _normalise_school_name(value: str) -> str:
    return _NON_WORD_RE.sub(" ", (value or "").lower()).strip()


def _is_numeric(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _mean_numeric(*values: Any) -> float | None:
    numeric_values: list[float] = []
    for value in values:
        if not _is_numeric(value):
            continue
        numeric_values.append(float(value))
    if not numeric_values:
        return None
    return sum(numeric_values) / len(numeric_values)
