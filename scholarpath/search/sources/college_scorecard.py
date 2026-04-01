"""College Scorecard API source (api.data.gov)."""

from __future__ import annotations

import logging
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
    "latest.cost.attendance.academic_year": "cost_of_attendance",
    "latest.student.size": "enrollment",
    "latest.student.demographics.race_ethnicity.white": "pct_white",
    "latest.student.retention_rate.four_year.full_time": "retention_rate",
    "latest.completion.rate_suppressed.overall": "graduation_rate",
    "latest.earnings.10_yrs_after_entry.median": "median_earnings_10yr",
    "latest.aid.median_debt.completers.overall": "median_debt",
    "school.city": "city",
    "school.state": "state",
    "school.school_url": "school_url",
}

# Reverse lookup: our variable name -> API field
_REVERSE_MAP: dict[str, str] = {v: k for k, v in _FIELD_MAP.items()}

_API_BASE = "https://api.data.gov/ed/collegescorecard/v1/schools.json"


class CollegeScorecardSource(BaseSource):
    """Official U.S. Department of Education College Scorecard data."""

    name = "college_scorecard"
    source_type = "official"

    def __init__(self, api_key: str = "DEMO_KEY") -> None:
        self._api_key = api_key

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        # Determine which API fields to request.
        if fields:
            api_fields = [_REVERSE_MAP[f] for f in fields if f in _REVERSE_MAP]
        else:
            api_fields = list(_FIELD_MAP.keys())

        # Always include the school name for identification.
        api_fields_csv = ",".join(["school.name", "id"] + api_fields)

        params: dict[str, Any] = {
            "school.name": school_name,
            "fields": api_fields_csv,
            "api_key": self._api_key,
            "per_page": 5,
        }

        results: list[SearchResult] = []
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(_API_BASE, params=params)
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPError as exc:
            logger.warning("College Scorecard request failed: %s", exc)
            return results

        for record in data.get("results", []):
            school_id = record.get("id", "")
            source_url = f"https://collegescorecard.ed.gov/school/?{school_id}"
            for api_field, var_name in _FIELD_MAP.items():
                if api_field not in api_fields:
                    continue
                value = _deep_get(record, api_field)
                if value is None:
                    continue
                numeric = float(value) if _is_numeric(value) else None
                results.append(
                    SearchResult(
                        source_name=self.name,
                        source_type=self.source_type,
                        source_url=source_url,
                        variable_name=var_name,
                        value_text=str(value),
                        value_numeric=numeric,
                        confidence=0.90,
                        sample_size=None,
                        temporal_range="latest",
                        raw_data={"api_field": api_field, "record_id": school_id},
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
    keys = dotted_key.split(".")
    current: Any = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current


def _is_numeric(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False
