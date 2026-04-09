"""College Scorecard API client for fetching real US school data."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from scholarpath.config import settings

logger = logging.getLogger(__name__)

SCORECARD_BASE = "https://api.data.gov/ed/collegescorecard/v1"

# Fields we fetch per school
_SCHOOL_FIELDS = ",".join([
    "id",
    "school.name",
    "school.city",
    "school.state",
    "school.school_url",
    "school.ownership",                    # 1=public, 2=private nonprofit, 3=private for-profit
    "school.carnegie_size_setting",
    "latest.admissions.admission_rate.overall",
    "latest.admissions.sat_scores.25th_percentile.critical_reading",
    "latest.admissions.sat_scores.75th_percentile.critical_reading",
    "latest.admissions.sat_scores.25th_percentile.math",
    "latest.admissions.sat_scores.75th_percentile.math",
    "latest.admissions.act_scores.25th_percentile.cumulative",
    "latest.admissions.act_scores.75th_percentile.cumulative",
    "latest.cost.tuition.in_state",
    "latest.cost.tuition.out_of_state",
    "latest.cost.avg_net_price.overall",
    "latest.student.size",
    "latest.student.demographics.share_25_plus",
    "latest.student.share_first_generation",
    "latest.completion.rate_suppressed.overall",
    "latest.student.retention_rate.four_year.full_time",
    "latest.earnings.10_yrs_after_entry.median",
    "latest.student.enrollment.undergrad_12_month",
])

# Fields for program/field-of-study queries
_PROGRAM_FIELDS = ",".join([
    "unit_id",
    "opeid6",
    "instnm",
    "control",
    "cipcode",
    "cipdesc",
    "creddesc",
    "credlev",
    "earn_count_wne_3yr",
    "earn_mdn_3yr",
    "earn_count_wne_4yr",
    "earn_mdn_4yr",
])


async def search_schools_scorecard(
    query: str | None = None,
    state: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Search schools via College Scorecard API."""
    api_key = settings.COLLEGE_SCORECARD_API_KEY
    if not api_key:
        raise ValueError("COLLEGE_SCORECARD_API_KEY not configured")

    params: dict[str, Any] = {
        "api_key": api_key,
        "fields": _SCHOOL_FIELDS,
        "per_page": limit,
    }
    if query:
        params["school.name"] = query
    if state:
        params["school.state"] = state

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{SCORECARD_BASE}/schools", params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results", [])
    return [_parse_school(r) for r in results]


async def get_school_by_name(name: str) -> dict[str, Any] | None:
    """Fetch a single school by exact or partial name match."""
    # Try full name first, then simplified (some names with commas cause API 500)
    for query in [name, name.split(",")[0].strip()]:
        try:
            results = await search_schools_scorecard(query=query, limit=5)
        except Exception:
            logger.warning("Scorecard API error for query=%s, trying fallback", query)
            continue
        if not results:
            continue
        # Prefer exact match
        name_lower = name.lower()
        for r in results:
            if r["name"].lower() == name_lower:
                return r
        return results[0]
    return None


async def get_programs_by_school(school_name: str) -> list[dict[str, Any]]:
    """Fetch field-of-study data for a school (earnings by major)."""
    api_key = settings.COLLEGE_SCORECARD_API_KEY
    if not api_key:
        raise ValueError("COLLEGE_SCORECARD_API_KEY not configured")

    params: dict[str, Any] = {
        "api_key": api_key,
        "fields": _PROGRAM_FIELDS,
        "instnm": school_name,
        "per_page": 100,
    }

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{SCORECARD_BASE}/schools/fields_of_study", params=params)
        resp.raise_for_status()
        data = resp.json()

    return [
        {
            "cip_code": r.get("cipcode"),
            "program_name": r.get("cipdesc"),
            "credential": r.get("creddesc"),
            "credential_level": r.get("credlev"),
            "earnings_3yr_median": r.get("earn_mdn_3yr"),
            "earnings_4yr_median": r.get("earn_mdn_4yr"),
            "earners_3yr_count": r.get("earn_count_wne_3yr"),
            "earners_4yr_count": r.get("earn_count_wne_4yr"),
        }
        for r in data.get("results", [])
    ]


async def bulk_fetch_schools(names: list[str]) -> dict[str, dict[str, Any]]:
    """Fetch data for multiple schools by name. Returns name→data mapping."""
    result = {}
    for name in names:
        try:
            school = await get_school_by_name(name)
            if school:
                result[name] = school
                logger.info("Scorecard: fetched %s", name)
            else:
                logger.warning("Scorecard: not found: %s", name)
        except Exception:
            logger.exception("Scorecard: error fetching %s", name)
    return result


def _parse_school(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize a Scorecard API result into our schema."""
    get = raw.get
    latest = lambda *keys: _nested_get(raw, *keys)

    ownership = get("school.ownership")
    if ownership == 1:
        school_type = "university"  # public
    elif ownership == 2:
        school_type = "university"  # private nonprofit
    else:
        school_type = "university"

    size_val = latest("latest.student.size")
    if isinstance(size_val, (int, float)) and size_val > 0:
        if size_val < 5000:
            size_cat = "small"
        elif size_val < 15000:
            size_cat = "medium"
        else:
            size_cat = "large"
    else:
        size_cat = "medium"

    # SAT composite from R+W and Math
    sat_rw_25 = latest("latest.admissions.sat_scores.25th_percentile.critical_reading")
    sat_math_25 = latest("latest.admissions.sat_scores.25th_percentile.math")
    sat_rw_75 = latest("latest.admissions.sat_scores.75th_percentile.critical_reading")
    sat_math_75 = latest("latest.admissions.sat_scores.75th_percentile.math")
    sat_25 = (sat_rw_25 + sat_math_25) if sat_rw_25 and sat_math_25 else None
    sat_75 = (sat_rw_75 + sat_math_75) if sat_rw_75 and sat_math_75 else None

    tuition_in = latest("latest.cost.tuition.in_state")
    tuition_oos = latest("latest.cost.tuition.out_of_state")

    return {
        "scorecard_id": get("id"),
        "name": get("school.name"),
        "city": get("school.city"),
        "state": get("school.state"),
        "website_url": _normalize_url(get("school.school_url")),
        "school_type": school_type,
        "size_category": size_cat,
        "acceptance_rate": _to_float(latest("latest.admissions.admission_rate.overall")),
        "sat_25": _to_int(sat_25),
        "sat_75": _to_int(sat_75),
        "act_25": _to_int(latest("latest.admissions.act_scores.25th_percentile.cumulative")),
        "act_75": _to_int(latest("latest.admissions.act_scores.75th_percentile.cumulative")),
        "tuition_in_state": _to_int(tuition_in),
        "tuition_oos": _to_int(tuition_oos),
        "tuition_intl": _to_int(tuition_oos),  # default: intl = out-of-state
        "avg_net_price": _to_int(latest("latest.cost.avg_net_price.overall")),
        "graduation_rate_4yr": _to_float(latest("latest.completion.rate_suppressed.overall")),
        "median_earnings_10yr": _to_int(latest("latest.earnings.10_yrs_after_entry.median")),
        "student_size": _to_int(latest("latest.student.size")),
    }


def _nested_get(d: dict, *keys: str) -> Any:
    """Get a dotted key from a flat Scorecard result dict."""
    for key in keys:
        if key in d:
            return d[key]
    return None


def _to_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _to_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _normalize_url(url: str | None) -> str | None:
    if not url:
        return None
    url = url.strip()
    if not url.startswith("http"):
        url = f"https://{url}"
    return url
