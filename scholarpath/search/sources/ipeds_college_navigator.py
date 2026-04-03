"""IPEDS / College Navigator bulk official source."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from scholarpath.search.sources.base import BaseSource, SearchResult

logger = logging.getLogger(__name__)

_NON_WORD_RE = re.compile(r"[^a-z0-9]+")

_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "applicants_total": (
        "applicants_total",
        "applicants",
        "admissions_applicants_total",
        "admissions_applicants",
    ),
    "admitted_total": (
        "admitted_total",
        "admitted",
        "admissions_admitted_total",
        "admissions_admitted",
    ),
    "enrolled_total": (
        "enrolled_total",
        "enrolled",
        "admissions_enrolled_total",
        "admissions_enrolled",
    ),
    "acceptance_rate": (
        "acceptance_rate",
        "admission_rate",
        "admissions_acceptance_rate",
    ),
    "yield_rate": (
        "yield_rate",
        "admissions_yield_rate",
    ),
    "sat_25": (
        "sat_25",
        "sat_total_25",
        "sat_percentile_25",
    ),
    "sat_75": (
        "sat_75",
        "sat_total_75",
        "sat_percentile_75",
    ),
    "act_25": (
        "act_25",
        "act_percentile_25",
    ),
    "act_75": (
        "act_75",
        "act_percentile_75",
    ),
    "tuition_out_of_state": (
        "tuition_out_of_state",
        "tuition_oos",
        "out_of_state_tuition",
    ),
    "avg_net_price": (
        "avg_net_price",
        "average_net_price",
    ),
    "graduation_rate_4yr": (
        "graduation_rate_4yr",
        "graduation_rate",
    ),
    "retention_rate": (
        "retention_rate",
        "retention_rate_full_time",
    ),
    "enrollment": (
        "enrollment",
        "student_size",
        "total_enrollment",
    ),
    "city": (
        "city",
        "school_city",
    ),
    "state": (
        "state",
        "school_state",
        "state_abbr",
    ),
    "website_url": (
        "website_url",
        "school_url",
        "institution_url",
    ),
}

_RATE_FIELDS = {
    "acceptance_rate",
    "yield_rate",
    "graduation_rate_4yr",
    "retention_rate",
}

_EXTERNAL_ID_ALIASES = (
    "unitid",
    "ipeds_unitid",
    "school_unitid",
    "institution_id",
    "school_id",
)
_NAME_ALIASES = (
    "institution_name",
    "school_name",
    "name",
    "school",
)
_STATE_ALIASES = ("state", "state_abbr", "school_state")
_CITY_ALIASES = ("city", "school_city")
_WEBSITE_ALIASES = ("website_url", "school_url", "institution_url")
_YEAR_ALIASES = ("year", "cycle_year", "survey_year", "academic_year")


@dataclass(slots=True)
class IPEDSRecord:
    row: dict[str, Any]
    canonical_name: str
    state: str
    city: str
    external_id: str
    website_domain: str
    cycle_year: int | None


class IPEDSCollegeNavigatorSource(BaseSource):
    """Official bulk source backed by IPEDS / College Navigator exports."""

    name = "ipeds_college_navigator"
    source_type = "official"

    def __init__(
        self,
        *,
        dataset_url: str = "",
        dataset_path: str = "",
        timeout_seconds: float = 30.0,
    ) -> None:
        self._dataset_url = (dataset_url or "").strip()
        self._dataset_path = (dataset_path or "").strip()
        self._timeout_seconds = max(float(timeout_seconds), 5.0)
        self._records_cache: list[IPEDSRecord] | None = None
        self._records_cache_ts: datetime | None = None
        self._load_lock = asyncio.Lock()

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        records = await self._load_records()
        if not records:
            return []
        match = self._match_by_name_state(
            records=records,
            school_name=school_name,
            school_state="",
        )
        if match is None:
            return []
        requested = {str(field).strip() for field in (fields or []) if str(field).strip()}
        return self._record_to_results(
            record=match,
            requested_fields=requested or set(_FIELD_ALIASES.keys()),
            match_method="name_state",
            match_confidence=0.80,
        )

    async def search_for_school(
        self,
        *,
        school_name: str,
        school_state: str,
        website_url: str | None,
        fields: list[str] | None = None,
        external_ids: dict[str, str] | None = None,
    ) -> list[SearchResult]:
        records = await self._load_records()
        if not records:
            return []

        requested = {str(field).strip() for field in (fields or []) if str(field).strip()}
        ids = {k.lower().strip(): str(v).strip() for k, v in (external_ids or {}).items() if str(v).strip()}
        match: IPEDSRecord | None = None
        match_method = "none"
        confidence = 0.0

        ipeds_id = ids.get("ipeds") or ids.get("unitid") or ids.get("ipeds_unitid")
        if ipeds_id:
            match = next((item for item in records if item.external_id == ipeds_id), None)
            if match is not None:
                match_method = "external_id"
                confidence = 0.99

        if match is None:
            match = self._match_by_name_state(
                records=records,
                school_name=school_name,
                school_state=school_state,
            )
            if match is not None:
                match_method = "name_state"
                confidence = 0.90

        if match is None:
            domain = _extract_domain(website_url or "")
            if domain:
                match = next((item for item in records if item.website_domain == domain), None)
                if match is not None:
                    match_method = "domain"
                    confidence = 0.78

        if match is None:
            return []
        return self._record_to_results(
            record=match,
            requested_fields=requested or set(_FIELD_ALIASES.keys()),
            match_method=match_method,
            match_confidence=confidence,
        )

    async def list_top_schools(
        self,
        *,
        top_n: int = 1000,
        years: int = 5,
        selection_metric: str = "applicants_total",
    ) -> list[dict[str, Any]]:
        records = await self._load_records()
        if not records:
            return []
        top_n = max(1, int(top_n))
        years = max(1, int(years))
        metric = str(selection_metric or "applicants_total").strip().lower()
        if metric not in {"applicants_total", "enrollment"}:
            metric = "applicants_total"

        latest_year = max((record.cycle_year or 0) for record in records) or datetime.now(timezone.utc).year
        min_year = latest_year - years + 1

        grouped: dict[str, dict[str, Any]] = {}
        for record in records:
            if record.cycle_year is not None and record.cycle_year < min_year:
                continue
            key = record.external_id or record.canonical_name
            bucket = grouped.setdefault(
                key,
                {
                    "external_id": record.external_id,
                    "school_name": record.row.get("institution_name") or record.row.get("school_name") or "",
                    "state": record.state,
                    "city": record.city,
                    "website_url": record.row.get("website_url") or "",
                    "score": 0.0,
                    "latest_year": record.cycle_year or 0,
                },
            )
            value = _to_float(_pick_value(record.row, _FIELD_ALIASES.get(metric, (metric,))))
            if value is not None:
                bucket["score"] = max(float(bucket["score"]), value)
            if (record.cycle_year or 0) >= int(bucket["latest_year"]):
                bucket["latest_year"] = record.cycle_year or 0

        ranked = sorted(grouped.values(), key=lambda item: (item["score"], item["latest_year"]), reverse=True)
        return ranked[:top_n]

    async def health_check(self) -> bool:
        records = await self._load_records()
        return bool(records)

    async def _load_records(self) -> list[IPEDSRecord]:
        async with self._load_lock:
            if self._records_cache is not None:
                return self._records_cache
            rows = await self._fetch_raw_rows()
            parsed: list[IPEDSRecord] = []
            for raw_row in rows:
                if not isinstance(raw_row, dict):
                    continue
                row = {_normalise_key(key): raw_row.get(key) for key in raw_row.keys()}
                external_id = str(_pick_value(row, _EXTERNAL_ID_ALIASES) or "").strip()
                name_raw = str(_pick_value(row, _NAME_ALIASES) or "").strip()
                if not name_raw:
                    continue
                state = str(_pick_value(row, _STATE_ALIASES) or "").strip()
                city = str(_pick_value(row, _CITY_ALIASES) or "").strip()
                website_url = str(_pick_value(row, _WEBSITE_ALIASES) or "").strip()
                row.setdefault("institution_name", name_raw)
                row.setdefault("state", state)
                row.setdefault("city", city)
                row.setdefault("website_url", website_url)
                cycle_year = _to_int(_pick_value(row, _YEAR_ALIASES))
                parsed.append(
                    IPEDSRecord(
                        row=row,
                        canonical_name=_normalise_name(name_raw),
                        state=state.lower().strip(),
                        city=city.lower().strip(),
                        external_id=external_id,
                        website_domain=_extract_domain(website_url),
                        cycle_year=cycle_year,
                    )
                )
            self._records_cache = parsed
            self._records_cache_ts = datetime.now(timezone.utc)
            return parsed

    async def _fetch_raw_rows(self) -> list[dict[str, Any]]:
        path = Path(self._dataset_path) if self._dataset_path else None
        if path and path.exists():
            try:
                return _parse_dataset(path.read_text(encoding="utf-8"), suffix=path.suffix.lower())
            except Exception:
                logger.warning("Failed to parse IPEDS dataset file: %s", path, exc_info=True)
                return []
        if not self._dataset_url:
            return []
        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.get(self._dataset_url)
                response.raise_for_status()
                text = response.text
        except Exception:
            logger.warning("Failed to fetch IPEDS dataset: %s", self._dataset_url, exc_info=True)
            return []
        suffix = Path(urlparse(self._dataset_url).path).suffix.lower()
        return _parse_dataset(text, suffix=suffix)

    def _match_by_name_state(
        self,
        *,
        records: list[IPEDSRecord],
        school_name: str,
        school_state: str,
    ) -> IPEDSRecord | None:
        canonical = _normalise_name(school_name)
        state = (school_state or "").strip().lower()
        best: tuple[int, IPEDSRecord] | None = None
        for record in records:
            exact = 1 if record.canonical_name == canonical else 0
            contains = 1 if canonical and canonical in record.canonical_name else 0
            state_match = 1 if state and record.state and record.state == state else 0
            score = exact * 100 + contains * 10 + state_match
            if best is None or score > best[0]:
                best = (score, record)
        if best is None:
            return None
        if best[0] <= 0:
            return None
        return best[1]

    def _record_to_results(
        self,
        *,
        record: IPEDSRecord,
        requested_fields: set[str],
        match_method: str,
        match_confidence: float,
    ) -> list[SearchResult]:
        results: list[SearchResult] = []
        for variable, aliases in _FIELD_ALIASES.items():
            if requested_fields and variable not in requested_fields:
                continue
            raw_value = _pick_value(record.row, aliases)
            if raw_value is None:
                continue
            value_numeric = _to_float(raw_value)
            value_text = str(raw_value).strip()
            if variable in _RATE_FIELDS and value_numeric is not None:
                numeric_pct = value_numeric * 100 if 0.0 <= value_numeric <= 1.0 else value_numeric
                value_numeric = round(numeric_pct, 6)
                value_text = f"{numeric_pct:.4f}%"
            results.append(
                SearchResult(
                    source_name=self.name,
                    source_type=self.source_type,
                    source_url=str(record.row.get("website_url") or ""),
                    variable_name=variable,
                    value_text=value_text,
                    value_numeric=value_numeric,
                    confidence=min(max(match_confidence, 0.0), 1.0),
                    temporal_range=str(record.cycle_year) if record.cycle_year else None,
                    raw_data={
                        "fetch_mode": "ipeds_bulk",
                        "source_kind": "ipeds_college_navigator",
                        "match_method": match_method,
                        "match_confidence": round(match_confidence, 4),
                        "external_id_provider": "ipeds",
                        "external_id": record.external_id,
                        "school_name": record.row.get("institution_name"),
                        "state": record.row.get("state"),
                        "cycle_year": record.cycle_year,
                    },
                )
            )
        return results


def _parse_dataset(payload: str, *, suffix: str) -> list[dict[str, Any]]:
    text = (payload or "").strip()
    if not text:
        return []
    if suffix == ".csv":
        return _parse_csv(text)
    if suffix == ".json":
        return _parse_json(text)
    # Auto-detect.
    if text.startswith("{") or text.startswith("["):
        return _parse_json(text)
    return _parse_csv(text)


def _parse_csv(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        if isinstance(row, dict):
            rows.append(dict(row))
    return rows


def _parse_json(text: str) -> list[dict[str, Any]]:
    payload = json.loads(text)
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("items") or payload.get("results") or payload.get("data")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _normalise_key(value: str) -> str:
    return _NON_WORD_RE.sub("_", str(value or "").strip().lower()).strip("_")


def _normalise_name(value: str) -> str:
    return _NON_WORD_RE.sub(" ", str(value or "").strip().lower()).strip()


def _extract_domain(url: str) -> str:
    parsed = urlparse(url or "")
    host = (parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _pick_value(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    for alias in aliases:
        normal = _normalise_key(alias)
        if normal in row and row.get(normal) not in (None, ""):
            return row.get(normal)
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return int(round(numeric))
