"""IPEDS / College Navigator bulk official source."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import re
import zipfile
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
    "instnm",
    "name",
    "school",
)
_STATE_ALIASES = ("state", "stabbr", "state_abbr", "school_state")
_CITY_ALIASES = ("city", "school_city")
_WEBSITE_ALIASES = ("website_url", "school_url", "institution_url", "webaddr")
_YEAR_ALIASES = ("year", "cycle_year", "survey_year", "academic_year")
_CIP_CODE_ALIASES = (
    "cip_code",
    "cipcode",
    "cip",
    "cip_6",
    "cip6",
    "cip_4",
    "cip4",
)
_CIP_TITLE_ALIASES = (
    "cip_title",
    "cip_description",
    "cip_desc",
    "cipdesc",
    "program_name",
    "program_title",
    "major",
    "major_name",
)
_AWARD_LEVEL_ALIASES = (
    "award_level",
    "awlevel",
    "degree_level",
    "credential_level",
    "award",
)
_COMPLETIONS_ALIASES = (
    "completions_total",
    "completions",
    "completions_count",
    "ctotalt",
    "awards",
    "graduates_total",
    "graduates",
)


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
        completions_dataset_url: str = "",
        completions_dataset_path: str = "",
        institution_dataset_url: str = "",
        institution_dataset_path: str = "",
        timeout_seconds: float = 30.0,
    ) -> None:
        self._dataset_url = (dataset_url or "").strip()
        self._dataset_path = (dataset_path or "").strip()
        self._completions_dataset_url = (completions_dataset_url or "").strip()
        self._completions_dataset_path = (completions_dataset_path or "").strip()
        self._institution_dataset_url = (institution_dataset_url or "").strip()
        self._institution_dataset_path = (institution_dataset_path or "").strip()
        self._timeout_seconds = max(float(timeout_seconds), 5.0)
        self._records_cache: list[IPEDSRecord] | None = None
        self._records_cache_ts: datetime | None = None
        self._program_rows_cache: list[dict[str, Any]] | None = None
        self._institution_rows_cache: list[dict[str, Any]] | None = None
        self._load_lock = asyncio.Lock()
        self._program_load_lock = asyncio.Lock()

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

    async def list_program_completions(
        self,
        *,
        years: int = 3,
        min_completions: int = 1,
        award_levels: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        async with self._program_load_lock:
            if self._program_rows_cache is None:
                self._program_rows_cache = await self._load_program_rows()
            rows = self._program_rows_cache
        if not rows:
            return []

        years = max(1, int(years))
        min_completions = max(0, int(min_completions))
        latest_year = max(int(item.get("year") or 0) for item in rows) or datetime.now(timezone.utc).year
        min_year = latest_year - years + 1
        normalized_awards = {
            _normalise_award_level(item)
            for item in (award_levels or set())
            if str(item).strip()
        }

        grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for item in rows:
            year = _to_int(item.get("year"))
            if year is None or year < min_year:
                continue
            completions = _to_int(item.get("completions"))
            if completions is None or completions < min_completions:
                continue
            award_level = _normalise_award_level(item.get("award_level"))
            if normalized_awards and award_level not in normalized_awards:
                continue
            external_id = str(item.get("external_id") or "").strip()
            cip_code = _normalise_cip_code(item.get("cip_code"))
            cip_title = str(item.get("cip_title") or "").strip() or (f"CIP {cip_code}" if cip_code else "")
            if not external_id or not cip_code:
                continue
            key = (external_id, cip_code, award_level)
            existing = grouped.get(key)
            candidate = {
                "external_id": external_id,
                "school_name": str(item.get("school_name") or "").strip(),
                "state": str(item.get("state") or "").strip(),
                "city": str(item.get("city") or "").strip(),
                "website_url": str(item.get("website_url") or "").strip(),
                "year": int(year),
                "cip_code": cip_code,
                "cip_title": cip_title,
                "award_level": award_level or "unknown",
                "completions": int(completions),
            }
            if existing is None:
                grouped[key] = candidate
                continue
            should_update = int(candidate["year"]) > int(existing.get("year") or 0) or (
                int(candidate["year"]) == int(existing.get("year") or 0)
                and int(candidate["completions"]) > int(existing.get("completions") or 0)
            )
            if should_update:
                grouped[key] = candidate

        return sorted(grouped.values(), key=lambda item: (item["completions"], item["year"]), reverse=True)

    async def list_institutions(self) -> list[dict[str, Any]]:
        async with self._program_load_lock:
            if self._institution_rows_cache is None:
                self._institution_rows_cache = await self._load_institution_rows()
            rows = self._institution_rows_cache
        return [dict(item) for item in rows]

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
        rows = await self._fetch_rows_from_dataset(
            dataset_path=self._dataset_path,
            dataset_url=self._dataset_url,
            label="ipeds_default",
        )
        return rows

    async def _load_program_rows(self) -> list[dict[str, Any]]:
        # Preferred Phase A path: explicit completions + institution datasets.
        if (
            self._completions_dataset_path
            or self._completions_dataset_url
            or self._institution_dataset_path
            or self._institution_dataset_url
        ):
            completion_rows = await self._fetch_rows_from_dataset(
                dataset_path=self._completions_dataset_path,
                dataset_url=self._completions_dataset_url,
                label="ipeds_completions",
            )
            institution_rows = await self._fetch_rows_from_dataset(
                dataset_path=self._institution_dataset_path,
                dataset_url=self._institution_dataset_url,
                label="ipeds_institution",
            )
            return _join_completion_with_institution(
                completion_rows=completion_rows,
                institution_rows=institution_rows,
                completions_year_hint=_extract_year_hint(self._completions_dataset_url, self._completions_dataset_path),
            )

        # Backward-compatible fallback: single merged dataset.
        records = await self._load_records()
        fallback_rows: list[dict[str, Any]] = []
        for record in records:
            row = record.row
            unitid = str(record.external_id or _pick_value(row, _EXTERNAL_ID_ALIASES) or "").strip()
            if not unitid:
                continue
            cip_code = _normalise_cip_code(_pick_value(row, _CIP_CODE_ALIASES))
            if not cip_code:
                continue
            completions = _to_int(_pick_value(row, _COMPLETIONS_ALIASES))
            if completions is None:
                continue
            award_level = _normalise_award_level(_pick_value(row, _AWARD_LEVEL_ALIASES))
            year = record.cycle_year or _to_int(_pick_value(row, _YEAR_ALIASES)) or datetime.now(timezone.utc).year
            cip_title = str(_pick_value(row, _CIP_TITLE_ALIASES) or "").strip() or f"CIP {cip_code}"
            fallback_rows.append(
                {
                    "external_id": unitid,
                    "school_name": str(row.get("institution_name") or row.get("school_name") or "").strip(),
                    "state": str(row.get("state") or "").strip(),
                    "city": str(row.get("city") or "").strip(),
                    "website_url": str(row.get("website_url") or "").strip(),
                    "year": year,
                    "cip_code": cip_code,
                    "cip_title": cip_title,
                    "award_level": award_level,
                    "completions": completions,
                }
            )
        return fallback_rows

    async def _load_institution_rows(self) -> list[dict[str, Any]]:
        rows = await self._fetch_rows_from_dataset(
            dataset_path=self._institution_dataset_path or self._dataset_path,
            dataset_url=self._institution_dataset_url or self._dataset_url,
            label="ipeds_institution",
        )
        if not rows:
            return []

        deduped: dict[str, dict[str, Any]] = {}
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            row = {_normalise_key(key): raw.get(key) for key in raw.keys()}
            unitid = str(_pick_value(row, _EXTERNAL_ID_ALIASES) or "").strip()
            if not unitid:
                continue
            merged = deduped.get(unitid, {})
            merged["external_id"] = unitid
            merged["school_name"] = str(_pick_value(row, _NAME_ALIASES) or merged.get("school_name") or "").strip()
            merged["state"] = str(_pick_value(row, _STATE_ALIASES) or merged.get("state") or "").strip()
            merged["city"] = str(_pick_value(row, _CITY_ALIASES) or merged.get("city") or "").strip()
            merged["website_url"] = str(_pick_value(row, _WEBSITE_ALIASES) or merged.get("website_url") or "").strip()
            deduped[unitid] = merged
        return sorted(deduped.values(), key=lambda item: str(item.get("external_id") or ""))

    async def _fetch_rows_from_dataset(
        self,
        *,
        dataset_path: str,
        dataset_url: str,
        label: str,
    ) -> list[dict[str, Any]]:
        path = Path(dataset_path) if dataset_path else None
        if path and path.exists():
            try:
                payload = path.read_bytes()
                return _parse_dataset_bytes(payload, suffix=path.suffix.lower(), source_hint=str(path))
            except Exception:
                logger.warning("Failed to parse %s dataset file: %s", label, path, exc_info=True)
                return []
        if not dataset_url:
            return []
        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.get(dataset_url)
                response.raise_for_status()
                payload = response.content
        except Exception:
            logger.warning("Failed to fetch %s dataset: %s", label, dataset_url, exc_info=True)
            return []
        suffix = Path(urlparse(dataset_url).path).suffix.lower()
        return _parse_dataset_bytes(payload, suffix=suffix, source_hint=dataset_url)

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
    # Backward-compatible text parser.
    text = (payload or "").strip()
    if not text:
        return []
    if suffix == ".csv":
        return _parse_csv(text)
    if suffix == ".json":
        return _parse_json(text)
    if text.startswith("{") or text.startswith("["):
        return _parse_json(text)
    return _parse_csv(text)


def _parse_dataset_bytes(payload: bytes, *, suffix: str, source_hint: str = "") -> list[dict[str, Any]]:
    blob = payload or b""
    if not blob:
        return []
    if suffix == ".zip" or _looks_like_zip(blob):
        return _parse_zip_blob(blob, source_hint=source_hint)
    text = _decode_blob(blob)
    return _parse_dataset(text, suffix=suffix)


def _looks_like_zip(blob: bytes) -> bool:
    return len(blob) >= 4 and blob[:2] == b"PK"


def _parse_zip_blob(blob: bytes, *, source_hint: str = "") -> list[dict[str, Any]]:
    try:
        zf = zipfile.ZipFile(io.BytesIO(blob))
    except zipfile.BadZipFile:
        logger.warning("Failed to parse zip dataset: %s", source_hint)
        return []
    members = [name for name in zf.namelist() if not name.endswith("/")]
    if not members:
        return []
    preferred = sorted(
        members,
        key=lambda name: (
            0 if name.lower().endswith(".csv") else 1,
            0 if name.lower().endswith(".json") else 1,
            len(name),
        ),
    )[0]
    try:
        with zf.open(preferred) as fh:
            inner = fh.read()
    except Exception:
        logger.warning("Failed to read zip member %s from %s", preferred, source_hint, exc_info=True)
        return []
    inner_suffix = Path(preferred).suffix.lower()
    return _parse_dataset_bytes(inner, suffix=inner_suffix, source_hint=f"{source_hint}:{preferred}")


def _decode_blob(blob: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return blob.decode(encoding)
        except UnicodeDecodeError:
            continue
    return blob.decode("latin1", errors="ignore")


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


def _normalise_cip_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"[^0-9.]", "", text)
    if not text:
        return ""
    if "." in text:
        left, _, right = text.partition(".")
        left = left.zfill(2)[:2]
        right = right[:4]
        return f"{left}.{right}" if right else left
    digits = text
    if len(digits) >= 6:
        return f"{digits[:2]}.{digits[2:6]}"
    if len(digits) >= 2:
        return digits[:2]
    return digits


def _normalise_award_level(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return "unknown"
    if raw in {"bachelor", "bachelors", "ba", "bs"}:
        return "bachelor"
    if raw in {"master", "masters", "ma", "ms", "mba"}:
        return "master"
    if raw in {"doctorate", "doctoral", "phd"}:
        return "doctorate"
    if raw in {"associate", "associates", "aa", "as"}:
        return "associate"
    if raw.isdigit():
        # IPEDS AWLEVEL numeric families.
        if raw in {"5", "6", "7"}:
            return "bachelor"
        if raw in {"8", "17", "18"}:
            return "master"
        if raw in {"9", "19"}:
            return "doctorate"
        if raw in {"3", "4"}:
            return "associate"
    return raw


def _extract_year_hint(*sources: str) -> int | None:
    for source in sources:
        text = str(source or "")
        if not text:
            continue
        for match in re.finditer(r"(19|20)\d{2}", text):
            year = int(match.group(0))
            if 1900 <= year <= 2100:
                return year
    return None


def _join_completion_with_institution(
    *,
    completion_rows: list[dict[str, Any]],
    institution_rows: list[dict[str, Any]],
    completions_year_hint: int | None,
) -> list[dict[str, Any]]:
    inst_map: dict[str, dict[str, Any]] = {}
    for raw in institution_rows:
        if not isinstance(raw, dict):
            continue
        row = {_normalise_key(key): raw.get(key) for key in raw.keys()}
        unitid = str(_pick_value(row, _EXTERNAL_ID_ALIASES) or "").strip()
        if not unitid:
            continue
        inst_map[unitid] = row

    merged: list[dict[str, Any]] = []
    for raw in completion_rows:
        if not isinstance(raw, dict):
            continue
        row = {_normalise_key(key): raw.get(key) for key in raw.keys()}
        unitid = str(_pick_value(row, _EXTERNAL_ID_ALIASES) or "").strip()
        if not unitid:
            continue
        inst = inst_map.get(unitid, {})
        cip_code = _normalise_cip_code(_pick_value(row, _CIP_CODE_ALIASES))
        if not cip_code:
            continue
        award_level = _normalise_award_level(_pick_value(row, _AWARD_LEVEL_ALIASES))
        completions = _to_int(_pick_value(row, _COMPLETIONS_ALIASES))
        if completions is None:
            continue
        year = _to_int(_pick_value(row, _YEAR_ALIASES)) or completions_year_hint or datetime.now(timezone.utc).year
        cip_title = str(_pick_value(row, _CIP_TITLE_ALIASES) or "").strip() or f"CIP {cip_code}"
        school_name = str(_pick_value(inst, _NAME_ALIASES) or "").strip()
        state = str(_pick_value(inst, _STATE_ALIASES) or "").strip()
        city = str(_pick_value(inst, _CITY_ALIASES) or "").strip()
        website_url = str(_pick_value(inst, _WEBSITE_ALIASES) or "").strip()
        merged.append(
            {
                "external_id": unitid,
                "school_name": school_name,
                "state": state,
                "city": city,
                "website_url": website_url,
                "year": int(year),
                "cip_code": cip_code,
                "cip_title": cip_title,
                "award_level": award_level,
                "completions": int(completions),
            }
        )
    return merged
