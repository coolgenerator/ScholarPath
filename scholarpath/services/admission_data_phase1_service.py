"""Phase-1 admissions data pipeline service (Bronze/Silver)."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.config import settings
from scholarpath.db.models import (
    AdmissionEvent,
    CausalOutcomeEvent,
    FactQuarantine,
    Institution,
    RawSourceSnapshot,
    RawStructuredRecord,
    School,
    SchoolExternalId,
    SchoolMetricsYear,
    SourceEntityMap,
)
from scholarpath.llm import LLMClient, get_llm_client
from scholarpath.search.sources.college_scorecard import CollegeScorecardSource

logger = logging.getLogger(__name__)

_SCORECARD_FIELDS = [
    "acceptance_rate",
    "sat_25",
    "sat_75",
    "avg_net_price",
    "graduation_rate_4yr",
]

_SCORECARD_ALIAS: dict[str, tuple[str, ...]] = {
    "unitid": ("unitid", "UNITID"),
    "school_name": ("institution_name", "INSTNM", "school_name", "name"),
    "state": ("state", "STABBR", "state_abbr"),
    "city": ("city", "CITY"),
    "website_url": ("website_url", "INSTURL", "school_url"),
    "data_year": ("year", "data_year", "YEAR"),
    "admit_rate": ("acceptance_rate", "ADM_RATE", "admit_rate"),
    "sat_25": (
        "sat_25",
        "SAT_25",
        "SATVR25",
        "SATMT25",
        "SAT_AVG_25",
    ),
    "sat_75": (
        "sat_75",
        "SAT_75",
        "SATVR75",
        "SATMT75",
        "SAT_AVG_75",
    ),
    "avg_net_price": (
        "avg_net_price",
        "NPT4_PUB",
        "NPT4_PRIV",
        "NPT4",
    ),
    "grad_rate": (
        "graduation_rate_4yr",
        "C150_4",
        "grad_rate",
    ),
    "applications": ("applications", "applicants_total", "APPLCN"),
    "admits": ("admits", "admitted_total", "ADMSSN"),
    "enrolled": ("enrolled", "enrolled_total", "ENRLT"),
    "yield_rate": ("yield_rate", "yield"),
}

_IPEDS_ALIAS: dict[str, tuple[str, ...]] = {
    "unitid": ("unitid", "ipeds_unitid", "school_unitid"),
    "school_name": ("institution_name", "school_name", "name"),
    "state": ("state", "state_abbr", "school_state"),
    "city": ("city", "school_city"),
    "website_url": ("website_url", "school_url", "institution_url"),
    "data_year": ("year", "cycle_year", "survey_year", "academic_year"),
    "applications": ("applications", "applicants_total", "applicants"),
    "admits": ("admits", "admitted_total", "admitted"),
    "enrolled": ("enrolled", "enrolled_total", "enrolled"),
    "admit_rate": ("admit_rate", "acceptance_rate", "admission_rate"),
    "yield_rate": ("yield_rate", "admissions_yield_rate"),
    "sat_25": ("sat_25", "sat_total_25", "sat_percentile_25"),
    "sat_75": ("sat_75", "sat_total_75", "sat_percentile_75"),
    "act_25": ("act_25", "act_percentile_25"),
    "act_75": ("act_75", "act_percentile_75"),
    "avg_net_price": ("avg_net_price", "average_net_price"),
    "grad_rate": ("graduation_rate_4yr", "graduation_rate"),
}

# Candidate URLs are best-effort only; caller can override via SCORECARD_BULK_URL.
_SCORECARD_BULK_URL_CANDIDATES = (
    "https://ed-public-download.scorecard.network/downloads/Most-Recent-Cohorts-Institution_05192025.zip",
    "https://ed-public-download.scorecard.network/downloads/Most-Recent-Cohorts-Institution.zip",
)

_PHASE1_DEFAULT_MIN_ADMIT_RATE_COVERAGE = 0.95
_PHASE1_DEFAULT_MIN_NET_PRICE_COVERAGE = 0.95


@dataclass(slots=True)
class SourceSnapshot:
    source_name: str
    source_version: str
    source_url: str | None
    file_path: str | None
    content_hash: str
    pulled_at: datetime


@dataclass(slots=True)
class MetricsCandidate:
    source_name: str
    school_id: UUID
    school_name: str
    unitid: str | None
    data_year: int
    payload: dict[str, Any]
    metrics: dict[str, Any]


def _normalize_name(value: str | None) -> str:
    text = (value or "").strip().lower()
    return " ".join(text.split())


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value != value:  # NaN
            return None
        return int(round(value))
    raw = str(value).strip().replace(",", "")
    if not raw:
        return None
    try:
        if "." in raw:
            return int(round(float(raw)))
        return int(raw)
    except ValueError:
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        v = float(value)
        if v != v:  # NaN
            return None
        return v
    raw = str(value).strip().replace(",", "")
    if not raw:
        return None
    if raw.endswith("%"):
        raw = raw[:-1].strip()
    try:
        return float(raw)
    except ValueError:
        return None


def _coerce_rate(value: Any) -> float | None:
    v = _coerce_float(value)
    if v is None:
        return None
    if v > 1.0 and v <= 100.0:
        v = v / 100.0
    if v < 0.0 or v > 1.0:
        return None
    return v


def _pick(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    for key in aliases:
        if key in row and row[key] not in (None, ""):
            return row[key]
    lowered = {str(k).lower(): v for k, v in row.items()}
    for key in aliases:
        v = lowered.get(str(key).lower())
        if v not in (None, ""):
            return v
    return None


def _extract_metrics_from_row(row: dict[str, Any], alias_map: dict[str, tuple[str, ...]]) -> dict[str, Any]:
    sat_25 = _coerce_int(_pick(row, alias_map.get("sat_25", ("sat_25",))))
    sat_75 = _coerce_int(_pick(row, alias_map.get("sat_75", ("sat_75",))))
    sat_50 = None
    if sat_25 is not None and sat_75 is not None:
        sat_50 = int(round((sat_25 + sat_75) / 2))
    act_25 = _coerce_int(_pick(row, alias_map.get("act_25", ("act_25",))))
    act_75 = _coerce_int(_pick(row, alias_map.get("act_75", ("act_75",))))
    act_50 = None
    if act_25 is not None and act_75 is not None:
        act_50 = int(round((act_25 + act_75) / 2))
    return {
        "applications": _coerce_int(_pick(row, alias_map.get("applications", ("applications",)))),
        "admits": _coerce_int(_pick(row, alias_map.get("admits", ("admits",)))),
        "enrolled": _coerce_int(_pick(row, alias_map.get("enrolled", ("enrolled",)))),
        "admit_rate": _coerce_rate(_pick(row, alias_map.get("admit_rate", ("admit_rate",)))),
        "yield_rate": _coerce_rate(_pick(row, alias_map.get("yield_rate", ("yield_rate",)))),
        "sat_25": sat_25,
        "sat_50": sat_50,
        "sat_75": sat_75,
        "act_25": act_25,
        "act_50": act_50,
        "act_75": act_75,
        "avg_net_price": _coerce_float(_pick(row, alias_map.get("avg_net_price", ("avg_net_price",)))),
        "grad_rate": _coerce_rate(_pick(row, alias_map.get("grad_rate", ("grad_rate",)))),
    }


def _validate_metrics(metrics: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    applications = metrics.get("applications")
    admits = metrics.get("admits")
    enrolled = metrics.get("enrolled")
    if applications is not None and admits is not None and applications < admits:
        issues.append("applications_lt_admits")
    if admits is not None and enrolled is not None and admits < enrolled:
        issues.append("admits_lt_enrolled")
    for rate_key in ("admit_rate", "yield_rate", "grad_rate"):
        rate = metrics.get(rate_key)
        if rate is not None and (rate < 0.0 or rate > 1.0):
            issues.append(f"{rate_key}_out_of_range")
    return issues


def _read_csv_rows(payload: bytes, *, filename: str | None = None) -> list[dict[str, Any]]:
    if filename and filename.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            if not members:
                return []
            # Prefer institution-level file when multiple members exist.
            members.sort(key=lambda item: ("institution" not in item.lower(), item))
            with zf.open(members[0], "r") as fh:
                text = fh.read().decode("utf-8", errors="ignore")
                return list(csv.DictReader(io.StringIO(text)))
    text = payload.decode("utf-8", errors="ignore")
    return list(csv.DictReader(io.StringIO(text)))


async def _download_bytes(url: str) -> tuple[bytes, str | None]:
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content, resp.headers.get("last-modified")


async def _load_payload_from_path_or_url(
    *,
    local_path: str | None,
    url: str | None,
    fallback_urls: tuple[str, ...] = (),
    download_dir: Path,
    filename_hint: str,
) -> tuple[bytes | None, str | None, str | None, str]:
    """Load bytes from local path first, then URL; return payload + metadata."""
    if local_path:
        path = Path(local_path)
        if path.exists():
            payload = path.read_bytes()
            return payload, None, str(path), path.name

    urls = []
    if url:
        urls.append(url)
    urls.extend(fallback_urls)
    for candidate in urls:
        try:
            payload, last_modified = await _download_bytes(candidate)
            suffix = Path(candidate).suffix or ".csv"
            local_name = f"{filename_hint}{suffix}"
            download_dir.mkdir(parents=True, exist_ok=True)
            out_path = download_dir / local_name
            out_path.write_bytes(payload)
            return payload, candidate, str(out_path), last_modified or local_name
        except Exception as exc:  # pragma: no cover - network unstable
            logger.warning("Download failed for %s: %s", candidate, exc)
            continue
    return None, None, None, "missing"


async def _upsert_raw_source_snapshot(
    session: AsyncSession,
    *,
    source_name: str,
    source_version: str,
    source_url: str | None,
    file_path: str | None,
    content_hash: str,
    pulled_at: datetime,
    metadata: dict[str, Any] | None,
) -> tuple[RawSourceSnapshot, bool]:
    row = await session.scalar(
        select(RawSourceSnapshot).where(
            and_(
                RawSourceSnapshot.source_name == source_name,
                RawSourceSnapshot.source_version == source_version,
                RawSourceSnapshot.content_hash == content_hash,
            )
        )
    )
    if row is not None:
        return row, False
    row = RawSourceSnapshot(
        source_name=source_name,
        source_version=source_version,
        source_url=source_url,
        file_path=file_path,
        content_hash=content_hash,
        pulled_at=pulled_at,
        metadata_=metadata or {},
    )
    session.add(row)
    await session.flush()
    return row, True


async def _upsert_raw_structured_record(
    session: AsyncSession,
    *,
    snapshot_id: UUID,
    source_name: str,
    record_key: str,
    data_year: int,
    school_name: str | None,
    state: str | None,
    city: str | None,
    external_id: str | None,
    payload: dict[str, Any],
    parse_status: str,
    metadata: dict[str, Any] | None,
) -> bool:
    row = await session.scalar(
        select(RawStructuredRecord).where(
            and_(
                RawStructuredRecord.snapshot_id == snapshot_id,
                RawStructuredRecord.record_key == record_key,
            )
        )
    )
    if row is not None:
        return False
    session.add(
        RawStructuredRecord(
            snapshot_id=snapshot_id,
            source_name=source_name,
            record_key=record_key,
            data_year=data_year,
            school_name=school_name,
            state=state,
            city=city,
            external_id=external_id,
            payload=payload,
            parse_status=parse_status,
            metadata_=metadata or {},
        )
    )
    return True


async def _upsert_source_entity_map(
    session: AsyncSession,
    *,
    school_id: UUID,
    source_name: str,
    external_id: str | None,
    source_school_name: str,
    source_state: str | None,
    source_city: str | None,
    match_method: str,
    match_confidence: float,
    metadata: dict[str, Any] | None,
) -> bool:
    row = None
    if external_id:
        row = await session.scalar(
            select(SourceEntityMap).where(
                and_(
                    SourceEntityMap.source_name == source_name,
                    SourceEntityMap.external_id == external_id,
                )
            )
        )
    if row is None:
        row = await session.scalar(
            select(SourceEntityMap).where(
                and_(
                    SourceEntityMap.school_id == school_id,
                    SourceEntityMap.source_name == source_name,
                    SourceEntityMap.is_primary.is_(True),
                )
            )
        )

    if row is None:
        session.add(
            SourceEntityMap(
                school_id=school_id,
                source_name=source_name,
                external_id=external_id,
                source_school_name=source_school_name,
                source_state=source_state,
                source_city=source_city,
                match_method=match_method,
                match_confidence=match_confidence,
                is_primary=True,
                metadata_=metadata or {},
            )
        )
        return True

    row.external_id = external_id or row.external_id
    row.source_school_name = source_school_name
    row.source_state = source_state
    row.source_city = source_city
    row.match_method = match_method
    row.match_confidence = match_confidence
    row.metadata_ = metadata or row.metadata_
    return False


async def _upsert_institution(
    session: AsyncSession,
    *,
    source_name: str,
    unitid: str,
    school_id: UUID | None,
    institution_name: str,
    state: str | None,
    city: str | None,
    website_url: str | None,
    opeid6: str | None,
    metadata: dict[str, Any] | None,
) -> bool:
    row = await session.scalar(
        select(Institution).where(
            and_(
                Institution.source_name == source_name,
                Institution.unitid == unitid,
            )
        )
    )
    if row is None:
        session.add(
            Institution(
                source_name=source_name,
                unitid=unitid,
                school_id=school_id,
                institution_name=institution_name,
                state=state,
                city=city,
                website_url=website_url,
                opeid6=opeid6,
                metadata_=metadata or {},
            )
        )
        return True
    row.school_id = school_id or row.school_id
    row.institution_name = institution_name or row.institution_name
    row.state = state or row.state
    row.city = city or row.city
    row.website_url = website_url or row.website_url
    row.opeid6 = opeid6 or row.opeid6
    row.metadata_ = metadata or row.metadata_
    return False


async def _upsert_school_metrics_year(
    session: AsyncSession,
    *,
    school_id: UUID,
    source_name: str,
    metric_year: int,
    metrics: dict[str, Any],
    metadata: dict[str, Any] | None,
) -> bool:
    row = await session.scalar(
        select(SchoolMetricsYear).where(
            and_(
                SchoolMetricsYear.school_id == school_id,
                SchoolMetricsYear.source_name == source_name,
                SchoolMetricsYear.metric_year == metric_year,
            )
        )
    )
    if row is None:
        session.add(
            SchoolMetricsYear(
                school_id=school_id,
                source_name=source_name,
                metric_year=metric_year,
                applications=metrics.get("applications"),
                admits=metrics.get("admits"),
                enrolled=metrics.get("enrolled"),
                admit_rate=metrics.get("admit_rate"),
                yield_rate=metrics.get("yield_rate"),
                sat_25=metrics.get("sat_25"),
                sat_50=metrics.get("sat_50"),
                sat_75=metrics.get("sat_75"),
                act_25=metrics.get("act_25"),
                act_50=metrics.get("act_50"),
                act_75=metrics.get("act_75"),
                avg_net_price=metrics.get("avg_net_price"),
                grad_rate=metrics.get("grad_rate"),
                metadata_=metadata or {},
            )
        )
        return True
    row.applications = metrics.get("applications")
    row.admits = metrics.get("admits")
    row.enrolled = metrics.get("enrolled")
    row.admit_rate = metrics.get("admit_rate")
    row.yield_rate = metrics.get("yield_rate")
    row.sat_25 = metrics.get("sat_25")
    row.sat_50 = metrics.get("sat_50")
    row.sat_75 = metrics.get("sat_75")
    row.act_25 = metrics.get("act_25")
    row.act_50 = metrics.get("act_50")
    row.act_75 = metrics.get("act_75")
    row.avg_net_price = metrics.get("avg_net_price")
    row.grad_rate = metrics.get("grad_rate")
    row.metadata_ = metadata or row.metadata_
    return False


async def _quarantine_issue(
    session: AsyncSession,
    *,
    school_id: UUID | None,
    cycle_year: int,
    outcome_name: str,
    raw_value: str,
    stage: str,
    reason: str,
    source_name: str,
    source_url: str | None = None,
    confidence: float | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    session.add(
        FactQuarantine(
            student_id=None,
            school_id=school_id,
            cycle_year=cycle_year,
            outcome_name=outcome_name,
            raw_value=raw_value,
            stage=stage,
            reason=reason,
            source_name=source_name,
            source_url=source_url,
            confidence=confidence,
            metadata_=metadata or {},
        )
    )


async def _judge_with_llm(
    llm: LLMClient | None,
    *,
    run_id: str,
    context: dict[str, Any],
) -> dict[str, Any]:
    if llm is None:
        return {
            "decision": "reject",
            "confidence": 0.0,
            "reason": "llm_unavailable",
        }
    schema = {
        "type": "object",
        "properties": {
            "decision": {"type": "string", "enum": ["keep", "reject"]},
            "confidence": {"type": "number"},
            "reason": {"type": "string"},
        },
        "required": ["decision", "confidence", "reason"],
    }
    try:
        payload = await llm.complete_json(
            [
                {
                    "role": "system",
                    "content": (
                        "You are a strict admissions data quality judge. "
                        "Reject impossible or contradictory records. Keep records only when issues are benign."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(context, ensure_ascii=False),
                },
            ],
            schema=schema,
            temperature=0.1,
            max_tokens=300,
            caller=f"admission.phase1.clean.judge#{run_id}",
        )
        decision = str(payload.get("decision") or "reject").strip().lower()
        confidence = float(payload.get("confidence") or 0.0)
        reason = str(payload.get("reason") or "judge_no_reason").strip()
        if decision not in {"keep", "reject"}:
            decision = "reject"
        return {
            "decision": decision,
            "confidence": max(0.0, min(confidence, 1.0)),
            "reason": reason or "judge_no_reason",
        }
    except Exception as exc:  # pragma: no cover - runtime/network dependent
        logger.warning("Phase1 LLM judge failed: %s", exc)
        return {
            "decision": "reject",
            "confidence": 0.0,
            "reason": "judge_call_failed",
        }


async def _build_scorecard_api_rows(
    *,
    schools: list[School],
    metric_year: int,
    run_id: str,
) -> list[dict[str, Any]]:
    api_key = (settings.SCORECARD_API_KEY or "").strip()
    if not api_key:
        return []
    source = CollegeScorecardSource(api_key=api_key)
    rows: list[dict[str, Any]] = []
    for school in schools:
        candidate_names = [school.name]
        compact = school.name.replace(",", " ").replace("  ", " ").strip()
        if compact and compact not in candidate_names:
            candidate_names.append(compact)
        if "," in school.name:
            leading = school.name.split(",", 1)[0].strip()
            if leading and leading not in candidate_names:
                candidate_names.append(leading)

        items = []
        for name in candidate_names:
            try:
                items = await source.search(
                    name,
                    fields=_SCORECARD_FIELDS,
                )
            except Exception:
                logger.warning("Scorecard API supplement failed for %s", name, exc_info=True)
                items = []
            if items:
                break

        payload = {
            "unitid": None,
            "school_name": school.name,
            "state": school.state,
            "city": school.city,
            "data_year": metric_year,
            "admit_rate": None,
            "sat_25": None,
            "sat_75": None,
            "avg_net_price": None,
            "grad_rate": None,
        }
        for item in items:
            if item.variable_name == "acceptance_rate":
                payload["admit_rate"] = item.value_numeric
            elif item.variable_name == "sat_25":
                payload["sat_25"] = item.value_numeric
            elif item.variable_name == "sat_75":
                payload["sat_75"] = item.value_numeric
            elif item.variable_name == "avg_net_price":
                payload["avg_net_price"] = item.value_numeric
            elif item.variable_name == "graduation_rate_4yr":
                payload["grad_rate"] = item.value_numeric
            if isinstance(item.raw_data, dict):
                external = item.raw_data.get("record_id")
                if external:
                    payload["unitid"] = str(external)
        payload["record_key"] = f"scorecard_api:{school.id}:{metric_year}"
        payload["source_url"] = "https://api.data.gov/ed/collegescorecard/v1/schools.json"
        payload["metadata"] = {
            "run_id": run_id,
            "source_kind": "scorecard_api_supplement",
        }
        rows.append(payload)
    return rows


def _rows_for_source(
    rows: list[dict[str, Any]],
    *,
    alias_map: dict[str, tuple[str, ...]],
    source_name: str,
    fallback_year: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        unitid = _pick(row, alias_map.get("unitid", ("unitid",)))
        school_name = _pick(row, alias_map.get("school_name", ("school_name",)))
        data_year = _coerce_int(_pick(row, alias_map.get("data_year", ("data_year",)))) or fallback_year
        source_row = {
            "source_name": source_name,
            "unitid": str(unitid).strip() if unitid is not None else None,
            "school_name": str(school_name).strip() if school_name is not None else "",
            "state": str(_pick(row, alias_map.get("state", ("state",))) or "").strip() or None,
            "city": str(_pick(row, alias_map.get("city", ("city",))) or "").strip() or None,
            "website_url": str(_pick(row, alias_map.get("website_url", ("website_url",))) or "").strip() or None,
            "data_year": int(data_year),
            "metrics": _extract_metrics_from_row(row, alias_map),
            "payload": row,
            "record_key": str(
                row.get("record_key")
                or f"{str(unitid or '').strip()}:{str(data_year)}:{_normalize_name(str(school_name or ''))}"
            ),
            "source_url": str(row.get("source_url") or "").strip() or None,
            "metadata": dict(row.get("metadata") or {}),
        }
        if not source_row["school_name"]:
            continue
        out.append(source_row)
    return out


async def _count_truth_tables(session: AsyncSession) -> dict[str, int]:
    admission_events = int((await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0)
    causal_outcomes = int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0)
    return {
        "admission_events": admission_events,
        "causal_outcome_events": causal_outcomes,
    }


def _evaluate_phase1_gate(
    summary: dict[str, Any],
    *,
    min_admit_rate_coverage: float,
    min_net_price_coverage: float,
) -> dict[str, Any]:
    coverage = dict(summary.get("coverage") or {})
    sources = dict(summary.get("sources") or {})
    truth_counts = dict(summary.get("truth_counts") or {})
    before_truth = dict(truth_counts.get("before") or {})
    after_truth = dict(truth_counts.get("after") or {})

    mapped_school_rate = float(coverage.get("mapped_school_rate") or 0.0)
    admit_rate_coverage = float(coverage.get("admit_rate_school_coverage") or 0.0)
    net_price_coverage = float(coverage.get("avg_net_price_school_coverage") or 0.0)
    scorecard_bulk = dict(sources.get("college_scorecard_bulk") or {})
    scorecard_bulk_rows = int(scorecard_bulk.get("rows_read") or 0)

    reasons: list[str] = []
    if mapped_school_rate != 1.0:
        reasons.append("mapped_school_rate_not_1.0")
    if admit_rate_coverage < min_admit_rate_coverage:
        reasons.append(f"admit_rate_coverage_lt_{min_admit_rate_coverage:.2f}")
    if net_price_coverage < min_net_price_coverage:
        reasons.append(f"avg_net_price_coverage_lt_{min_net_price_coverage:.2f}")
    if scorecard_bulk_rows <= 0:
        reasons.append("scorecard_bulk_rows_read_eq_0")

    for field in ("admission_events", "causal_outcome_events"):
        if int(after_truth.get(field) or 0) != int(before_truth.get(field) or 0):
            reasons.append(f"{field}_changed")

    return {
        "passed": len(reasons) == 0,
        "reasons": reasons,
        "thresholds": {
            "mapped_school_rate_eq": 1.0,
            "min_admit_rate_coverage": min_admit_rate_coverage,
            "min_net_price_coverage": min_net_price_coverage,
            "scorecard_bulk_rows_read_gt": 0,
            "admission_events_unchanged": True,
            "causal_outcome_events_unchanged": True,
        },
        "observed": {
            "mapped_school_rate": mapped_school_rate,
            "admit_rate_coverage": admit_rate_coverage,
            "avg_net_price_coverage": net_price_coverage,
            "scorecard_bulk_rows_read": scorecard_bulk_rows,
            "truth_counts_before": before_truth,
            "truth_counts_after": after_truth,
        },
    }


async def run_admission_phase1_pipeline(
    session: AsyncSession,
    *,
    run_id: str,
    scope: str = "existing_65",
    dry_run: bool = False,
    resume_run_id: str | None = None,
    output_dir: str = ".benchmarks/official_phase1",
    metric_year: int | None = None,
    run_gate: bool = True,
    min_admit_rate_coverage: float = _PHASE1_DEFAULT_MIN_ADMIT_RATE_COVERAGE,
    min_net_price_coverage: float = _PHASE1_DEFAULT_MIN_NET_PRICE_COVERAGE,
    llm: LLMClient | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    year = int(metric_year or now.year)
    llm_client = llm or get_llm_client()

    schools = list((await session.execute(select(School).order_by(School.name.asc()))).scalars().all())
    if scope == "existing_65":
        # Current production expectation is 65 schools; use all current rows in this scope.
        scoped_schools = schools
    else:
        scoped_schools = schools
    school_ids = {item.id for item in scoped_schools}
    school_name_state_map = {
        (_normalize_name(item.name), _normalize_name(item.state)): item
        for item in scoped_schools
    }

    external_rows = (
        (
            await session.execute(
                select(SchoolExternalId).where(
                    SchoolExternalId.provider == "ipeds",
                    SchoolExternalId.school_id.in_([item.id for item in scoped_schools]),
                )
            )
        )
        .scalars()
        .all()
    )
    school_by_ipeds = {
        str(row.external_id).strip(): row.school_id
        for row in external_rows
        if str(row.external_id).strip()
    }
    school_by_id = {item.id: item for item in scoped_schools}

    run_root = Path(output_dir).expanduser().resolve() / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    data_root = run_root / "datasets"
    data_root.mkdir(parents=True, exist_ok=True)
    truth_before = await _count_truth_tables(session)

    summary: dict[str, Any] = {
        "status": "ok",
        "run_id": run_id,
        "scope": scope,
        "dry_run": dry_run,
        "resume_run_id": resume_run_id,
        "metric_year": year,
        "run_gate": run_gate,
        "started_at": now.isoformat(),
        "schools_total_in_scope": len(scoped_schools),
        "sources": {},
        "counts": {
            "raw_source_snapshots_created": 0,
            "raw_structured_records_created": 0,
            "source_entity_maps_created": 0,
            "institutions_created": 0,
            "school_metrics_created": 0,
            "school_metrics_updated": 0,
            "quarantine_created": 0,
            "llm_judge_calls": 0,
            "llm_judge_keep": 0,
            "llm_judge_reject": 0,
        },
        "coverage": {},
        "issues": {
            "invalid_rows": 0,
            "cross_source_conflicts": 0,
        },
        "truth_counts": {
            "before": truth_before,
            "after": {},
            "delta": {},
        },
    }

    # ------------------------------------------------------------------
    # Load Scorecard bulk
    # ------------------------------------------------------------------
    scorecard_payload, scorecard_url, scorecard_file_path, scorecard_version_hint = await _load_payload_from_path_or_url(
        local_path=(settings.SCORECARD_BULK_PATH or "").strip() or None,
        url=(settings.SCORECARD_BULK_URL or "").strip() or None,
        fallback_urls=_SCORECARD_BULK_URL_CANDIDATES,
        download_dir=data_root / "scorecard",
        filename_hint="scorecard_bulk",
    )
    scorecard_rows: list[dict[str, Any]] = []
    scorecard_hash: str | None = None
    scorecard_source_status = "missing"
    if scorecard_payload is not None:
        scorecard_rows = _read_csv_rows(scorecard_payload, filename=scorecard_file_path)
        scorecard_hash = hashlib.sha256(scorecard_payload).hexdigest()
        source_version = f"{scorecard_version_hint}:{scorecard_hash[:12]}"
        scorecard_source_status = "ok"
        if not dry_run:
            snapshot, snapshot_created = await _upsert_raw_source_snapshot(
                session,
                source_name="college_scorecard_bulk",
                source_version=source_version,
                source_url=scorecard_url,
                file_path=scorecard_file_path,
                content_hash=scorecard_hash,
                pulled_at=now,
                metadata={"run_id": run_id},
            )
            summary["counts"]["raw_source_snapshots_created"] += int(snapshot_created)
            snapshot_id = snapshot.id
        else:
            snapshot_id = None

        source_rows = _rows_for_source(
            scorecard_rows,
            alias_map=_SCORECARD_ALIAS,
            source_name="college_scorecard_bulk",
            fallback_year=year,
        )
    else:
        snapshot_id = None
        source_rows = []

    # API supplement for missing/bulk gaps.
    scorecard_api_rows = await _build_scorecard_api_rows(
        schools=scoped_schools,
        metric_year=year,
        run_id=run_id,
    )
    source_rows.extend(
        _rows_for_source(
            scorecard_api_rows,
            alias_map={
                "unitid": ("unitid",),
                "school_name": ("school_name",),
                "state": ("state",),
                "city": ("city",),
                "website_url": ("website_url",),
                "data_year": ("data_year",),
                "admit_rate": ("admit_rate",),
                "sat_25": ("sat_25",),
                "sat_75": ("sat_75",),
                "avg_net_price": ("avg_net_price",),
                "grad_rate": ("grad_rate",),
                "applications": ("applications",),
                "admits": ("admits",),
                "enrolled": ("enrolled",),
                "yield_rate": ("yield_rate",),
            },
            source_name="college_scorecard_api",
            fallback_year=year,
        )
    )

    # ------------------------------------------------------------------
    # Load IPEDS bulk
    # ------------------------------------------------------------------
    ipeds_default_path = Path(".benchmarks/datasets/ipeds/ipeds_cn_adm_hd_2019_2023.csv")
    ipeds_local_path = (settings.IPEDS_DATASET_PATH or "").strip() or (
        str(ipeds_default_path) if ipeds_default_path.exists() else None
    )
    ipeds_payload, ipeds_url, ipeds_file_path, ipeds_version_hint = await _load_payload_from_path_or_url(
        local_path=ipeds_local_path,
        url=(settings.IPEDS_DATASET_URL or "").strip() or None,
        download_dir=data_root / "ipeds",
        filename_hint="ipeds_bulk",
    )
    ipeds_source_rows: list[dict[str, Any]] = []
    ipeds_hash: str | None = None
    ipeds_source_status = "missing"
    if ipeds_payload is not None:
        ipeds_rows = _read_csv_rows(ipeds_payload, filename=ipeds_file_path)
        ipeds_hash = hashlib.sha256(ipeds_payload).hexdigest()
        ipeds_version = f"{ipeds_version_hint}:{ipeds_hash[:12]}"
        ipeds_source_status = "ok"
        if not dry_run:
            snapshot, snapshot_created = await _upsert_raw_source_snapshot(
                session,
                source_name="ipeds_bulk",
                source_version=ipeds_version,
                source_url=ipeds_url,
                file_path=ipeds_file_path,
                content_hash=ipeds_hash,
                pulled_at=now,
                metadata={"run_id": run_id},
            )
            summary["counts"]["raw_source_snapshots_created"] += int(snapshot_created)
            ipeds_snapshot_id = snapshot.id
        else:
            ipeds_snapshot_id = None
        ipeds_source_rows = _rows_for_source(
            ipeds_rows,
            alias_map=_IPEDS_ALIAS,
            source_name="ipeds_bulk",
            fallback_year=year,
        )
    else:
        ipeds_snapshot_id = None

    all_rows = source_rows + ipeds_source_rows
    mapped_candidates: list[MetricsCandidate] = []
    raw_created = 0

    for row in all_rows:
        source_name = str(row["source_name"])
        unitid = str(row.get("unitid") or "").strip() or None
        school_name = str(row.get("school_name") or "").strip()
        state = str(row.get("state") or "").strip()
        city = str(row.get("city") or "").strip() or None
        data_year = int(row.get("data_year") or year)

        school_id: UUID | None = None
        match_method = "none"
        match_confidence = 0.0

        if unitid and unitid in school_by_ipeds:
            school_id = school_by_ipeds[unitid]
            match_method = "external_id"
            match_confidence = 0.99
        if school_id is None:
            by_name = school_name_state_map.get((_normalize_name(school_name), _normalize_name(state)))
            if by_name is not None:
                school_id = by_name.id
                match_method = "name_state"
                match_confidence = 0.88

        if school_id is None or school_id not in school_ids:
            continue

        if not dry_run:
            snapshot_for_row: UUID | None = (
                snapshot_id if source_name.startswith("college_scorecard") else ipeds_snapshot_id
            )
            if snapshot_for_row:
                created = await _upsert_raw_structured_record(
                    session,
                    snapshot_id=snapshot_for_row,
                    source_name=source_name,
                    record_key=str(row.get("record_key") or f"{unitid}:{data_year}:{_normalize_name(school_name)}"),
                    data_year=data_year,
                    school_name=school_name,
                    state=state or None,
                    city=city,
                    external_id=unitid,
                    payload=dict(row.get("payload") or {}),
                    parse_status="raw",
                    metadata={
                        "run_id": run_id,
                        "match_method": match_method,
                        "match_confidence": match_confidence,
                    },
                )
                raw_created += int(created)

            map_created = await _upsert_source_entity_map(
                session,
                school_id=school_id,
                source_name=source_name,
                external_id=unitid,
                source_school_name=school_name,
                source_state=state or None,
                source_city=city,
                match_method=match_method,
                match_confidence=match_confidence,
                metadata={"run_id": run_id},
            )
            summary["counts"]["source_entity_maps_created"] += int(map_created)

            if unitid:
                inst_created = await _upsert_institution(
                    session,
                    source_name=source_name,
                    unitid=unitid,
                    school_id=school_id,
                    institution_name=school_name,
                    state=state or None,
                    city=city,
                    website_url=row.get("website_url"),
                    opeid6=None,
                    metadata={"run_id": run_id},
                )
                summary["counts"]["institutions_created"] += int(inst_created)

        mapped_candidates.append(
            MetricsCandidate(
                source_name=source_name,
                school_id=school_id,
                school_name=school_by_id[school_id].name,
                unitid=unitid,
                data_year=data_year,
                payload=dict(row.get("payload") or {}),
                metrics=dict(row.get("metrics") or {}),
            )
        )

    summary["counts"]["raw_structured_records_created"] = raw_created

    # ------------------------------------------------------------------
    # Cleaning + LLM judge + Silver write
    # ------------------------------------------------------------------
    kept_candidates: list[MetricsCandidate] = []
    for candidate in mapped_candidates:
        issues = _validate_metrics(candidate.metrics)
        if not issues:
            kept_candidates.append(candidate)
            continue
        summary["issues"]["invalid_rows"] += 1
        summary["counts"]["llm_judge_calls"] += 1
        judge = await _judge_with_llm(
            llm_client,
            run_id=run_id,
            context={
                "source_name": candidate.source_name,
                "school_name": candidate.school_name,
                "data_year": candidate.data_year,
                "metrics": candidate.metrics,
                "issues": issues,
            },
        )
        if judge["decision"] == "keep":
            summary["counts"]["llm_judge_keep"] += 1
            candidate.metrics = dict(candidate.metrics)
            candidate.metrics["metadata_judge"] = judge
            kept_candidates.append(candidate)
        else:
            summary["counts"]["llm_judge_reject"] += 1
            summary["counts"]["quarantine_created"] += 1
            if not dry_run:
                await _quarantine_issue(
                    session,
                    school_id=candidate.school_id,
                    cycle_year=candidate.data_year,
                    outcome_name="school_metrics_year",
                    raw_value=json.dumps(candidate.metrics, ensure_ascii=False),
                    stage="phase1_rule_validation",
                    reason="|".join(issues),
                    source_name=candidate.source_name,
                    confidence=float(judge.get("confidence") or 0.0),
                    metadata={
                        "run_id": run_id,
                        "judge": judge,
                    },
                )

    # Cross-source conflict detection (admit_rate and avg_net_price).
    grouped = defaultdict(list)
    for candidate in kept_candidates:
        grouped[(candidate.school_id, candidate.data_year)].append(candidate)
    for (school_id, data_year), rows in grouped.items():
        if len(rows) < 2:
            continue
        conflict_reasons: list[str] = []
        rates = [item.metrics.get("admit_rate") for item in rows if item.metrics.get("admit_rate") is not None]
        if len(rates) >= 2 and (max(rates) - min(rates) > 0.2):
            conflict_reasons.append("admit_rate_conflict_gt_0.2")
        prices = [item.metrics.get("avg_net_price") for item in rows if item.metrics.get("avg_net_price") is not None]
        if len(prices) >= 2 and (max(prices) - min(prices) > 15000):
            conflict_reasons.append("avg_net_price_conflict_gt_15000")
        if not conflict_reasons:
            continue

        summary["issues"]["cross_source_conflicts"] += 1
        summary["counts"]["llm_judge_calls"] += 1
        judge = await _judge_with_llm(
            llm_client,
            run_id=run_id,
            context={
                "type": "cross_source_conflict",
                "school_id": str(school_id),
                "school_name": school_by_id.get(school_id).name if school_by_id.get(school_id) else "",
                "data_year": data_year,
                "reasons": conflict_reasons,
                "source_rows": [
                    {
                        "source_name": item.source_name,
                        "metrics": item.metrics,
                    }
                    for item in rows
                ],
            },
        )
        if judge["decision"] == "keep":
            summary["counts"]["llm_judge_keep"] += 1
        else:
            summary["counts"]["llm_judge_reject"] += 1
            summary["counts"]["quarantine_created"] += 1
            if not dry_run:
                await _quarantine_issue(
                    session,
                    school_id=school_id,
                    cycle_year=data_year,
                    outcome_name="school_metrics_year",
                    raw_value=json.dumps(
                        [
                            {"source_name": item.source_name, "metrics": item.metrics}
                            for item in rows
                        ],
                        ensure_ascii=False,
                    ),
                    stage="phase1_cross_source_conflict",
                    reason="|".join(conflict_reasons),
                    source_name="multi_source",
                    confidence=float(judge.get("confidence") or 0.0),
                    metadata={"run_id": run_id, "judge": judge},
                )

    for candidate in kept_candidates:
        metadata = {
            "run_id": run_id,
            "unitid": candidate.unitid,
            "source_payload_keys": sorted(candidate.payload.keys())[:40],
        }
        metrics_copy = dict(candidate.metrics)
        judge_meta = metrics_copy.pop("metadata_judge", None)
        if judge_meta is not None:
            metadata["judge"] = judge_meta
        if not dry_run:
            created = await _upsert_school_metrics_year(
                session,
                school_id=candidate.school_id,
                source_name=candidate.source_name,
                metric_year=candidate.data_year,
                metrics=metrics_copy,
                metadata=metadata,
            )
            if created:
                summary["counts"]["school_metrics_created"] += 1
            else:
                summary["counts"]["school_metrics_updated"] += 1

    # Coverage report.
    if kept_candidates:
        by_school = defaultdict(list)
        for item in kept_candidates:
            by_school[item.school_id].append(item)
        mapped_schools = len(by_school)
        admit_rate_cov = 0
        net_price_cov = 0
        year_counter = Counter()
        for school_id, rows in by_school.items():
            _ = school_id
            year_counter.update(str(item.data_year) for item in rows)
            if any(item.metrics.get("admit_rate") is not None for item in rows):
                admit_rate_cov += 1
            if any(item.metrics.get("avg_net_price") is not None for item in rows):
                net_price_cov += 1
        summary["coverage"] = {
            "mapped_schools": mapped_schools,
            "mapped_school_rate": round(mapped_schools / max(1, len(scoped_schools)), 4),
            "admit_rate_school_coverage": round(admit_rate_cov / max(1, len(scoped_schools)), 4),
            "avg_net_price_school_coverage": round(net_price_cov / max(1, len(scoped_schools)), 4),
            "metric_year_distribution": dict(year_counter),
        }
    else:
        summary["coverage"] = {
            "mapped_schools": 0,
            "mapped_school_rate": 0.0,
            "admit_rate_school_coverage": 0.0,
            "avg_net_price_school_coverage": 0.0,
            "metric_year_distribution": {},
        }

    summary["sources"]["college_scorecard_bulk"] = {
        "status": scorecard_source_status,
        "rows_read": len(scorecard_rows),
        "rows_mapped": len([item for item in mapped_candidates if item.source_name == "college_scorecard_bulk"]),
        "file_path": scorecard_file_path,
        "source_url": scorecard_url,
        "content_hash": scorecard_hash,
    }
    summary["sources"]["college_scorecard_api"] = {
        "status": "ok" if scorecard_api_rows else "missing",
        "rows_read": len(scorecard_api_rows),
        "rows_mapped": len([item for item in mapped_candidates if item.source_name == "college_scorecard_api"]),
    }
    summary["sources"]["ipeds_bulk"] = {
        "status": ipeds_source_status,
        "rows_read": len(ipeds_source_rows),
        "rows_mapped": len([item for item in mapped_candidates if item.source_name == "ipeds_bulk"]),
        "file_path": ipeds_file_path,
        "source_url": ipeds_url,
        "content_hash": ipeds_hash,
    }

    truth_after = await _count_truth_tables(session)
    summary["truth_counts"]["after"] = truth_after
    summary["truth_counts"]["delta"] = {
        key: int(truth_after.get(key) or 0) - int(truth_before.get(key) or 0)
        for key in sorted(set(truth_before) | set(truth_after))
    }

    if run_gate:
        summary["gate"] = _evaluate_phase1_gate(
            summary,
            min_admit_rate_coverage=min_admit_rate_coverage,
            min_net_price_coverage=min_net_price_coverage,
        )
    else:
        summary["gate"] = {
            "passed": True,
            "skipped": True,
            "reasons": [],
            "thresholds": {},
            "observed": {},
        }

    if not summary["gate"].get("passed", False):
        summary["status"] = "gate_failed"

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()

    json_path = run_root / "phase1_report.json"
    md_path = run_root / "phase1_report.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    md_lines = [
        f"# Admission Phase1 Report `{run_id}`",
        "",
        f"- scope: `{scope}`",
        f"- dry_run: `{dry_run}`",
        f"- schools_in_scope: `{len(scoped_schools)}`",
        f"- mapped_school_rate: `{summary['coverage'].get('mapped_school_rate', 0.0)}`",
        f"- admit_rate_coverage: `{summary['coverage'].get('admit_rate_school_coverage', 0.0)}`",
        f"- avg_net_price_coverage: `{summary['coverage'].get('avg_net_price_school_coverage', 0.0)}`",
        f"- invalid_rows: `{summary['issues'].get('invalid_rows', 0)}`",
        f"- cross_source_conflicts: `{summary['issues'].get('cross_source_conflicts', 0)}`",
        f"- school_metrics_created: `{summary['counts'].get('school_metrics_created', 0)}`",
        f"- school_metrics_updated: `{summary['counts'].get('school_metrics_updated', 0)}`",
        f"- scorecard_bulk_rows_read: `{summary['sources'].get('college_scorecard_bulk', {}).get('rows_read', 0)}`",
        f"- scorecard_bulk_status: `{summary['sources'].get('college_scorecard_bulk', {}).get('status', 'missing')}`",
        f"- admission_events_before_after: `{summary['truth_counts'].get('before', {}).get('admission_events', 0)} -> {summary['truth_counts'].get('after', {}).get('admission_events', 0)}`",
        f"- causal_outcome_events_before_after: `{summary['truth_counts'].get('before', {}).get('causal_outcome_events', 0)} -> {summary['truth_counts'].get('after', {}).get('causal_outcome_events', 0)}`",
        f"- gate_passed: `{summary['gate'].get('passed', False)}`",
        f"- gate_reasons: `{', '.join(summary['gate'].get('reasons', [])) or 'none'}`",
    ]
    md_path.write_text("\n".join(md_lines), encoding="utf-8")
    summary["report_json"] = str(json_path)
    summary["report_md"] = str(md_path)

    if not dry_run:
        await session.flush()
    return summary
