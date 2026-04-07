"""Phase-4 training preparation service for strict true-only multi-outcome labels."""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from sqlalchemy import and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.config import settings
from scholarpath.db.models import (
    CanonicalFact,
    CausalFeatureSnapshot,
    CausalOutcomeEvent,
    School,
    SchoolExternalId,
    SchoolMetricsYear,
    SourceEntityMap,
)
from scholarpath.llm import LLMClient
from scholarpath.scripts.causal_staged_train import _check_stage_data_gate
from scholarpath.search.canonical_merge import coerce_numeric, fingerprint_value, normalise_variable_name
from scholarpath.services.causal_data_service import ingest_official_facts

logger = logging.getLogger(__name__)

PHASE4_OUTCOME_SOURCE = "official_school_year_truth_v1"
PHASE4_REQUIRED_FACT_FIELDS = [
    "graduation_rate_4yr",
    "retention_rate",
    "median_earnings_10yr",
    "doctoral_completions_share",
]
_NON_ADMISSION_OUTCOMES = [
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]
_PHASE4_COVERAGE_OUTCOMES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]

_DEFAULT_IPEDS_COMPLETIONS_PATH = ".benchmarks/datasets/ipeds/ipeds_completions_2019_2023.csv"

_UNITID_ALIASES = (
    "unitid",
    "ipeds_unitid",
    "school_unitid",
    "institution_id",
)
_YEAR_ALIASES = (
    "year",
    "cycle_year",
    "survey_year",
    "award_year",
    "data_year",
)
_DOCTORAL_COUNT_ALIASES = (
    "doctoral_completions",
    "doctorate_completions",
    "completions_doctoral",
    "count_doctoral",
    "doctoral_awards",
    "awards_doctoral",
)
_TOTAL_COUNT_ALIASES = (
    "total_completions",
    "completions_total",
    "total_awards",
    "awards_total",
    "completion_count_total",
)
_AWARD_LEVEL_ALIASES = (
    "award_level",
    "awardlevel",
    "awlevel",
    "degree_level",
    "credential_level",
    "credlev",
)
_COUNT_ALIASES = (
    "count",
    "completions",
    "completion_count",
    "awards",
    "value",
)


@dataclass(slots=True)
class _SnapshotDensity:
    school_id: UUID
    school_name: str
    snapshot_count: int


def _normalise_key(value: str | None) -> str:
    text = str(value or "").strip().lower()
    return "_".join(part for part in text.replace("-", "_").replace(" ", "_").split("_") if part)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        out = float(value)
        if out != out:
            return None
        return out
    raw = str(value).strip().replace(",", "")
    if not raw:
        return None
    if raw.endswith("%"):
        raw = raw[:-1].strip()
    try:
        return float(raw)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    out = _to_float(value)
    if out is None:
        return None
    return int(round(out))


def _to_probability_rate(value: float | None) -> float | None:
    if value is None:
        return None
    out = float(value)
    if out > 1.0 and out <= 100.0:
        out = out / 100.0
    if out < 0.0 or out > 1.0:
        return None
    return round(out, 6)


def _percentile_rank(values: list[float]) -> dict[float, float]:
    clean = sorted(float(v) for v in values)
    if not clean:
        return {}
    if len(clean) == 1:
        return {clean[0]: 0.5}

    out: dict[float, float] = {}
    n = len(clean)
    idx = 0
    while idx < n:
        start = idx
        current = clean[idx]
        while idx < n and clean[idx] == current:
            idx += 1
        end = idx - 1
        mean_rank = (start + end) / 2.0
        out[current] = round(mean_rank / (n - 1), 6)
    return out


def _pick_alias(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    lowered = {_normalise_key(str(key)): value for key, value in row.items()}
    for alias in aliases:
        value = lowered.get(_normalise_key(alias))
        if value not in (None, ""):
            return value
    return None


def _is_doctoral_award_level(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    if any(token in text for token in ("doctor", "doctoral", "phd", "research", "professional practice")):
        return True
    numeric = _to_int(text)
    if numeric is None:
        return False
    return numeric in {17, 18, 19}


def _load_csv_rows(payload: bytes, *, filename: str | None = None) -> list[dict[str, Any]]:
    if filename and filename.lower().endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(payload)) as zf:
            members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            if not members:
                return []
            members.sort()
            with zf.open(members[0], "r") as fh:
                content = fh.read().decode("utf-8", errors="ignore")
                return list(csv.DictReader(io.StringIO(content)))
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
    download_dir: Path,
    filename_hint: str,
) -> tuple[bytes | None, str | None, str | None, str]:
    if local_path:
        path = Path(local_path)
        if path.exists():
            payload = path.read_bytes()
            return payload, None, str(path), path.name

    if url:
        try:
            payload, last_modified = await _download_bytes(url)
            suffix = Path(url).suffix or ".csv"
            download_dir.mkdir(parents=True, exist_ok=True)
            out_path = download_dir / f"{filename_hint}{suffix}"
            out_path.write_bytes(payload)
            return payload, url, str(out_path), last_modified or out_path.name
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("Download failed for %s: %s", url, exc)
    return None, None, None, "missing"


def _resolve_metric_year(
    *,
    school_year_metrics: dict[int, dict[str, float | None]],
    observed_year: int,
) -> tuple[int, dict[str, float]] | None:
    candidates: list[tuple[int, dict[str, float]]] = []
    for metric_year, payload in school_year_metrics.items():
        academic = payload.get("academic_outcome")
        career = payload.get("career_outcome")
        life = payload.get("life_satisfaction")
        phd = payload.get("phd_probability")
        if None in (academic, career, life, phd):
            continue
        candidates.append(
            (
                metric_year,
                {
                    "academic_outcome": float(academic),
                    "career_outcome": float(career),
                    "life_satisfaction": float(life),
                    "phd_probability": float(phd),
                },
            )
        )

    if not candidates:
        return None

    def _rank(item: tuple[int, dict[str, float]]) -> tuple[int, int, int]:
        metric_year = item[0]
        return (0 if metric_year <= observed_year else 1, abs(observed_year - metric_year), -metric_year)

    return sorted(candidates, key=_rank)[0]


async def _count_eligible_snapshots(
    session: AsyncSession,
    *,
    lookback_days: int,
    school_ids: list[UUID],
    school_year_metric_index: dict[UUID, dict[int, dict[str, float | None]]],
) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(lookback_days)))
    rows = (
        await session.execute(
            select(CausalFeatureSnapshot.school_id, CausalFeatureSnapshot.observed_at).where(
                and_(
                    CausalFeatureSnapshot.observed_at >= cutoff,
                    CausalFeatureSnapshot.school_id.in_(school_ids),
                )
            )
        )
    ).all()
    eligible = 0
    for school_id, observed_at in rows:
        by_year = school_year_metric_index.get(school_id) or {}
        resolved = _resolve_metric_year(
            school_year_metrics=by_year,
            observed_year=int(observed_at.year),
        )
        if resolved is not None:
            eligible += 1
    return eligible


async def _collect_coverage_with_session(
    session: AsyncSession,
    *,
    lookback_days: int,
) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(lookback_days)))
    snapshots = int(
        (
            await session.scalar(
                select(func.count())
                .select_from(CausalFeatureSnapshot)
                .where(CausalFeatureSnapshot.observed_at >= cutoff)
            )
        )
        or 0
    )
    rows = (
        (
            await session.execute(
                select(
                    CausalOutcomeEvent.outcome_name,
                    CausalOutcomeEvent.label_type,
                    CausalOutcomeEvent.label_confidence,
                ).where(CausalOutcomeEvent.observed_at >= cutoff)
            )
        )
        .all()
    )

    counts = {key: 0 for key in _PHASE4_COVERAGE_OUTCOMES}
    true_counts = {key: 0 for key in _PHASE4_COVERAGE_OUTCOMES}
    anchor_counts = {key: 0 for key in _PHASE4_COVERAGE_OUTCOMES}

    for outcome_name, label_type, label_confidence in rows:
        outcome = str(outcome_name)
        if outcome not in counts:
            continue
        counts[outcome] += 1
        if str(label_type).lower() == "true":
            true_counts[outcome] += 1
        if float(label_confidence or 0.0) >= 0.9:
            anchor_counts[outcome] += 1

    return {
        "snapshots": snapshots,
        "counts": counts,
        "true_counts": true_counts,
        "anchor_counts": anchor_counts,
    }


async def _load_snapshot_density(
    session: AsyncSession,
    *,
    cutoff: datetime,
    school_names: list[str] | None,
) -> list[_SnapshotDensity]:
    stmt = (
        select(
            CausalFeatureSnapshot.school_id,
            School.name,
            func.count().label("snapshot_count"),
        )
        .join(School, School.id == CausalFeatureSnapshot.school_id)
        .where(CausalFeatureSnapshot.observed_at >= cutoff)
        .group_by(CausalFeatureSnapshot.school_id, School.name)
        .order_by(desc("snapshot_count"), School.name.asc())
    )
    if school_names:
        stmt = stmt.where(func.lower(School.name).in_([name.lower() for name in school_names]))

    rows = (await session.execute(stmt)).all()
    out: list[_SnapshotDensity] = []
    for school_id, school_name, snapshot_count in rows:
        out.append(
            _SnapshotDensity(
                school_id=school_id,
                school_name=str(school_name),
                snapshot_count=int(snapshot_count or 0),
            )
        )
    return out


async def _load_school_year_metric_index(
    session: AsyncSession,
    *,
    school_ids: list[UUID],
) -> dict[UUID, dict[int, dict[str, float | None]]]:
    index: dict[UUID, dict[int, dict[str, float | None]]] = defaultdict(dict)

    # SchoolMetricsYear provides strong structured baseline for graduation_rate.
    metric_rows = (
        (
            await session.execute(
                select(SchoolMetricsYear).where(SchoolMetricsYear.school_id.in_(school_ids))
            )
        )
        .scalars()
        .all()
    )
    for row in metric_rows:
        school_year = index[row.school_id].setdefault(
            int(row.metric_year),
            {
                "academic_outcome": None,
                "career_raw": None,
                "career_outcome": None,
                "life_satisfaction": None,
                "phd_probability": None,
            },
        )
        school_year["academic_outcome"] = _to_probability_rate(_to_float(row.grad_rate))

    # Canonical facts provide retention/earnings/doctoral share and fallback graduation.
    canonical_rows = (
        (
            await session.execute(
                select(CanonicalFact)
                .where(
                    and_(
                        CanonicalFact.school_id.in_(school_ids),
                        CanonicalFact.outcome_name.in_(
                            [
                                "graduation_rate_4yr",
                                "retention_rate",
                                "median_earnings_10yr",
                                "doctoral_completions_share",
                            ]
                        ),
                    )
                )
                .order_by(
                    CanonicalFact.school_id.asc(),
                    CanonicalFact.cycle_year.asc(),
                    CanonicalFact.outcome_name.asc(),
                    CanonicalFact.confidence.desc(),
                    CanonicalFact.observed_at.desc(),
                )
            )
        )
        .scalars()
        .all()
    )

    seen_metric_key: set[tuple[UUID, int, str]] = set()
    for row in canonical_rows:
        outcome = normalise_variable_name(row.outcome_name)
        key = (row.school_id, int(row.cycle_year), outcome)
        if key in seen_metric_key:
            continue
        seen_metric_key.add(key)

        numeric = row.canonical_value_numeric
        if numeric is None:
            numeric = coerce_numeric(row.canonical_value_text, variable_name=outcome)
        school_year = index[row.school_id].setdefault(
            int(row.cycle_year),
            {
                "academic_outcome": None,
                "career_raw": None,
                "career_outcome": None,
                "life_satisfaction": None,
                "phd_probability": None,
            },
        )

        if outcome == "graduation_rate_4yr":
            school_year["academic_outcome"] = _to_probability_rate(_to_float(numeric))
        elif outcome == "retention_rate":
            school_year["life_satisfaction"] = _to_probability_rate(_to_float(numeric))
        elif outcome == "median_earnings_10yr":
            school_year["career_raw"] = _to_float(numeric)
        elif outcome == "doctoral_completions_share":
            school_year["phd_probability"] = _to_probability_rate(_to_float(numeric))

    # Metadata fallback from school.official_facts
    schools = (
        (await session.execute(select(School).where(School.id.in_(school_ids))))
        .scalars()
        .all()
    )
    for school in schools:
        metadata = dict(school.metadata_ or {})
        official = dict(metadata.get("official_facts") or {})
        fields = dict(official.get("fields") or {})
        for field_name, value in fields.items():
            field = normalise_variable_name(field_name)
            if field not in {
                "graduation_rate_4yr",
                "retention_rate",
                "median_earnings_10yr",
                "doctoral_completions_share",
            }:
                continue
            payload = dict(value or {}) if isinstance(value, dict) else {}
            cycle_year = _to_int(payload.get("cycle_year")) or _to_int(official.get("cycle_year"))
            if cycle_year is None:
                continue
            numeric = _to_float(payload.get("value_numeric"))
            if numeric is None:
                numeric = coerce_numeric(
                    str(payload.get("value_text") or ""),
                    variable_name=field,
                )
            if numeric is None:
                continue
            school_year = index[school.id].setdefault(
                int(cycle_year),
                {
                    "academic_outcome": None,
                    "career_raw": None,
                    "career_outcome": None,
                    "life_satisfaction": None,
                    "phd_probability": None,
                },
            )
            if field == "graduation_rate_4yr" and school_year.get("academic_outcome") is None:
                school_year["academic_outcome"] = _to_probability_rate(numeric)
            elif field == "retention_rate" and school_year.get("life_satisfaction") is None:
                school_year["life_satisfaction"] = _to_probability_rate(numeric)
            elif field == "median_earnings_10yr" and school_year.get("career_raw") is None:
                school_year["career_raw"] = _to_float(numeric)
            elif field == "doctoral_completions_share" and school_year.get("phd_probability") is None:
                school_year["phd_probability"] = _to_probability_rate(numeric)

    # Career outcome is percentile-normalized per year.
    earnings_by_year: dict[int, list[float]] = defaultdict(list)
    for by_year in index.values():
        for metric_year, metric_payload in by_year.items():
            raw = metric_payload.get("career_raw")
            if raw is not None:
                earnings_by_year[int(metric_year)].append(float(raw))

    percentile_map_by_year = {
        int(metric_year): _percentile_rank(values)
        for metric_year, values in earnings_by_year.items()
    }
    for by_year in index.values():
        for metric_year, metric_payload in by_year.items():
            raw = metric_payload.get("career_raw")
            if raw is None:
                continue
            pct_map = percentile_map_by_year.get(int(metric_year), {})
            metric_payload["career_outcome"] = pct_map.get(float(raw), 0.5)

    return index


async def _ingest_ipeds_completions_truth(
    session: AsyncSession,
    *,
    run_id: str,
    school_ids: list[UUID],
    cycle_year: int,
    output_dir: Path,
) -> dict[str, Any]:
    local_path = (settings.IPEDS_COMPLETIONS_DATASET_PATH or "").strip() or None
    url = (settings.IPEDS_COMPLETIONS_DATASET_URL or "").strip() or None
    if not local_path and Path(_DEFAULT_IPEDS_COMPLETIONS_PATH).exists():
        local_path = _DEFAULT_IPEDS_COMPLETIONS_PATH

    payload, source_url, file_path, version_hint = await _load_payload_from_path_or_url(
        local_path=local_path,
        url=url,
        download_dir=output_dir / "ipeds_completions",
        filename_hint="ipeds_completions",
    )
    if payload is None:
        return {
            "status": "missing",
            "rows_read": 0,
            "facts_upserted": 0,
            "source_url": source_url,
            "file_path": file_path,
            "source_version": version_hint,
        }

    rows = _load_csv_rows(payload, filename=file_path)
    if not rows:
        return {
            "status": "empty",
            "rows_read": 0,
            "facts_upserted": 0,
            "source_url": source_url,
            "file_path": file_path,
            "source_version": version_hint,
        }

    # Build school<->unitid map.
    school_to_unitid: dict[UUID, str] = {}
    external_rows = (
        (
            await session.execute(
                select(SchoolExternalId).where(
                    and_(
                        SchoolExternalId.school_id.in_(school_ids),
                        SchoolExternalId.provider.in_(["ipeds", "unitid", "ipeds_unitid"]),
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    for row in external_rows:
        external_id = str(row.external_id or "").strip()
        if external_id:
            school_to_unitid[row.school_id] = external_id

    source_map_rows = (
        (
            await session.execute(
                select(SourceEntityMap).where(
                    and_(
                        SourceEntityMap.school_id.in_(school_ids),
                        SourceEntityMap.source_name.in_(["ipeds_bulk", "ipeds_college_navigator"]),
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    for row in source_map_rows:
        external_id = str(row.external_id or "").strip()
        if external_id and row.school_id not in school_to_unitid:
            school_to_unitid[row.school_id] = external_id

    unitid_to_school = {unitid: school_id for school_id, unitid in school_to_unitid.items() if unitid}

    aggregated: dict[tuple[str, int], dict[str, float]] = defaultdict(lambda: {"doctoral": 0.0, "total": 0.0})
    for row in rows:
        unitid_raw = _pick_alias(row, _UNITID_ALIASES)
        unitid = str(unitid_raw or "").strip()
        if not unitid:
            continue
        year = _to_int(_pick_alias(row, _YEAR_ALIASES)) or int(cycle_year)

        direct_doctoral = _to_float(_pick_alias(row, _DOCTORAL_COUNT_ALIASES))
        direct_total = _to_float(_pick_alias(row, _TOTAL_COUNT_ALIASES))
        key = (unitid, int(year))

        if direct_doctoral is not None or direct_total is not None:
            aggregated[key]["doctoral"] += max(0.0, float(direct_doctoral or 0.0))
            aggregated[key]["total"] += max(
                float(direct_total or 0.0),
                float(direct_doctoral or 0.0),
            )
            continue

        level = _pick_alias(row, _AWARD_LEVEL_ALIASES)
        count = _to_float(_pick_alias(row, _COUNT_ALIASES))
        if count is None:
            continue
        count = max(0.0, float(count))
        aggregated[key]["total"] += count
        if _is_doctoral_award_level(level):
            aggregated[key]["doctoral"] += count

    if not aggregated:
        return {
            "status": "parsed_no_doctoral_fields",
            "rows_read": len(rows),
            "facts_upserted": 0,
            "source_url": source_url,
            "file_path": file_path,
            "source_version": version_hint,
        }

    now = datetime.now(timezone.utc)
    upserted = 0
    for (unitid, year), counts in aggregated.items():
        school_id = unitid_to_school.get(unitid)
        if school_id is None:
            continue
        total = float(counts.get("total") or 0.0)
        doctoral = float(counts.get("doctoral") or 0.0)
        if total <= 0.0:
            continue
        share = max(0.0, min(1.0, doctoral / total))

        existing = await session.scalar(
            select(CanonicalFact).where(
                and_(
                    CanonicalFact.student_id.is_(None),
                    CanonicalFact.school_id == school_id,
                    CanonicalFact.cycle_year == int(year),
                    CanonicalFact.outcome_name == "doctoral_completions_share",
                    CanonicalFact.source_family == "ipeds_completions",
                )
            )
        )

        value_text = f"{share:.6f}"
        value_bucket = fingerprint_value(value_text=value_text, value_numeric=share)
        metadata = {
            "run_id": run_id,
            "source_url": source_url,
            "file_path": file_path,
            "source_version": version_hint,
            "unitid": unitid,
            "doctoral_count": round(doctoral, 3),
            "total_completions": round(total, 3),
        }

        if existing is None:
            session.add(
                CanonicalFact(
                    student_id=None,
                    school_id=school_id,
                    cycle_year=int(year),
                    outcome_name="doctoral_completions_share",
                    canonical_value_text=value_text,
                    canonical_value_numeric=share,
                    canonical_value_bucket=value_bucket,
                    source_family="ipeds_completions",
                    confidence=0.99,
                    observed_at=now,
                    metadata_=metadata,
                )
            )
            upserted += 1
            continue

        existing.canonical_value_text = value_text
        existing.canonical_value_numeric = share
        existing.canonical_value_bucket = value_bucket
        existing.confidence = max(float(existing.confidence or 0.0), 0.99)
        existing.observed_at = now
        existing.metadata_ = {**(existing.metadata_ or {}), **metadata}
        upserted += 1

    await session.flush()
    return {
        "status": "ok",
        "rows_read": len(rows),
        "facts_upserted": upserted,
        "source_url": source_url,
        "file_path": file_path,
        "source_version": version_hint,
    }


async def materialize_non_admission_true_labels(
    session: AsyncSession,
    *,
    run_id: str,
    lookback_days: int,
    school_ids: list[UUID],
    school_year_metric_index: dict[UUID, dict[int, dict[str, float | None]]],
) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(lookback_days)))
    snapshot_rows = (
        (
            await session.execute(
                select(CausalFeatureSnapshot)
                .where(
                    and_(
                        CausalFeatureSnapshot.observed_at >= cutoff,
                        CausalFeatureSnapshot.school_id.in_(school_ids),
                    )
                )
                .order_by(CausalFeatureSnapshot.observed_at.asc())
            )
        )
        .scalars()
        .all()
    )

    existing_rows = (
        (
            await session.execute(
                select(CausalOutcomeEvent).where(
                    and_(
                        CausalOutcomeEvent.source == PHASE4_OUTCOME_SOURCE,
                        CausalOutcomeEvent.school_id.in_(school_ids),
                        CausalOutcomeEvent.outcome_name.in_(_NON_ADMISSION_OUTCOMES),
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    existing_keys: set[tuple[str, str, str, int, str]] = set()
    for row in existing_rows:
        meta = dict(row.metadata_ or {})
        source_key = str(meta.get("source_key") or "")
        metric_year = _to_int(meta.get("metric_year")) or row.observed_at.year
        existing_keys.add(
            (
                str(row.student_id),
                str(row.school_id),
                str(row.outcome_name),
                int(metric_year),
                source_key,
            )
        )

    created = 0
    deduped = 0
    eligible_snapshots = 0
    skipped_missing_metrics = 0
    missing_by_school: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "sample_years": []})

    for snapshot in snapshot_rows:
        by_year = school_year_metric_index.get(snapshot.school_id) or {}
        resolved = _resolve_metric_year(
            school_year_metrics=by_year,
            observed_year=int(snapshot.observed_at.year),
        )
        if resolved is None:
            skipped_missing_metrics += 1
            bucket = missing_by_school[str(snapshot.school_id)]
            bucket["count"] = int(bucket.get("count") or 0) + 1
            sample_years = list(bucket.get("sample_years") or [])
            if len(sample_years) < 5:
                sample_years.append(int(snapshot.observed_at.year))
            bucket["sample_years"] = sample_years
            continue

        metric_year, outcome_values = resolved
        eligible_snapshots += 1
        for outcome_name in _NON_ADMISSION_OUTCOMES:
            value = float(outcome_values[outcome_name])
            source_key = f"{PHASE4_OUTCOME_SOURCE}:{snapshot.id}:{metric_year}:{outcome_name}"
            key = (
                str(snapshot.student_id),
                str(snapshot.school_id),
                outcome_name,
                int(metric_year),
                source_key,
            )
            if key in existing_keys:
                deduped += 1
                continue
            existing_keys.add(key)
            session.add(
                CausalOutcomeEvent(
                    student_id=snapshot.student_id,
                    school_id=snapshot.school_id,
                    offer_id=snapshot.offer_id,
                    outcome_name=outcome_name,
                    outcome_value=max(0.0, min(1.0, value)),
                    label_type="true",
                    label_confidence=0.99,
                    source=PHASE4_OUTCOME_SOURCE,
                    observed_at=snapshot.observed_at,
                    metadata_={
                        "run_id": run_id,
                        "source_key": source_key,
                        "metric_year": int(metric_year),
                        "snapshot_id": str(snapshot.id),
                    },
                )
            )
            created += 1

    await session.flush()
    return {
        "created": created,
        "deduped": deduped,
        "eligible_snapshots": eligible_snapshots,
        "processed_snapshots": len(snapshot_rows),
        "skipped_missing_metrics": skipped_missing_metrics,
        "missing_by_school": dict(missing_by_school),
    }


async def run_phase4_training_prep(
    session: AsyncSession,
    *,
    run_id: str,
    output_dir: str,
    lookback_days: int = 540,
    target_eligible_snapshots: int = 3500,
    school_names: list[str] | None = None,
    cycle_year: int | None = None,
    ingest_official_facts_enabled: bool = True,
    ingest_ipeds_completions_enabled: bool = True,
    school_concurrency_initial: int = 6,
    school_concurrency_max: int = 20,
    target_rpm_total: float = 180.0,
    rpm_band_low: float = 170.0,
    rpm_band_high: float = 185.0,
    max_auto_schools: int = 500,
    llm: LLMClient | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max(1, int(lookback_days)))
    effective_cycle_year = int(cycle_year or now.year)

    run_root = Path(output_dir).expanduser().resolve() / run_id
    run_root.mkdir(parents=True, exist_ok=True)

    densities = await _load_snapshot_density(
        session,
        cutoff=cutoff,
        school_names=school_names,
    )
    ordered = sorted(densities, key=lambda item: (-item.snapshot_count, item.school_name.lower()))

    selected: list[_SnapshotDensity] = []
    selected_raw_snapshots = 0
    if school_names:
        selected = ordered
        selected_raw_snapshots = sum(int(item.snapshot_count) for item in selected)
    else:
        for item in ordered:
            if len(selected) >= max(1, int(max_auto_schools)):
                break
            selected.append(item)
            selected_raw_snapshots += int(item.snapshot_count)
            if selected_raw_snapshots >= max(1, int(target_eligible_snapshots)):
                break

    selected_school_ids = [item.school_id for item in selected]
    selected_school_names = [item.school_name for item in selected]

    official_ingest_runs: list[dict[str, Any]] = []
    if ingest_official_facts_enabled and selected_school_names:
        official_result = await ingest_official_facts(
            session,
            school_names=selected_school_names,
            cycle_year=effective_cycle_year,
            run_id=f"{run_id}:phase4-official",
            fields=PHASE4_REQUIRED_FACT_FIELDS,
            llm=llm,
            school_concurrency_initial=school_concurrency_initial,
            school_concurrency_max=school_concurrency_max,
            target_rpm_total=target_rpm_total,
            rpm_band_low=rpm_band_low,
            rpm_band_high=rpm_band_high,
        )
        official_ingest_runs.append(official_result)

    completions_result = {
        "status": "skipped",
        "rows_read": 0,
        "facts_upserted": 0,
    }
    if ingest_ipeds_completions_enabled and selected_school_ids:
        completions_result = await _ingest_ipeds_completions_truth(
            session,
            run_id=f"{run_id}:ipeds-completions",
            school_ids=selected_school_ids,
            cycle_year=effective_cycle_year,
            output_dir=run_root,
        )

    metric_index = await _load_school_year_metric_index(
        session,
        school_ids=selected_school_ids,
    )

    # Expand school scope by density when eligible snapshots are below target.
    expandable = [item for item in ordered if item.school_id not in {row.school_id for row in selected}]
    scope_expansions = 0
    while not school_names and expandable:
        eligible_count = await _count_eligible_snapshots(
            session=session,
            lookback_days=lookback_days,
            school_ids=selected_school_ids,
            school_year_metric_index=metric_index,
        )
        if eligible_count >= int(target_eligible_snapshots):
            break
        if len(selected) >= max(1, int(max_auto_schools)):
            break

        add_count = min(25, len(expandable), max(1, int(max_auto_schools) - len(selected)))
        if add_count <= 0:
            break
        appended = expandable[:add_count]
        expandable = expandable[add_count:]
        selected.extend(appended)
        selected_school_ids = [item.school_id for item in selected]
        selected_school_names = [item.school_name for item in selected]
        selected_raw_snapshots += sum(int(item.snapshot_count) for item in appended)
        scope_expansions += 1

        if ingest_official_facts_enabled:
            official_result = await ingest_official_facts(
                session,
                school_names=[item.school_name for item in appended],
                cycle_year=effective_cycle_year,
                run_id=f"{run_id}:phase4-official-expand-{scope_expansions}",
                fields=PHASE4_REQUIRED_FACT_FIELDS,
                llm=llm,
                school_concurrency_initial=school_concurrency_initial,
                school_concurrency_max=school_concurrency_max,
                target_rpm_total=target_rpm_total,
                rpm_band_low=rpm_band_low,
                rpm_band_high=rpm_band_high,
            )
            official_ingest_runs.append(official_result)

        if ingest_ipeds_completions_enabled:
            completions_result = await _ingest_ipeds_completions_truth(
                session,
                run_id=f"{run_id}:ipeds-completions-expand-{scope_expansions}",
                school_ids=selected_school_ids,
                cycle_year=effective_cycle_year,
                output_dir=run_root,
            )

        metric_index = await _load_school_year_metric_index(
            session,
            school_ids=selected_school_ids,
        )

    before_non_true = int(
        (
            await session.scalar(
                select(func.count()).select_from(CausalOutcomeEvent).where(
                    and_(
                        CausalOutcomeEvent.outcome_name.in_(_NON_ADMISSION_OUTCOMES),
                        CausalOutcomeEvent.label_type != "true",
                    )
                )
            )
        )
        or 0
    )

    materialized = await materialize_non_admission_true_labels(
        session,
        run_id=run_id,
        lookback_days=lookback_days,
        school_ids=selected_school_ids,
        school_year_metric_index=metric_index,
    )

    after_non_true = int(
        (
            await session.scalar(
                select(func.count()).select_from(CausalOutcomeEvent).where(
                    and_(
                        CausalOutcomeEvent.outcome_name.in_(_NON_ADMISSION_OUTCOMES),
                        CausalOutcomeEvent.label_type != "true",
                    )
                )
            )
        )
        or 0
    )

    coverage = await _collect_coverage_with_session(
        session,
        lookback_days=lookback_days,
    )
    stage1_gate_passed, stage1_gate_reasons = _check_stage_data_gate(1, coverage)

    non_admission_counts = (
        (
            await session.execute(
                select(
                    CausalOutcomeEvent.outcome_name,
                    func.count().label("count"),
                )
                .where(CausalOutcomeEvent.outcome_name.in_(_NON_ADMISSION_OUTCOMES))
                .group_by(CausalOutcomeEvent.outcome_name)
            )
        )
        .all()
    )

    counts_by_outcome = {str(name): int(count or 0) for name, count in non_admission_counts}
    for outcome_name in _NON_ADMISSION_OUTCOMES:
        counts_by_outcome.setdefault(outcome_name, 0)

    quality_gate_reasons: list[str] = []
    if int(materialized.get("eligible_snapshots") or 0) < int(target_eligible_snapshots):
        quality_gate_reasons.append("eligible_snapshots_lt_target")
    if int(after_non_true) != int(before_non_true):
        quality_gate_reasons.append("non_true_non_admission_labels_changed")

    payload = {
        "status": "ok" if stage1_gate_passed and not quality_gate_reasons else "watch",
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "lookback_days": int(lookback_days),
            "target_eligible_snapshots": int(target_eligible_snapshots),
            "cycle_year": effective_cycle_year,
            "ingest_official_facts_enabled": bool(ingest_official_facts_enabled),
            "ingest_ipeds_completions_enabled": bool(ingest_ipeds_completions_enabled),
            "school_concurrency_initial": int(school_concurrency_initial),
            "school_concurrency_max": int(school_concurrency_max),
            "target_rpm_total": float(target_rpm_total),
            "rpm_band_low": float(rpm_band_low),
            "rpm_band_high": float(rpm_band_high),
            "max_auto_schools": int(max_auto_schools),
        },
        "school_scope": {
            "selected_schools_count": len(selected_school_ids),
            "selected_school_names": selected_school_names,
            "selected_snapshots_raw": int(selected_raw_snapshots),
            "scope_expansions": scope_expansions,
            "density_top10": [
                {
                    "school_name": item.school_name,
                    "snapshot_count": int(item.snapshot_count),
                }
                for item in ordered[:10]
            ],
        },
        "official_ingest_runs": official_ingest_runs,
        "ipeds_completions_ingest": completions_result,
        "materialization": materialized,
        "non_admission_counts_by_outcome": counts_by_outcome,
        "strict_true_only": {
            "before_non_true_non_admission": before_non_true,
            "after_non_true_non_admission": after_non_true,
            "passed": int(after_non_true) == int(before_non_true),
        },
        "stage1_readiness": {
            "passed": bool(stage1_gate_passed),
            "reasons": stage1_gate_reasons,
            "coverage": coverage,
        },
        "quality_gate": {
            "passed": len(quality_gate_reasons) == 0,
            "reasons": quality_gate_reasons,
        },
        "next_command": (
            "python -m scholarpath.scripts.causal_staged_train "
            "--stage 1 --max-rpm-total 180 --judge-concurrency 2 --train-candidates-per-stage 3"
        ),
    }
    return payload
