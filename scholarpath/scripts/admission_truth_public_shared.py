"""Shared helpers for public admission truth ingestion pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.data.ranked_school_allowlist import (
    RANKED_SCHOOL_ALLOWLIST,
    RANKED_SCHOOL_ALLOWLIST_VERSION,
)
from scholarpath.db.models import CausalFeatureSnapshot, School
from scholarpath.services.admission_data_phase4_service import (
    _count_eligible_snapshots,
    _load_school_year_metric_index,
)


# High-frequency aliases observed in Reddit/CC public intake.
_SCHOOL_ALIAS_MAP: dict[str, str] = {
    "mit": "Massachusetts Institute of Technology",
    "umich": "University of Michigan, Ann Arbor",
    "u mich": "University of Michigan, Ann Arbor",
    "umich ann arbor": "University of Michigan, Ann Arbor",
    "umich lsa": "University of Michigan, Ann Arbor",
    "umich ross": "University of Michigan, Ann Arbor",
    "uva": "University of Virginia",
    "uva oos": "University of Virginia",
    "unc": "University of North Carolina at Chapel Hill",
    "chapel hill": "University of North Carolina at Chapel Hill",
    "unc chapel hill": "University of North Carolina at Chapel Hill",
    "unc-chapel hill": "University of North Carolina at Chapel Hill",
    "gt": "Georgia Institute of Technology",
    "gatech": "Georgia Institute of Technology",
    "georgia tech": "Georgia Institute of Technology",
    "ucb": "University of California, Berkeley",
    "uc berkeley": "University of California, Berkeley",
    "berkeley": "University of California, Berkeley",
    "uw": "University of Washington",
    "uw seattle": "University of Washington",
    "uw madison": "University of Wisconsin-Madison",
    "uiuc": "University of Illinois Urbana-Champaign",
    "ucsd": "University of California, San Diego",
    "uci": "University of California, Irvine",
    "ucd": "University of California, Davis",
    "ucla": "University of California, Los Angeles",
    "ucsb": "University of California, Santa Barbara",
    "ucsc": "University of California, Santa Cruz",
    "sjsu": "San Jose State University",
    "sdsu": "San Diego State University",
    "cal poly slo": "California Polytechnic State University-San Luis Obispo",
    "cal poly": "California Polytechnic State University-San Luis Obispo",
    "penn state": "Pennsylvania State University",
    "umd": "University of Maryland, College Park",
    "umd cp": "University of Maryland, College Park",
    "college park": "University of Maryland, College Park",
    "nyu": "New York University",
    "bu": "Boston University",
    "boston u": "Boston University",
    "wash u": "Washington University in St. Louis",
}


@dataclass(slots=True)
class MetricsSchoolContext:
    covered_school_ids: set[str]
    covered_name_index: dict[str, str]
    all_name_index: dict[str, str]


@dataclass(slots=True)
class RankedSchoolAllowlistContext:
    version: str
    allowed_keys: set[str]
    alias_map: dict[str, str]
    rank_bucket_by_key: dict[str, str]
    state_by_key: dict[str, str]


def normalise_school_key(name: str) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(name or "").lower())
    return re.sub(r"\s+", " ", text).strip()


def load_ranked_school_allowlist(
    *,
    version: str | None = None,
) -> RankedSchoolAllowlistContext:
    requested = str(version or RANKED_SCHOOL_ALLOWLIST_VERSION).strip()
    if requested != RANKED_SCHOOL_ALLOWLIST_VERSION:
        raise ValueError(
            f"Unsupported ranked allowlist version: {requested}. "
            f"Supported version: {RANKED_SCHOOL_ALLOWLIST_VERSION}."
        )

    allowed_keys: set[str] = set()
    alias_map: dict[str, str] = {}
    rank_bucket_by_key: dict[str, str] = {}
    state_by_key: dict[str, str] = {}

    for row in RANKED_SCHOOL_ALLOWLIST:
        canonical = str(row.get("canonical_name") or "").strip()
        if not canonical:
            continue
        canonical_key = normalise_school_key(canonical)
        if not canonical_key:
            continue
        allowed_keys.add(canonical_key)
        rank_bucket_by_key[canonical_key] = str(row.get("rank_bucket") or "").strip()
        state_by_key[canonical_key] = str(row.get("state") or "").strip()

        aliases = row.get("aliases") or []
        for alias in aliases:
            alias_key = normalise_school_key(str(alias))
            if not alias_key:
                continue
            alias_map.setdefault(alias_key, canonical)

    return RankedSchoolAllowlistContext(
        version=RANKED_SCHOOL_ALLOWLIST_VERSION,
        allowed_keys=allowed_keys,
        alias_map=alias_map,
        rank_bucket_by_key=rank_bucket_by_key,
        state_by_key=state_by_key,
    )


def resolve_school_alias(
    school_name: str,
    *,
    extra_alias_map: dict[str, str] | None = None,
) -> tuple[str, bool]:
    raw = str(school_name or "").strip()
    if not raw:
        return "", False
    key = normalise_school_key(raw)
    mapped = _SCHOOL_ALIAS_MAP.get(key)
    if not mapped and extra_alias_map:
        mapped = extra_alias_map.get(key)
    if not mapped:
        return raw, False
    changed = normalise_school_key(mapped) != normalise_school_key(raw)
    return mapped, changed


async def load_metrics_school_context(session: AsyncSession) -> MetricsSchoolContext:
    school_rows = list((await session.execute(select(School.id, School.name))).all())
    if not school_rows:
        return MetricsSchoolContext(covered_school_ids=set(), covered_name_index={}, all_name_index={})

    all_name_index: dict[str, str] = {}
    school_id_by_key: dict[str, str] = {}
    school_ids: list[UUID] = []
    for school_id, school_name in school_rows:
        school_ids.append(school_id)
        key = normalise_school_key(str(school_name or ""))
        if key and key not in all_name_index:
            all_name_index[key] = str(school_name)
            school_id_by_key[key] = str(school_id)

    metric_index = await _load_school_year_metric_index(
        session,
        school_ids=school_ids,
    )
    covered_school_ids: set[str] = set()
    for school_id in school_ids:
        by_year = metric_index.get(school_id) or {}
        has_complete_year = False
        for payload in by_year.values():
            if (
                payload.get("academic_outcome") is not None
                and payload.get("career_outcome") is not None
                and payload.get("life_satisfaction") is not None
                and payload.get("phd_probability") is not None
            ):
                has_complete_year = True
                break
        if has_complete_year:
            covered_school_ids.add(str(school_id))

    covered_name_index: dict[str, str] = {}
    for key, school_name in all_name_index.items():
        school_id = school_id_by_key.get(key)
        if school_id and school_id in covered_school_ids:
            covered_name_index[key] = school_name

    return MetricsSchoolContext(
        covered_school_ids=covered_school_ids,
        covered_name_index=covered_name_index,
        all_name_index=all_name_index,
    )


def resolve_school_name_for_ingest(
    school_name: str,
    *,
    context: MetricsSchoolContext | None = None,
    extra_alias_map: dict[str, str] | None = None,
) -> tuple[str, bool]:
    alias_name, alias_changed = resolve_school_alias(
        school_name,
        extra_alias_map=extra_alias_map,
    )
    key = normalise_school_key(alias_name)

    if context is not None:
        canonical = context.covered_name_index.get(key) or context.all_name_index.get(key)
        if canonical:
            changed = alias_changed or (normalise_school_key(canonical) != normalise_school_key(school_name))
            return canonical, changed
    return alias_name, alias_changed


async def estimate_eligible_snapshots(
    session: AsyncSession,
    *,
    lookback_days: int = 540,
) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(lookback_days)))
    school_ids = list(
        (
            await session.execute(
                select(CausalFeatureSnapshot.school_id)
                .where(CausalFeatureSnapshot.observed_at >= cutoff)
                .distinct()
            )
        )
        .scalars()
        .all()
    )
    if not school_ids:
        return 0
    metric_index = await _load_school_year_metric_index(
        session,
        school_ids=school_ids,
    )
    return int(
        await _count_eligible_snapshots(
            session,
            lookback_days=lookback_days,
            school_ids=school_ids,
            school_year_metric_index=metric_index,
        )
    )
