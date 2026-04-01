"""Database-first coverage loader for DeepSearch V2."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from scholarpath.db.models import DataPoint, School
from scholarpath.search.canonical_merge import (
    CanonicalMergeService,
    normalise_numeric,
    normalise_variable_name,
)
from scholarpath.search.sources.base import SearchResult

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class SchoolCoverageSnapshot:
    """Current DB coverage state for a target school."""

    target_school: str
    resolved_school_name: str | None = None
    school_id: str | None = None
    existing_results: list[SearchResult] = field(default_factory=list)
    covered_fields: set[str] = field(default_factory=set)
    missing_fields: list[str] = field(default_factory=list)


class DBCoverageLoader:
    """Load existing recent facts for target schools before external search."""

    def __init__(self, merger: CanonicalMergeService) -> None:
        self._merger = merger

    async def load(
        self,
        *,
        target_schools: list[str],
        required_fields: list[str],
        freshness_days: int,
    ) -> dict[str, SchoolCoverageSnapshot]:
        snapshots = {
            school: SchoolCoverageSnapshot(target_school=school)
            for school in target_schools
        }
        if not target_schools:
            return snapshots

        from scholarpath.db.session import async_session_factory

        cutoff = datetime.now(timezone.utc) - timedelta(days=max(freshness_days, 0))
        required = {normalise_variable_name(field) for field in required_fields}

        async with async_session_factory() as session:
            resolved = await self._resolve_schools(session, target_schools)

            school_ids: list = []
            school_id_to_targets: dict[str, list[str]] = {}
            for target, school in resolved.items():
                if school is None:
                    continue
                snapshots[target].resolved_school_name = school.name
                snapshots[target].school_id = str(school.id)
                sid = str(school.id)
                school_ids.append(school.id)
                school_id_to_targets.setdefault(sid, []).append(target)

            if school_ids:
                stmt = (
                    select(DataPoint)
                    .where(DataPoint.school_id.in_(school_ids))
                    .where(DataPoint.crawled_at >= cutoff)
                )
                rows = (await session.execute(stmt)).scalars().all()
                for row in rows:
                    sid = str(row.school_id) if row.school_id else None
                    if sid is None:
                        continue
                    targets = school_id_to_targets.get(sid, [])
                    if not targets:
                        continue
                    canonical_var = normalise_variable_name(row.variable_name)
                    numeric = normalise_numeric(
                        row.value_numeric,
                        variable_name=canonical_var,
                        value_text=row.value_text,
                    )
                    for target in targets:
                        result = SearchResult(
                            source_name=row.source_name,
                            source_type=row.source_type,
                            source_url=row.source_url or "",
                            variable_name=canonical_var,
                            value_text=row.value_text,
                            value_numeric=numeric,
                            confidence=row.confidence,
                            sample_size=row.sample_size,
                            temporal_range=row.temporal_range,
                            raw_data={
                                "from_db": True,
                                "school_id": sid,
                                "queried_school": target,
                                "canonical_name": snapshots[target].resolved_school_name
                                or target,
                                "crawled_at": row.crawled_at.isoformat()
                                if row.crawled_at
                                else None,
                            },
                        )
                        snapshots[target].existing_results.append(result)

            for target, snapshot in snapshots.items():
                merged = self._merger.merge(snapshot.existing_results)
                snapshot.existing_results = merged
                covered = {
                    normalise_variable_name(item.variable_name)
                    for item in merged
                    if normalise_variable_name(item.variable_name) in required
                }
                snapshot.covered_fields = covered
                snapshot.missing_fields = sorted(required - covered)

        return snapshots

    async def _resolve_schools(
        self,
        session: "AsyncSession",
        target_schools: list[str],
    ) -> dict[str, School | None]:
        resolved: dict[str, School | None] = {school: None for school in target_schools}
        target_lower = {school.lower(): school for school in target_schools}

        stmt = select(School).where(func.lower(School.name).in_(list(target_lower.keys())))
        exact_rows = (await session.execute(stmt)).scalars().all()
        for school in exact_rows:
            key = school.name.lower()
            target = target_lower.get(key)
            if target:
                resolved[target] = school

        for target in target_schools:
            if resolved[target] is not None:
                continue
            fuzzy_stmt = (
                select(School)
                .where(School.name.ilike(f"%{target}%"))
                .order_by(School.us_news_rank.asc().nullslast())
                .limit(1)
            )
            fuzzy = (await session.execute(fuzzy_stmt)).scalars().first()
            resolved[target] = fuzzy

        return resolved
