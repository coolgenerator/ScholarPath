"""Causal real-data ingestion, cleaning, canonicalization, and dataset registry."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import uuid
from typing import Any

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.config import settings
from scholarpath.db.models import (
    AdmissionEvent,
    CanonicalFact,
    CausalDatasetVersion,
    CausalOutcomeEvent,
    CausalTrendSignal,
    EvidenceArtifact,
    FactLineage,
    FactQuarantine,
    School,
    SchoolExternalId,
    Student,
)
from scholarpath.llm import LLMClient, get_llm_client
from scholarpath.search.canonical_merge import (
    coerce_numeric,
    fingerprint_value,
    normalise_numeric,
    normalise_variable_name,
)
from scholarpath.search.sources.cds_parser import CommonDataSetSource
from scholarpath.search.sources.college_scorecard import CollegeScorecardSource
from scholarpath.search.sources.base import SearchResult
from scholarpath.search.sources.ipeds_college_navigator import IPEDSCollegeNavigatorSource
from scholarpath.search.official_direct_fetch import (
    fetch_common_dataset_direct,
    fetch_school_official_profile_direct,
)
from scholarpath.search.sources.school_official_profile import SchoolOfficialProfileSource
from scholarpath.search.trends.common_app import CommonAppTrendSource

logger = logging.getLogger(__name__)

_ADMISSION_STAGE_TO_VALUE: dict[str, float | None] = {
    "submitted": None,
    "interview": None,
    "waitlist": None,
    "deferred": None,
    "admit": 1.0,
    "reject": 0.0,
    "declined": 0.0,
    "commit": 1.0,
}

_MINI_GATE_THRESHOLDS = {
    "schema_valid_rate": 0.99,
    "extraction_success": 0.95,
    "unresolved_conflict_rate": 0.03,
    "quarantine_rate": 0.10,
    "rpm_actual_avg": 180.0,
    "rate_limit_error_count": 0,
    "evidence_coverage_rate": 0.0,
}

_DEFAULT_FACT_FIELDS = [
    "acceptance_rate",
    "applicants_total",
    "admitted_total",
    "enrolled_total",
    "yield_rate",
    "sat_math_mid",
    "sat_reading_mid",
    "sat_25",
    "sat_75",
    "act_25",
    "act_75",
    "tuition_out_of_state",
    "avg_net_price",
    "graduation_rate_4yr",
    "student_faculty_ratio",
    "enrollment",
    "endowment_total",
    "endowment_per_student",
    "city",
    "state",
]

_DEFAULT_ACTIVE_OUTCOMES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]

_MIN_EXTERNAL_ID_CONFIDENCE = 0.75


@dataclass(slots=True)
class IngestMetrics:
    run_id: str
    processed_schools: int = 0
    raw_facts: int = 0
    schema_valid_count: int = 0
    extracted_count: int = 0
    kept_count: int = 0
    quarantined_count: int = 0
    conflicts_count: int = 0
    deduped_count: int = 0
    llm_calls_extract: int = 0
    llm_calls_judge: int = 0
    llm_errors: int = 0
    schools_with_any_fact: int = 0
    schools_with_official_fact: int = 0
    external_id_match_success: int = 0
    external_id_match_total: int = 0
    trend_signals_written: int = 0
    trend_signals_attempted: int = 0

    def to_dict(self) -> dict[str, Any]:
        schema_valid_rate = (
            self.schema_valid_count / self.raw_facts if self.raw_facts else 0.0
        )
        extraction_success = (
            self.extracted_count / self.raw_facts if self.raw_facts else 0.0
        )
        quarantine_rate = (
            self.quarantined_count / self.raw_facts if self.raw_facts else 0.0
        )
        unresolved_conflict_rate = (
            self.conflicts_count / max(1, self.kept_count)
        )
        official_source_coverage_rate = (
            self.schools_with_official_fact / max(1, self.processed_schools)
        )
        external_id_match_rate = (
            self.external_id_match_success / max(1, self.external_id_match_total)
        )
        trend_coverage_rate = (
            self.trend_signals_written / max(1, self.trend_signals_attempted)
        )
        return {
            "run_id": self.run_id,
            "processed_schools": self.processed_schools,
            "raw_facts": self.raw_facts,
            "schema_valid_count": self.schema_valid_count,
            "extracted_count": self.extracted_count,
            "kept_count": self.kept_count,
            "quarantined_count": self.quarantined_count,
            "conflicts_count": self.conflicts_count,
            "deduped_count": self.deduped_count,
            "schema_valid_rate": round(schema_valid_rate, 4),
            "extraction_success": round(extraction_success, 4),
            "quarantine_rate": round(quarantine_rate, 4),
            "unresolved_conflict_rate": round(unresolved_conflict_rate, 4),
            "llm_calls_extract": self.llm_calls_extract,
            "llm_calls_judge": self.llm_calls_judge,
            "llm_errors": self.llm_errors,
            "official_source_coverage_rate": round(official_source_coverage_rate, 4),
            "external_id_match_rate": round(external_id_match_rate, 4),
            "trend_coverage_rate": round(trend_coverage_rate, 4),
        }


async def register_evidence_artifact(
    session: AsyncSession,
    *,
    student_id: str | None,
    school_id: str | None,
    cycle_year: int | None,
    source_name: str,
    source_type: str,
    source_url: str | None,
    content_text: str | None,
    metadata: dict[str, Any] | None = None,
) -> EvidenceArtifact:
    digest = None
    if content_text:
        digest = hashlib.sha256(content_text.encode("utf-8")).hexdigest()
    student_uuid = _as_uuid(student_id)
    school_uuid = _as_uuid(school_id)

    if digest:
        existing = await session.scalar(
            select(EvidenceArtifact).where(
                and_(
                    EvidenceArtifact.student_id.is_(student_uuid)
                    if student_uuid is None
                    else EvidenceArtifact.student_id == student_uuid,
                    EvidenceArtifact.school_id.is_(school_uuid)
                    if school_uuid is None
                    else EvidenceArtifact.school_id == school_uuid,
                    EvidenceArtifact.cycle_year.is_(cycle_year)
                    if cycle_year is None
                    else EvidenceArtifact.cycle_year == cycle_year,
                    EvidenceArtifact.source_name == source_name.strip(),
                    EvidenceArtifact.source_type == (source_type.strip() or "user_upload"),
                    EvidenceArtifact.source_url.is_(source_url)
                    if source_url is None
                    else EvidenceArtifact.source_url == source_url,
                    EvidenceArtifact.source_hash == digest,
                )
            )
        )
        if existing is not None:
            return existing

    row = EvidenceArtifact(
        student_id=student_uuid,
        school_id=school_uuid,
        cycle_year=cycle_year,
        source_name=source_name.strip(),
        source_type=source_type.strip() or "user_upload",
        source_url=source_url,
        source_hash=digest,
        redaction_status="pending",
        metadata_=metadata or {},
    )
    session.add(row)
    await session.flush()
    return row


async def register_admission_event(
    session: AsyncSession,
    *,
    student_id: str,
    school_id: str,
    cycle_year: int,
    stage: str,
    major_bucket: str | None = None,
    happened_at: datetime | None = None,
    evidence_ref: str | None = None,
    source_name: str = "manual",
    metadata: dict[str, Any] | None = None,
) -> AdmissionEvent:
    student_uuid = _as_uuid(student_id)
    school_uuid = _as_uuid(school_id)
    if student_uuid is None:
        raise ValueError("student_id is required")
    if school_uuid is None:
        raise ValueError("school_id is required")

    student = await session.get(Student, student_uuid)
    if student is None:
        raise ValueError(f"Student {student_id} not found")
    school = await session.get(School, school_uuid)
    if school is None:
        raise ValueError(f"School {school_id} not found")

    stage_norm = str(stage).strip().lower()
    if stage_norm not in _ADMISSION_STAGE_TO_VALUE:
        raise ValueError(f"Unsupported admission stage: {stage}")

    source_key = None
    if metadata and isinstance(metadata, dict):
        source_key = str(metadata.get("source_key") or "").strip() or None

    existing_rows = (
        (
            await session.execute(
                select(AdmissionEvent).where(
                    and_(
                        AdmissionEvent.student_id == student_uuid,
                        AdmissionEvent.school_id == school_uuid,
                        AdmissionEvent.cycle_year == int(cycle_year),
                        AdmissionEvent.stage == stage_norm,
                        AdmissionEvent.source_name == source_name,
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    for existing in existing_rows:
        existing_meta = existing.metadata_ or {}
        if source_key and str(existing_meta.get("source_key") or "").strip() == source_key:
            return existing
        if (
            existing.happened_at == (happened_at or existing.happened_at)
            and existing.major_bucket == major_bucket
            and existing.evidence_ref == _as_uuid(evidence_ref)
        ):
            return existing

    row = AdmissionEvent(
        student_id=student_uuid,
        school_id=school_uuid,
        cycle_year=int(cycle_year),
        major_bucket=major_bucket,
        stage=stage_norm,
        happened_at=happened_at or datetime.now(timezone.utc),
        evidence_ref=_as_uuid(evidence_ref),
        source_name=source_name,
        metadata_=metadata or {},
    )
    session.add(row)
    await session.flush()

    outcome_value = _ADMISSION_STAGE_TO_VALUE[stage_norm]
    if outcome_value is not None:
        exists_stmt = select(CausalOutcomeEvent).where(
            and_(
                CausalOutcomeEvent.student_id == student_uuid,
                CausalOutcomeEvent.school_id == school_uuid,
                CausalOutcomeEvent.outcome_name == "admission_probability",
                CausalOutcomeEvent.observed_at >= datetime(cycle_year, 1, 1, tzinfo=timezone.utc),
                CausalOutcomeEvent.observed_at < datetime(cycle_year + 1, 1, 1, tzinfo=timezone.utc),
            )
        ).limit(1)
        existing = (await session.execute(exists_stmt)).scalars().first()
        if existing is None:
            session.add(
                CausalOutcomeEvent(
                    student_id=student_uuid,
                    school_id=school_uuid,
                    offer_id=None,
                    outcome_name="admission_probability",
                    outcome_value=float(outcome_value),
                    label_type="true",
                    label_confidence=0.99,
                    source="admission_events",
                    observed_at=row.happened_at,
                    metadata_={
                        "stage": stage_norm,
                        "admission_event_id": str(row.id),
                    },
                )
            )

    await session.flush()
    return row


async def ingest_ipeds_school_pool(
    session: AsyncSession,
    *,
    run_id: str,
    top_schools: int = 1000,
    years: int = 5,
    selection_metric: str = "applicants_total",
) -> dict[str, Any]:
    source = IPEDSCollegeNavigatorSource(
        dataset_url=settings.IPEDS_DATASET_URL,
        dataset_path=settings.IPEDS_DATASET_PATH,
    )
    records = await source.list_top_schools(
        top_n=top_schools,
        years=years,
        selection_metric=selection_metric,
    )
    if not records:
        return {
            "status": "ok",
            "run_id": run_id,
            "requested_top_schools": top_schools,
            "schools_upserted": 0,
            "external_ids_upserted": 0,
            "school_names": [],
        }

    schools_upserted = 0
    ids_upserted = 0
    school_names: list[str] = []
    for row in records:
        school_name = str(row.get("school_name") or "").strip()
        if not school_name:
            continue
        state = str(row.get("state") or "").strip() or "NA"
        city = str(row.get("city") or "").strip() or "Unknown"
        website_url = str(row.get("website_url") or "").strip() or None
        school = await session.scalar(
            select(School).where(
                and_(
                    func.lower(School.name) == school_name.lower(),
                    func.lower(School.state) == state.lower(),
                )
            )
        )
        if school is None:
            school = School(
                name=school_name,
                city=city,
                state=state,
                school_type="university",
                size_category="large",
                website_url=website_url,
                metadata_={
                    "official_seed": {
                        "run_id": run_id,
                        "source": "ipeds_college_navigator",
                    }
                },
            )
            session.add(school)
            await session.flush()
            schools_upserted += 1
        elif website_url and not school.website_url:
            school.website_url = website_url

        school_names.append(school.name)
        external_id = str(row.get("external_id") or "").strip()
        if external_id:
            updated = await _upsert_school_external_id(
                session,
                school_id=school.id,
                provider="ipeds",
                external_id=external_id,
                is_primary=True,
                match_method="ipeds_bulk_seed",
                confidence=0.99,
                metadata={
                    "run_id": run_id,
                    "selection_metric": selection_metric,
                    "score": row.get("score"),
                    "latest_year": row.get("latest_year"),
                },
            )
            if updated:
                ids_upserted += 1

    await session.flush()
    return {
        "status": "ok",
        "run_id": run_id,
        "requested_top_schools": top_schools,
        "schools_upserted": schools_upserted,
        "external_ids_upserted": ids_upserted,
        "school_names": sorted(set(school_names)),
    }


async def ingest_common_app_trends(
    session: AsyncSession,
    *,
    run_id: str,
    years: int = 5,
) -> dict[str, Any]:
    source = CommonAppTrendSource(
        dataset_url=settings.COMMON_APP_TREND_URL,
        dataset_path=settings.COMMON_APP_TREND_PATH,
    )
    signals = await source.load_signals(years=years)
    if not signals:
        return {
            "status": "ok",
            "run_id": run_id,
            "signals_written": 0,
            "signals_skipped": 0,
            "trend_coverage_rate": 0.0,
        }

    written = 0
    skipped = 0
    for signal in signals:
        existing = await session.scalar(
            select(CausalTrendSignal).where(
                and_(
                    CausalTrendSignal.source_name == signal.source_name,
                    CausalTrendSignal.metric == signal.metric,
                    CausalTrendSignal.period == signal.period,
                    CausalTrendSignal.segment.is_(signal.segment)
                    if signal.segment is None
                    else CausalTrendSignal.segment == signal.segment,
                    CausalTrendSignal.school_id.is_(None),
                )
            )
        )
        if existing is not None:
            skipped += 1
            continue
        session.add(
            CausalTrendSignal(
                source_name=signal.source_name,
                metric=signal.metric,
                period=signal.period,
                segment=signal.segment,
                school_id=None,
                value_numeric=signal.value_numeric,
                value_text=signal.value_text,
                source_url=signal.source_url,
                metadata_={
                    "run_id": run_id,
                    "provenance": "common_app_trend_only",
                    **(signal.metadata or {}),
                },
            )
        )
        written += 1
    await session.flush()
    attempted = written + skipped
    return {
        "status": "ok",
        "run_id": run_id,
        "signals_written": written,
        "signals_skipped": skipped,
        "trend_coverage_rate": round((written / attempted), 4) if attempted else 0.0,
    }


async def ingest_official_facts(
    session: AsyncSession,
    *,
    school_names: list[str],
    cycle_year: int,
    run_id: str,
    fields: list[str] | None = None,
    llm: LLMClient | None = None,
) -> dict[str, Any]:
    """Ingest official admission facts and write canonical/quarantine rows."""
    if not school_names:
        return {"run_id": run_id, "status": "ok", "raw_facts": 0}

    target_fields = [normalise_variable_name(field) for field in (fields or _DEFAULT_FACT_FIELDS)]
    metrics = IngestMetrics(run_id=run_id)
    llm_client = llm or get_llm_client()

    sources = _build_official_sources()

    school_rows = (
        await session.execute(
            select(School).where(func.lower(School.name).in_([name.lower() for name in school_names]))
        )
    ).scalars().all()
    school_map = {row.name.lower(): row for row in school_rows}
    updated_school_ids: set[str] = set()

    for school_name in school_names:
        school = school_map.get(school_name.lower())
        if school is None:
            continue
        metrics.processed_schools += 1
        school_results: list[SearchResult] = []
        seen_fields: set[str] = set()
        external_ids = await _load_school_external_ids(session, school_id=school.id)
        for source in sources:
            try:
                if hasattr(source, "search_for_school"):
                    rows = await source.search_for_school(
                        school_name=school.name,
                        school_state=school.state,
                        website_url=school.website_url,
                        fields=target_fields,
                        external_ids=external_ids,
                    )
                else:
                    rows = await source.search(school.name, target_fields)
            except Exception:
                logger.warning("Official source failed: %s", source.name, exc_info=True)
                continue
            for item in rows:
                school_results.append(item)
                seen_fields.add(normalise_variable_name(item.variable_name))

        if school_results:
            metrics.schools_with_any_fact += 1

        missing_fields = [
            field for field in target_fields if normalise_variable_name(field) not in seen_fields
        ]
        if missing_fields and (school.website_url or school.cds_url):
            try:
                fallback_results: list[SearchResult] = []
                if school.website_url:
                    fallback_results.extend(
                        await fetch_school_official_profile_direct(
                            session,
                            school=school,
                            fields=missing_fields,
                            run_id=run_id,
                        )
                    )
                fallback_results.extend(
                    await fetch_common_dataset_direct(
                        session,
                        school=school,
                        fields=missing_fields,
                        run_id=run_id,
                    )
                )
                for item in fallback_results:
                    school_results.append(item)
                    seen_fields.add(normalise_variable_name(item.variable_name))
            except Exception:
                logger.warning("Official direct fallback failed: %s", school.name, exc_info=True)

        for item in school_results:
            match_confidence = float((item.raw_data or {}).get("match_confidence") or 1.0)
            match_method = str((item.raw_data or {}).get("match_method") or "").strip().lower()
            external_id = str((item.raw_data or {}).get("external_id") or "").strip()
            if external_id:
                metrics.external_id_match_total += 1
                if match_confidence >= _MIN_EXTERNAL_ID_CONFIDENCE:
                    updated = await _upsert_school_external_id(
                        session,
                        school_id=school.id,
                        provider="ipeds",
                        external_id=external_id,
                        is_primary=(match_method == "external_id"),
                        match_method=match_method or "ipeds_bulk",
                        confidence=match_confidence,
                        metadata={
                            "run_id": run_id,
                            "source_name": item.source_name,
                            "source_url": item.source_url,
                        },
                    )
                    if updated:
                        external_ids["ipeds"] = external_id
                    metrics.external_id_match_success += 1
                else:
                    await _to_quarantine(
                        session=session,
                        school_id=school.id,
                        cycle_year=cycle_year,
                        outcome_name=normalise_variable_name(item.variable_name),
                        raw_value=item.value_text,
                        stage="entity_match",
                        reason="low_confidence_school_match",
                        source_name=item.source_name,
                        source_url=item.source_url,
                        confidence=match_confidence,
                        metadata={
                            "run_id": run_id,
                            "match_method": match_method,
                            "match_confidence": match_confidence,
                            "external_id": external_id,
                        },
                    )
                    metrics.quarantined_count += 1
                    continue

            await _process_official_fact_item(
                session=session,
                school=school,
                cycle_year=cycle_year,
                run_id=run_id,
                item=item,
                metrics=metrics,
                llm_client=llm_client,
                updated_school_ids=updated_school_ids,
            )
        if any(item.source_type == "official" for item in school_results):
            metrics.schools_with_official_fact += 1

    await session.flush()
    payload = metrics.to_dict()
    payload["status"] = "ok"
    payload["schools_updated"] = sorted(updated_school_ids)
    payload["schools_updated_count"] = len(updated_school_ids)
    return payload


async def reprocess_quarantine(
    session: AsyncSession,
    *,
    run_id: str,
    limit: int = 200,
    llm: LLMClient | None = None,
) -> dict[str, Any]:
    llm_client = llm or get_llm_client()
    rows = (
        (
            await session.execute(
                select(FactQuarantine)
                .where(FactQuarantine.resolved.is_(False))
                .order_by(FactQuarantine.created_at.asc())
                .limit(max(1, limit))
            )
        )
        .scalars()
        .all()
    )
    promoted = 0
    still_quarantined = 0
    for row in rows:
        judged = await _judge_fact(
            llm=llm_client,
            run_id=run_id,
            fact={
                "variable_name": row.outcome_name,
                "value_text": row.raw_value,
                "value_numeric": coerce_numeric(row.raw_value, variable_name=row.outcome_name),
            },
            metrics=None,
        )
        if judged["decision"] != "keep":
            still_quarantined += 1
            continue
        canonical = CanonicalFact(
            student_id=row.student_id,
            school_id=row.school_id,
            cycle_year=row.cycle_year or datetime.now(timezone.utc).year,
            outcome_name=row.outcome_name,
            canonical_value_text=row.raw_value,
            canonical_value_numeric=coerce_numeric(row.raw_value, variable_name=row.outcome_name),
            canonical_value_bucket=_canonical_bucket(
                row.raw_value,
                coerce_numeric(row.raw_value, variable_name=row.outcome_name),
            ),
            source_family=row.source_name,
            confidence=float(judged.get("confidence") or 0.6),
            observed_at=datetime.now(timezone.utc),
            metadata_={"reprocessed_from_quarantine": str(row.id), "run_id": run_id},
        )
        session.add(canonical)
        row.resolved = True
        promoted += 1
    await session.flush()
    return {
        "run_id": run_id,
        "status": "ok",
        "processed": len(rows),
        "promoted": promoted,
        "still_quarantined": still_quarantined,
    }


async def build_dataset_version(
    session: AsyncSession,
    *,
    version: str,
    lookback_days: int = 540,
    include_proxy: bool = True,
    min_true_per_outcome: int = 100,
    active_outcomes: list[str] | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    window_start = datetime.now(timezone.utc) - timedelta(days=max(lookback_days, 1))
    active = [
        normalise_variable_name(outcome)
        for outcome in (active_outcomes or _DEFAULT_ACTIVE_OUTCOMES)
    ]
    active_set = {outcome for outcome in active if outcome}
    if not active_set:
        active_set = {"admission_probability"}

    rows = (
        (
            await session.execute(
                select(CausalOutcomeEvent).where(CausalOutcomeEvent.observed_at >= window_start)
            )
        )
        .scalars()
        .all()
    )

    counts = defaultdict(int)
    true_counts = defaultdict(int)
    active_rows = []
    for row in rows:
        key = normalise_variable_name(row.outcome_name)
        if key not in active_set:
            continue
        active_rows.append(row)
        counts[key] += 1
        if str(row.label_type).lower() == "true":
            true_counts[key] += 1

    truth_ratio = {
        outcome: round(true_counts[outcome] / counts[outcome], 4) if counts[outcome] else 0.0
        for outcome in sorted(counts.keys())
    }
    missing_true = [
        outcome for outcome in sorted(counts.keys()) if true_counts[outcome] < min_true_per_outcome
    ]

    row = CausalDatasetVersion(
        version=version,
        status="ready" if not missing_true else "watch",
        config_json={
            "lookback_days": lookback_days,
            "include_proxy": include_proxy,
            "min_true_per_outcome": min_true_per_outcome,
            "active_outcomes": sorted(active_set),
        },
        stats_json={
            "rows_total": len(active_rows),
            "counts_by_outcome": dict(counts),
            "true_counts_by_outcome": dict(true_counts),
            "missing_true_outcomes": missing_true,
        },
        truth_ratio_by_outcome=truth_ratio,
        training_window_start=window_start,
        training_window_end=now,
        mini_gate_passed=False,
        notes=None,
    )
    session.add(row)
    await session.flush()
    return {
        "status": row.status,
        "version": row.version,
        "rows_total": len(active_rows),
        "counts_by_outcome": dict(counts),
        "true_counts_by_outcome": dict(true_counts),
        "truth_ratio_by_outcome": truth_ratio,
        "missing_true_outcomes": missing_true,
        "active_outcomes": sorted(active_set),
    }


async def run_mini_gate(
    session: AsyncSession,
    *,
    run_id: str,
    metrics: dict[str, Any],
    dataset_version: str | None = None,
) -> dict[str, Any]:
    schema_valid_rate = float(metrics.get("schema_valid_rate", 0.0))
    extraction_success = float(metrics.get("extraction_success", 0.0))
    unresolved_conflict_rate = float(metrics.get("unresolved_conflict_rate", 1.0))
    quarantine_rate = float(metrics.get("quarantine_rate", 1.0))
    evidence_coverage_rate = float(metrics.get("evidence_coverage_rate", 0.0))
    rpm_actual_avg = float(metrics.get("rpm_actual_avg", 0.0))
    rate_limit_error_count = int(metrics.get("rate_limit_error_count", 0))

    checks = {
        "schema_valid_rate": schema_valid_rate >= _MINI_GATE_THRESHOLDS["schema_valid_rate"],
        "extraction_success": extraction_success >= _MINI_GATE_THRESHOLDS["extraction_success"],
        "unresolved_conflict_rate": unresolved_conflict_rate <= _MINI_GATE_THRESHOLDS["unresolved_conflict_rate"],
        "quarantine_rate": quarantine_rate <= _MINI_GATE_THRESHOLDS["quarantine_rate"],
        "evidence_coverage_rate": evidence_coverage_rate >= _MINI_GATE_THRESHOLDS["evidence_coverage_rate"],
        "rpm_actual_avg": rpm_actual_avg <= _MINI_GATE_THRESHOLDS["rpm_actual_avg"],
        "rate_limit_error_count": rate_limit_error_count <= _MINI_GATE_THRESHOLDS["rate_limit_error_count"],
    }
    passed = all(checks.values())

    if dataset_version:
        row = await session.scalar(
            select(CausalDatasetVersion).where(CausalDatasetVersion.version == dataset_version)
        )
        if row is not None:
            row.mini_gate_passed = passed
            stats = dict(row.stats_json or {})
            stats["mini_gate"] = {
                "run_id": run_id,
                "passed": passed,
                "checks": checks,
                "metrics": metrics,
            }
            row.stats_json = stats

    await session.flush()
    return {
        "run_id": run_id,
        "mini_gate_passed": passed,
        "checks": checks,
        "thresholds": dict(_MINI_GATE_THRESHOLDS),
        "metrics": metrics,
    }


async def _load_school_external_ids(
    session: AsyncSession,
    *,
    school_id: uuid.UUID,
) -> dict[str, str]:
    rows = (
        (
            await session.execute(
                select(SchoolExternalId).where(SchoolExternalId.school_id == school_id)
            )
        )
        .scalars()
        .all()
    )
    out: dict[str, str] = {}
    for row in rows:
        provider = str(row.provider or "").strip().lower()
        external_id = str(row.external_id or "").strip()
        if provider and external_id:
            out[provider] = external_id
    return out


async def _upsert_school_external_id(
    session: AsyncSession,
    *,
    school_id: uuid.UUID,
    provider: str,
    external_id: str,
    is_primary: bool,
    match_method: str,
    confidence: float,
    metadata: dict[str, Any] | None = None,
) -> bool:
    provider_norm = str(provider or "").strip().lower()
    external_norm = str(external_id or "").strip()
    if not provider_norm or not external_norm:
        return False

    existing = await session.scalar(
        select(SchoolExternalId).where(
            and_(
                SchoolExternalId.provider == provider_norm,
                SchoolExternalId.external_id == external_norm,
            )
        )
    )
    if existing is not None:
        if existing.school_id != school_id:
            return False
        existing.match_method = match_method
        existing.confidence = confidence
        existing.metadata_ = metadata or {}
        existing.is_primary = bool(is_primary)
        return True

    existing_slot = await session.scalar(
        select(SchoolExternalId).where(
            and_(
                SchoolExternalId.school_id == school_id,
                SchoolExternalId.provider == provider_norm,
                SchoolExternalId.is_primary == bool(is_primary),
            )
        )
    )
    if existing_slot is not None:
        existing_slot.external_id = external_norm
        existing_slot.match_method = match_method
        existing_slot.confidence = confidence
        existing_slot.metadata_ = metadata or {}
        return True

    row = SchoolExternalId(
        school_id=school_id,
        provider=provider_norm,
        external_id=external_norm,
        is_primary=bool(is_primary),
        match_method=match_method,
        confidence=confidence,
        metadata_=metadata or {},
    )
    session.add(row)
    await session.flush()
    return True


def _build_official_sources():
    sources = []
    scorecard_key = (settings.SCORECARD_API_KEY or "").strip()
    if scorecard_key:
        sources.append(CollegeScorecardSource(api_key=scorecard_key))
    ipeds_url = (settings.IPEDS_DATASET_URL or "").strip()
    ipeds_path = (settings.IPEDS_DATASET_PATH or "").strip()
    if ipeds_url or ipeds_path:
        sources.append(
            IPEDSCollegeNavigatorSource(
                dataset_url=ipeds_url,
                dataset_path=ipeds_path,
            )
        )
    sources.append(
        SchoolOfficialProfileSource(
            search_api_url=settings.SCHOOL_PROFILE_SEARCH_API_URL or settings.WEB_SEARCH_API_URL,
            search_api_key=settings.SCHOOL_PROFILE_SEARCH_API_KEY or settings.WEB_SEARCH_API_KEY,
        )
    )
    sources.append(
        CommonDataSetSource(
            search_api_url=settings.SCHOOL_PROFILE_SEARCH_API_URL or settings.WEB_SEARCH_API_URL,
            search_api_key=settings.SCHOOL_PROFILE_SEARCH_API_KEY or settings.WEB_SEARCH_API_KEY,
        )
    )
    return sources


def _apply_official_school_fact(
    school: School,
    *,
    variable_name: str,
    value_text: str,
    value_numeric: float | None,
    source_name: str,
    source_url: str | None,
    fetch_mode: str,
    run_id: str,
    cycle_year: int,
) -> None:
    field_name = _OFFICIAL_SCHOOL_FIELD_MAP.get(normalise_variable_name(variable_name))
    if field_name is not None and hasattr(school, field_name):
        if field_name in {"avg_net_price", "tuition_oos", "sat_25", "sat_75", "act_25", "act_75", "endowment_per_student"}:
            setattr(school, field_name, _coerce_int(value_numeric, value_text))
        elif field_name in {"acceptance_rate", "graduation_rate_4yr", "student_faculty_ratio"}:
            setattr(school, field_name, _coerce_float(value_numeric, value_text))
        else:
            setattr(school, field_name, value_text.strip())

    metadata = dict(school.metadata_ or {})
    official = dict(metadata.get("official_facts") or {})
    fields = dict(official.get("fields") or {})
    fields[normalise_variable_name(variable_name)] = {
        "value_text": value_text,
        "value_numeric": value_numeric,
        "source_name": source_name,
        "source_url": source_url,
        "fetch_mode": fetch_mode,
        "run_id": run_id,
        "cycle_year": cycle_year,
    }
    official["fields"] = fields
    official["run_id"] = run_id
    official["cycle_year"] = cycle_year
    official["updated_at"] = datetime.now(timezone.utc).isoformat()
    official["field_count"] = len(fields)
    official["last_fetch_mode"] = fetch_mode
    metadata["official_facts"] = official
    school.metadata_ = metadata


async def _process_official_fact_item(
    *,
    session: AsyncSession,
    school: School,
    cycle_year: int,
    run_id: str,
    item: SearchResult,
    metrics: IngestMetrics,
    llm_client: LLMClient,
    updated_school_ids: set[str],
) -> None:
    fetch_mode = str((item.raw_data or {}).get("fetch_mode") or "search_api")
    evidence_payload = {
        "variable_name": item.variable_name,
        "value_text": item.value_text,
        "value_numeric": item.value_numeric,
        "temporal_range": item.temporal_range,
    }
    evidence = await register_evidence_artifact(
        session,
        student_id=None,
        school_id=str(school.id),
        cycle_year=cycle_year,
        source_name=item.source_name,
        source_type="official_source",
        source_url=item.source_url,
        content_text=json.dumps(evidence_payload, ensure_ascii=False, sort_keys=True),
        metadata={
            "run_id": run_id,
            "school_name": school.name,
            "source_url": item.source_url,
            "fetch_mode": fetch_mode,
            "source_kind": (item.raw_data or {}).get("source_kind"),
        },
    )
    metrics.raw_facts += 1
    cleaned = await _clean_fact(
        llm=llm_client,
        run_id=run_id,
        source_name=item.source_name,
        source_url=item.source_url,
        variable_name=item.variable_name,
        value_text=item.value_text,
        value_numeric=item.value_numeric,
        metrics=metrics,
    )
    if cleaned is None:
        await _to_quarantine(
            session=session,
            school_id=school.id,
            cycle_year=cycle_year,
            outcome_name=normalise_variable_name(item.variable_name),
            raw_value=item.value_text,
            stage="rule_normalize",
            reason="invalid_schema_or_value",
            source_name=item.source_name,
            source_url=item.source_url,
            confidence=item.confidence,
            metadata={
                "run_id": run_id,
                "fetch_mode": fetch_mode,
                "evidence_artifact_id": str(evidence.id),
            },
        )
        metrics.quarantined_count += 1
        return
    metrics.schema_valid_count += 1

    judged = await _judge_fact(
        llm=llm_client,
        run_id=run_id,
        fact=cleaned,
        metrics=metrics,
    )
    if judged["decision"] != "keep":
        await _to_quarantine(
            session=session,
            school_id=school.id,
            cycle_year=cycle_year,
            outcome_name=cleaned["variable_name"],
            raw_value=cleaned["value_text"],
            stage="llm_judge_fact",
            reason=str(judged.get("reason") or "judge_reject"),
            source_name=item.source_name,
            source_url=item.source_url,
            confidence=float(judged.get("confidence") or 0.0),
            metadata={
                "run_id": run_id,
                "fetch_mode": fetch_mode,
                "judge": judged,
                "evidence_artifact_id": str(evidence.id),
            },
        )
        metrics.quarantined_count += 1
        return

    conflict = await _check_conflict(
        session=session,
        school_id=school.id,
        cycle_year=cycle_year,
        variable_name=cleaned["variable_name"],
        value_numeric=cleaned["value_numeric"],
        value_text=cleaned["value_text"],
    )
    if conflict and conflict.get("duplicate_fact_id"):
        _apply_official_school_fact(
            school,
            variable_name=cleaned["variable_name"],
            value_text=cleaned["value_text"],
            value_numeric=cleaned["value_numeric"],
            source_name=item.source_name,
            source_url=item.source_url,
            fetch_mode=fetch_mode,
            run_id=run_id,
            cycle_year=cycle_year,
        )
        updated_school_ids.add(str(school.id))
        await _append_lineage(
            session=session,
            canonical_fact_id=str(conflict["duplicate_fact_id"]),
            evidence_artifact_id=evidence.id,
            source_name=item.source_name,
            source_url=item.source_url,
            raw_value_text=item.value_text,
            raw_value_numeric=item.value_numeric,
            decision="duplicate",
            metadata={"run_id": run_id, "fetch_mode": fetch_mode},
        )
        metrics.deduped_count += 1
        metrics.extracted_count += 1
        return

    if conflict:
        metrics.conflicts_count += 1
        await _to_quarantine(
            session=session,
            school_id=school.id,
            cycle_year=cycle_year,
            outcome_name=cleaned["variable_name"],
            raw_value=cleaned["value_text"],
            stage="conflict_merge",
            reason="conflict_with_existing_canonical",
            source_name=item.source_name,
            source_url=item.source_url,
            confidence=float(judged.get("confidence") or 0.0),
            metadata={
                "run_id": run_id,
                "fetch_mode": fetch_mode,
                "conflict": conflict,
                "evidence_artifact_id": str(evidence.id),
            },
        )
        metrics.quarantined_count += 1
        return

    _apply_official_school_fact(
        school,
        variable_name=cleaned["variable_name"],
        value_text=cleaned["value_text"],
        value_numeric=cleaned["value_numeric"],
        source_name=item.source_name,
        source_url=item.source_url,
        fetch_mode=fetch_mode,
        run_id=run_id,
        cycle_year=cycle_year,
    )
    updated_school_ids.add(str(school.id))
    canonical = await _create_or_get_canonical(
        session=session,
        school_id=school.id,
        cycle_year=cycle_year,
        outcome_name=cleaned["variable_name"],
        value_text=cleaned["value_text"],
        value_numeric=cleaned["value_numeric"],
        source_family=item.source_name,
        confidence=max(float(item.confidence), float(judged.get("confidence") or 0.5)),
        observed_at=datetime.now(timezone.utc),
        metadata={
            "run_id": run_id,
            "source_url": item.source_url,
            "temporal_range": item.temporal_range,
            "fetch_mode": fetch_mode,
            "judge": judged,
        },
    )
    await _append_lineage(
        session=session,
        canonical_fact_id=str(canonical.id),
        evidence_artifact_id=evidence.id,
        source_name=item.source_name,
        source_url=item.source_url,
        raw_value_text=item.value_text,
        raw_value_numeric=item.value_numeric,
        decision="kept",
        metadata={"run_id": run_id, "fetch_mode": fetch_mode},
    )
    metrics.kept_count += 1
    metrics.extracted_count += 1


_OFFICIAL_SCHOOL_FIELD_MAP: dict[str, str] = {
    "acceptance_rate": "acceptance_rate",
    "avg_net_price": "avg_net_price",
    "tuition_out_of_state": "tuition_oos",
    "graduation_rate_4yr": "graduation_rate_4yr",
    "student_faculty_ratio": "student_faculty_ratio",
    "endowment_per_student": "endowment_per_student",
    "website_url": "website_url",
    "school_url": "website_url",
    "city": "city",
    "state": "state",
    "sat_25": "sat_25",
    "sat_75": "sat_75",
    "act_25": "act_25",
    "act_75": "act_75",
}


def _coerce_int(value_numeric: float | None, value_text: str) -> int | None:
    if value_numeric is not None:
        return int(round(float(value_numeric)))
    cleaned = coerce_numeric(value_text, variable_name="unknown")
    if cleaned is None:
        return None
    return int(round(cleaned))


def _coerce_float(value_numeric: float | None, value_text: str) -> float | None:
    if value_numeric is not None:
        return float(value_numeric)
    cleaned = coerce_numeric(value_text, variable_name="unknown")
    if cleaned is None:
        return None
    return float(cleaned)


async def _clean_fact(
    *,
    llm: LLMClient,
    run_id: str,
    source_name: str,
    source_url: str,
    variable_name: str,
    value_text: str,
    value_numeric: float | None,
    metrics: IngestMetrics,
) -> dict[str, Any] | None:
    var = normalise_variable_name(variable_name)
    numeric = value_numeric
    if numeric is None:
        numeric = coerce_numeric(value_text, variable_name=var)
    else:
        numeric = normalise_numeric(float(numeric), variable_name=var, value_text=value_text)

    if not value_text.strip():
        return None

    # Optional LLM extraction for noisy numeric payloads.
    if numeric is None and any(token in var for token in ("rate", "pct", "price", "tuition", "sat", "act")):
        metrics.llm_calls_extract += 1
        try:
            extracted = await llm.complete_json(
                [
                    {
                        "role": "system",
                        "content": (
                            "Normalize one admissions fact. Return JSON with keys: "
                            "value_text, value_numeric."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"variable_name={var}\nsource={source_name}\nurl={source_url}\n"
                            f"value_text={value_text}"
                        ),
                    },
                ],
                temperature=0.0,
                max_tokens=120,
                caller=f"data.clean.extract#{run_id}",
            )
            extracted_text = str(extracted.get("value_text") or value_text).strip()
            extracted_numeric_raw = extracted.get("value_numeric")
            if extracted_numeric_raw is not None:
                try:
                    numeric = normalise_numeric(
                        float(extracted_numeric_raw),
                        variable_name=var,
                        value_text=extracted_text,
                    )
                    value_text = extracted_text
                except (TypeError, ValueError):
                    numeric = coerce_numeric(extracted_text, variable_name=var)
                    value_text = extracted_text
        except Exception:
            metrics.llm_errors += 1
            logger.debug("LLM extract failed for %s", var, exc_info=True)

    return {
        "variable_name": var,
        "value_text": value_text.strip(),
        "value_numeric": numeric,
    }


async def _judge_fact(
    *,
    llm: LLMClient,
    run_id: str,
    fact: dict[str, Any],
    metrics: IngestMetrics | None,
) -> dict[str, Any]:
    if metrics is not None:
        metrics.llm_calls_judge += 1
    try:
        payload = await llm.complete_json(
            [
                {
                    "role": "system",
                    "content": (
                        "Judge if this admissions fact is evidence-ready. "
                        "Return JSON: {decision: keep|quarantine, confidence: 0-1, reason: str}."
                    ),
                },
                {
                    "role": "user",
                    "content": str(fact),
                },
            ],
            temperature=0.0,
            max_tokens=160,
            caller=f"data.clean.judge#{run_id}",
        )
        decision = str(payload.get("decision") or "quarantine").strip().lower()
        if decision not in {"keep", "quarantine"}:
            decision = "quarantine"
        confidence = float(payload.get("confidence") or 0.0)
        reason = str(payload.get("reason") or "").strip() or "judge_no_reason"
        return {"decision": decision, "confidence": max(0.0, min(1.0, confidence)), "reason": reason}
    except Exception:
        if metrics is not None:
            metrics.llm_errors += 1
        logger.debug("LLM judge failed", exc_info=True)
        return {"decision": "keep", "confidence": 0.55, "reason": "judge_fallback_keep"}


async def _create_or_get_canonical(
    *,
    session: AsyncSession,
    school_id: uuid.UUID,
    cycle_year: int,
    outcome_name: str,
    value_text: str,
    value_numeric: float | None,
    source_family: str,
    confidence: float,
    observed_at: datetime,
    metadata: dict[str, Any] | None,
) -> CanonicalFact:
    value_bucket = _canonical_bucket(value_text, value_numeric)
    existing = await session.scalar(
        select(CanonicalFact).where(
            and_(
                CanonicalFact.student_id.is_(None),
                CanonicalFact.school_id == school_id,
                CanonicalFact.cycle_year == cycle_year,
                CanonicalFact.outcome_name == outcome_name,
                CanonicalFact.canonical_value_bucket == value_bucket,
                CanonicalFact.source_family == source_family,
            )
        )
    )
    if existing is not None:
        return existing

    canonical = CanonicalFact(
        student_id=None,
        school_id=school_id,
        cycle_year=cycle_year,
        outcome_name=outcome_name,
        canonical_value_text=value_text,
        canonical_value_numeric=value_numeric,
        canonical_value_bucket=value_bucket,
        source_family=source_family,
        confidence=confidence,
        observed_at=observed_at,
        metadata_=metadata or {},
    )
    session.add(canonical)
    await session.flush()
    return canonical


async def _append_lineage(
    *,
    session: AsyncSession,
    canonical_fact_id: str,
    evidence_artifact_id: uuid.UUID | None,
    source_name: str,
    source_url: str | None,
    raw_value_text: str,
    raw_value_numeric: float | None,
    decision: str,
    metadata: dict[str, Any] | None,
) -> None:
    canonical_uuid = _as_uuid(canonical_fact_id)
    if canonical_uuid is None:
        return
    existing = await session.scalar(
        select(FactLineage).where(
            and_(
                FactLineage.canonical_fact_id == canonical_uuid,
                FactLineage.evidence_artifact_id.is_(evidence_artifact_id)
                if evidence_artifact_id is None
                else FactLineage.evidence_artifact_id == evidence_artifact_id,
                FactLineage.source_name == source_name,
                FactLineage.raw_value_text == raw_value_text,
            )
        )
    )
    if existing is not None:
        return
    session.add(
        FactLineage(
            canonical_fact_id=canonical_uuid,
            evidence_artifact_id=evidence_artifact_id,
            source_name=source_name,
            source_url=source_url,
            raw_value_text=raw_value_text,
            raw_value_numeric=raw_value_numeric,
            decision=decision,
            metadata_=metadata or {},
        )
    )


async def _check_conflict(
    *,
    session: AsyncSession,
    school_id: uuid.UUID,
    cycle_year: int,
    variable_name: str,
    value_numeric: float | None,
    value_text: str,
) -> dict[str, Any] | None:
    rows = (
        (
            await session.execute(
                select(CanonicalFact).where(
                    and_(
                        CanonicalFact.school_id == school_id,
                        CanonicalFact.cycle_year == cycle_year,
                        CanonicalFact.outcome_name == variable_name,
                    )
                )
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        return None
    new_bucket = _canonical_bucket(value_text, value_numeric)
    for row in rows:
        if row.canonical_value_bucket == new_bucket:
            return {"duplicate_fact_id": str(row.id)}
        if value_numeric is not None and row.canonical_value_numeric is not None:
            base = max(abs(row.canonical_value_numeric), 1e-6)
            relative = abs(value_numeric - row.canonical_value_numeric) / base
            if relative > 0.20:
                return {
                    "existing_fact_id": str(row.id),
                    "relative_diff": round(relative, 4),
                }
        elif row.canonical_value_text.strip().lower() != value_text.strip().lower():
            return {"existing_fact_id": str(row.id), "text_mismatch": True}
    return None


async def _to_quarantine(
    *,
    session: AsyncSession,
    school_id: uuid.UUID | None,
    cycle_year: int | None,
    outcome_name: str,
    raw_value: str,
    stage: str,
    reason: str,
    source_name: str,
    source_url: str | None,
    confidence: float | None,
    metadata: dict[str, Any] | None,
) -> None:
    safe_reason = (reason or "").strip() or "unknown"
    if len(safe_reason) > 200:
        safe_reason = safe_reason[:197] + "..."
    safe_stage = (stage or "").strip() or "unknown"
    safe_source_name = (source_name or "").strip() or "unknown"
    session.add(
        FactQuarantine(
            student_id=None,
            school_id=school_id,
            cycle_year=cycle_year,
            outcome_name=outcome_name,
            raw_value=raw_value,
            stage=safe_stage[:50],
            reason=safe_reason,
            source_name=safe_source_name[:80],
            source_url=source_url,
            confidence=confidence,
            resolved=False,
            metadata_=metadata or {},
        )
    )


def _canonical_bucket(value_text: str, value_numeric: float | None) -> str:
    return fingerprint_value(value_text=value_text, value_numeric=value_numeric)


def _as_uuid(value: str | uuid.UUID | None) -> uuid.UUID | None:
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return uuid.UUID(text)
    except ValueError:
        return None
