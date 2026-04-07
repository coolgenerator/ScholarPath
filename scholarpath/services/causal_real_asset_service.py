"""Helpers for building real-only causal training assets.

This module intentionally keeps the real-truth path separate from the
synthetic/bootstrap pipeline so admission-only supervision can be expanded
without mixing proxy labels into the primary training set.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.causal_engine.feature_builder import build_feature_payload
from scholarpath.db.models import (
    AdmissionEvent,
    CausalFeatureSnapshot,
    Offer,
    School,
    SchoolEvaluation,
    Student,
)
from scholarpath.services.causal_data_service import (
    build_dataset_version,
    ingest_official_facts,
    register_admission_event,
    register_evidence_artifact,
)

logger = logging.getLogger(__name__)

_TRUTH_STAGES = {"admit", "commit", "reject", "declined", "waitlist", "deferred"}
_EVENT_ONLY_STAGES = {"submitted", "interview"}


async def backfill_real_admission_assets(
    session: AsyncSession,
    *,
    run_id: str,
    student_ids: list[str] | None = None,
    school_ids: list[str] | None = None,
    import_rows: list[dict[str, Any]] | None = None,
    include_school_evaluations: bool = True,
    include_offers: bool = True,
    include_admission_events: bool = True,
    ingest_official_facts_enabled: bool = False,
    cycle_year: int | None = None,
    active_outcomes: list[str] | None = None,
    lookback_days: int = 540,
    min_true_per_outcome: int = 1,
    build_dataset: bool = True,
    dataset_version: str | None = None,
) -> dict[str, Any]:
    """Backfill real-only causal training assets from current business data.

    The primary supervision is admission truth only:
    - ``CausalFeatureSnapshot`` is built from real student/school/offer/event rows.
    - ``CausalOutcomeEvent`` is only created for externally verifiable admission
      truth stages: admit/commit/reject/declined/waitlist/deferred.
    - submitted/interview are preserved as event-only records.
    """
    active = [str(item).strip() for item in (active_outcomes or ["admission_probability"]) if str(item).strip()]
    if not active:
        active = ["admission_probability"]

    students = await _load_students(session, student_ids)
    if not students:
        return {
            "status": "failed_precondition",
            "reason": "no_students",
        }

    student_map = {str(student.id): student for student in students}

    school_ids_from_scope = [str(item).strip() for item in (school_ids or []) if str(item).strip()]
    school_map = await _load_schools(session, school_ids_from_scope or None)

    official_school_ids: set[str] = set()
    if ingest_official_facts_enabled:
        school_names = [school.name for school in school_map.values()]
        if school_names:
            try:
                ingest_result = await ingest_official_facts(
                    session,
                    school_names=school_names,
                    cycle_year=cycle_year or datetime.now(timezone.utc).year,
                    run_id=f"{run_id}:official-facts",
                )
                official_school_ids = {
                    str(item).strip()
                    for item in (ingest_result.get("schools_updated") or [])
                    if str(item).strip()
                }
            except Exception:
                logger.warning("Optional official fact refresh failed", exc_info=True)
                official_school_ids = set()

    eval_rows = await _load_school_evaluations(session, student_map.keys(), school_map.keys(), include_school_evaluations)
    offer_rows = await _load_offers(session, student_map.keys(), school_map.keys(), include_offers)
    event_rows = await _load_admission_events(session, student_map.keys(), school_map.keys(), include_admission_events)

    # Ensure imported rows are normalized to the same schema.
    import_rows = import_rows or []
    normalized_import_rows = [
        _normalize_import_row(row, default_cycle_year=cycle_year)
        for row in import_rows
        if isinstance(row, dict)
    ]
    normalized_import_rows = [row for row in normalized_import_rows if row]

    # If no explicit school scope was provided, infer the required schools from source rows.
    required_school_ids = set(school_map.keys())
    required_school_ids.update(str(row.school_id) for row in eval_rows)
    required_school_ids.update(str(row.school_id) for row in offer_rows)
    required_school_ids.update(str(row.school_id) for row in event_rows)
    required_school_ids.update(str(row["school_id"]) for row in normalized_import_rows if row.get("school_id"))
    if not required_school_ids:
        return {
            "status": "failed_precondition",
            "reason": "no_school_scope",
        }

    # Fill in school rows for any inferred schools.
    missing_school_ids = [school_id for school_id in required_school_ids if school_id not in school_map]
    if missing_school_ids:
        extra_school_map = await _load_schools(session, missing_school_ids)
        school_map.update(extra_school_map)

    existing_snapshot_keys = await _load_existing_source_keys(
        session,
        CausalFeatureSnapshot,
        student_ids=student_map.keys(),
        school_ids=school_map.keys(),
    )
    existing_event_keys = await _load_existing_source_keys(
        session,
        AdmissionEvent,
        student_ids=student_map.keys(),
        school_ids=school_map.keys(),
    )
    created_snapshots = 0
    created_events = 0
    created_outcomes = 0
    created_official_snapshots = 0
    deduped_snapshots = 0
    deduped_events = 0
    deduped_outcomes = 0

    # 0) Official school-profile snapshots to thicken real-school context.
    if official_school_ids:
        snapshot_cycle_year = cycle_year or datetime.now(timezone.utc).year
        for student in students:
            for school_id in official_school_ids:
                school = school_map.get(str(school_id))
                if student is None or school is None:
                    continue
                source_key = f"official_school_profile:{student.id}:{school.id}:{snapshot_cycle_year}"
                if source_key in existing_snapshot_keys:
                    deduped_snapshots += 1
                    continue
                payload = build_feature_payload(
                    student=student,
                    school=school,
                    context="official_school_profile",
                    metadata={
                        "run_id": run_id,
                        "source_key": source_key,
                        "source_kind": "official_school_profile",
                        "cycle_year": snapshot_cycle_year,
                    },
                )
                session.add(
                    CausalFeatureSnapshot(
                        student_id=student.id,
                        school_id=school.id,
                        offer_id=None,
                        context="official_school_profile",
                        feature_payload=payload.as_dict(),
                        metadata_={
                            "run_id": run_id,
                            "source_key": source_key,
                            "source_kind": "official_school_profile",
                            "cycle_year": snapshot_cycle_year,
                            "school_name": school.name,
                        },
                        observed_at=datetime.now(timezone.utc),
                    )
                )
                existing_snapshot_keys.add(source_key)
                created_official_snapshots += 1

    # 1) Real student-school snapshots from existing evaluations.
    if include_school_evaluations:
        for row in eval_rows:
            student = student_map.get(str(row.student_id))
            school = school_map.get(str(row.school_id))
            if student is None or school is None:
                continue
            source_key = f"school_evaluation:{row.id}"
            if source_key in existing_snapshot_keys:
                deduped_snapshots += 1
                continue
            payload = build_feature_payload(
                student=student,
                school=school,
                context="school_evaluation",
                metadata={
                    "run_id": run_id,
                    "source_key": source_key,
                    "source_kind": "school_evaluation",
                    "source_row_id": str(row.id),
                },
            )
            session.add(
                CausalFeatureSnapshot(
                    student_id=student.id,
                    school_id=school.id,
                    offer_id=None,
                    context="school_evaluation",
                    feature_payload=payload.as_dict(),
                    metadata_={
                        "run_id": run_id,
                        "source_key": source_key,
                        "source_kind": "school_evaluation",
                        "source_row_id": str(row.id),
                    },
                    observed_at=row.created_at or datetime.now(timezone.utc),
                )
            )
            existing_snapshot_keys.add(source_key)
            created_snapshots += 1

    # 2) Offer-derived truths and snapshots.
    if include_offers:
        for offer in offer_rows:
            student = student_map.get(str(offer.student_id))
            school = school_map.get(str(offer.school_id))
            if student is None or school is None:
                continue
            offer_key = f"offer:{offer.id}"
            if offer_key not in existing_snapshot_keys:
                payload = build_feature_payload(
                    student=student,
                    school=school,
                    context="offer_context",
                    metadata={
                        "run_id": run_id,
                        "source_key": offer_key,
                        "source_kind": "offer",
                        "source_row_id": str(offer.id),
                        "offer_status": offer.status,
                    },
                )
                session.add(
                    CausalFeatureSnapshot(
                        student_id=student.id,
                        school_id=school.id,
                        offer_id=offer.id,
                        context="offer_context",
                        feature_payload=payload.as_dict(),
                        metadata_={
                            "run_id": run_id,
                            "source_key": offer_key,
                            "source_kind": "offer",
                            "source_row_id": str(offer.id),
                            "offer_status": offer.status,
                        },
                        observed_at=offer.created_at or datetime.now(timezone.utc),
                    )
                )
                existing_snapshot_keys.add(offer_key)
                created_snapshots += 1
            else:
                deduped_snapshots += 1

            stage = _offer_status_to_stage(offer.status)
            if stage is None:
                continue
            event_key = f"offer_truth_event:{offer.id}:{stage}"
            if event_key in existing_event_keys:
                deduped_events += 1
                continue
            await register_admission_event(
                session,
                student_id=str(student.id),
                school_id=str(school.id),
                cycle_year=offer.created_at.year if offer.created_at else (cycle_year or datetime.now(timezone.utc).year),
                major_bucket=None,
                stage=stage,
                happened_at=offer.created_at or datetime.now(timezone.utc),
                evidence_ref=None,
                source_name="offer_import",
                metadata={
                    "run_id": run_id,
                    "source_key": event_key,
                    "source_kind": "offer",
                    "offer_id": str(offer.id),
                    "offer_status": offer.status,
                },
            )
            created_events += 1
            existing_event_keys.add(event_key)
            if _stage_creates_truth(stage):
                created_outcomes += 1

    # 3) Existing admission events become snapshots and remain the truth source.
    if include_admission_events:
        for row in event_rows:
            student = student_map.get(str(row.student_id))
            school = school_map.get(str(row.school_id))
            if student is None or school is None:
                continue
            source_key = f"admission_event:{row.id}"
            if source_key not in existing_snapshot_keys:
                payload = build_feature_payload(
                    student=student,
                    school=school,
                    context="admission_event",
                    metadata={
                        "run_id": run_id,
                        "source_key": source_key,
                        "source_kind": "admission_event",
                        "source_row_id": str(row.id),
                        "stage": row.stage,
                    },
                )
                session.add(
                    CausalFeatureSnapshot(
                        student_id=student.id,
                        school_id=school.id,
                        offer_id=None,
                        context="admission_event",
                        feature_payload=payload.as_dict(),
                        metadata_={
                            "run_id": run_id,
                            "source_key": source_key,
                            "source_kind": "admission_event",
                            "source_row_id": str(row.id),
                            "stage": row.stage,
                        },
                        observed_at=row.created_at or datetime.now(timezone.utc),
                    )
                )
                existing_snapshot_keys.add(source_key)
                created_snapshots += 1
            else:
                deduped_snapshots += 1

    # 4) Imported rows from CSV/JSON are treated as external truth records.
    for row in normalized_import_rows:
        student = student_map.get(str(row["student_id"]))
        school = school_map.get(str(row["school_id"]))
        if student is None or school is None:
            continue
        source_key = str(row["source_key"])
        snapshot_key = f"import_snapshot:{source_key}"
        if snapshot_key not in existing_snapshot_keys:
            payload = build_feature_payload(
                student=student,
                school=school,
                context="imported_truth",
                metadata={
                    "run_id": run_id,
                    "source_key": snapshot_key,
                    "source_kind": "import",
                    "source_row_id": source_key,
                },
            )
            session.add(
                CausalFeatureSnapshot(
                    student_id=student.id,
                    school_id=school.id,
                    offer_id=None,
                    context="imported_truth",
                    feature_payload=payload.as_dict(),
                    metadata_={
                        "run_id": run_id,
                        "source_key": snapshot_key,
                        "source_kind": "import",
                        "source_row_id": source_key,
                    },
                    observed_at=row.get("happened_at") or datetime.now(timezone.utc),
                )
            )
            existing_snapshot_keys.add(snapshot_key)
            created_snapshots += 1
        else:
            deduped_snapshots += 1

        if row["stage"] not in _TRUTH_STAGES and row["stage"] not in _EVENT_ONLY_STAGES:
            continue

        import_event_key = f"import_event:{source_key}:{row['stage']}"
        if import_event_key in existing_event_keys:
            deduped_events += 1
            continue

        evidence_ref = None
        if row.get("content_text"):
            evidence = await register_evidence_artifact(
                session,
                student_id=str(student.id),
                school_id=str(school.id),
                cycle_year=int(row["cycle_year"]),
                source_name=str(row.get("source_name") or "import"),
                source_type=str(row.get("source_type") or "user_upload"),
                source_url=row.get("source_url"),
                content_text=str(row["content_text"]),
                metadata={
                    "run_id": run_id,
                    "source_key": source_key,
                    "source_kind": "import",
                },
            )
            evidence_ref = str(evidence.id)

        await register_admission_event(
            session,
            student_id=str(student.id),
            school_id=str(school.id),
            cycle_year=int(row["cycle_year"]),
            major_bucket=row.get("major_bucket"),
            stage=str(row["stage"]),
            happened_at=row.get("happened_at"),
            evidence_ref=evidence_ref,
            source_name=str(row.get("source_name") or "import"),
            metadata={
                "run_id": run_id,
                "source_key": import_event_key,
                "source_kind": "import",
                "source_row_id": source_key,
            },
        )
        existing_event_keys.add(import_event_key)
        if _stage_creates_truth(str(row["stage"])):
            created_outcomes += 1
        created_events += 1

    dataset_result: dict[str, Any] | None = None
    if build_dataset:
        version = dataset_version or f"causal-real-admission-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
        dataset_result = await build_dataset_version(
            session,
            version=version,
            lookback_days=lookback_days,
            include_proxy=False,
            min_true_per_outcome=min_true_per_outcome,
            active_outcomes=active,
        )

    await session.flush()
    return {
        "status": "ok",
        "run_id": run_id,
        "students": len(students),
        "schools": len(school_map),
        "official_snapshots_created": created_official_snapshots,
        "snapshots_created": created_snapshots,
        "snapshots_deduped": deduped_snapshots,
        "events_created": created_events,
        "events_deduped": deduped_events,
        "outcomes_created": created_outcomes,
        "outcomes_deduped": deduped_outcomes,
        "active_outcomes": active,
        "dataset_result": dataset_result,
    }


async def _load_students(session: AsyncSession, student_ids: list[str] | None) -> list[Student]:
    stmt = select(Student)
    ids = [_as_uuid(value) for value in (student_ids or [])]
    ids = [value for value in ids if value is not None]
    if ids:
        stmt = stmt.where(Student.id.in_(ids))
    result = await session.execute(stmt.order_by(Student.created_at.asc()))
    return list(result.scalars().all())


async def _load_schools(session: AsyncSession, school_ids: list[str] | None) -> dict[str, School]:
    stmt = select(School)
    ids = [_as_uuid(value) for value in (school_ids or [])]
    ids = [value for value in ids if value is not None]
    if ids:
        stmt = stmt.where(School.id.in_(ids))
    result = await session.execute(stmt.order_by(School.name.asc()))
    schools = list(result.scalars().all())
    return {str(school.id): school for school in schools}


async def _load_school_evaluations(
    session: AsyncSession,
    student_ids: Iterable[str],
    school_ids: Iterable[str],
    enabled: bool,
) -> list[SchoolEvaluation]:
    if not enabled:
        return []
    stmt = select(SchoolEvaluation)
    student_uuid = [_as_uuid(value) for value in student_ids]
    school_uuid = [_as_uuid(value) for value in school_ids]
    student_uuid = [value for value in student_uuid if value is not None]
    school_uuid = [value for value in school_uuid if value is not None]
    if student_uuid:
        stmt = stmt.where(SchoolEvaluation.student_id.in_(student_uuid))
    if school_uuid:
        stmt = stmt.where(SchoolEvaluation.school_id.in_(school_uuid))
    result = await session.execute(stmt.order_by(SchoolEvaluation.created_at.asc()))
    return list(result.scalars().all())


async def _load_offers(
    session: AsyncSession,
    student_ids: Iterable[str],
    school_ids: Iterable[str],
    enabled: bool,
) -> list[Offer]:
    if not enabled:
        return []
    stmt = select(Offer)
    student_uuid = [_as_uuid(value) for value in student_ids]
    school_uuid = [_as_uuid(value) for value in school_ids]
    student_uuid = [value for value in student_uuid if value is not None]
    school_uuid = [value for value in school_uuid if value is not None]
    if student_uuid:
        stmt = stmt.where(Offer.student_id.in_(student_uuid))
    if school_uuid:
        stmt = stmt.where(Offer.school_id.in_(school_uuid))
    result = await session.execute(stmt.order_by(Offer.created_at.asc()))
    return list(result.scalars().all())


async def _load_admission_events(
    session: AsyncSession,
    student_ids: Iterable[str],
    school_ids: Iterable[str],
    enabled: bool,
) -> list[AdmissionEvent]:
    if not enabled:
        return []
    stmt = select(AdmissionEvent)
    student_uuid = [_as_uuid(value) for value in student_ids]
    school_uuid = [_as_uuid(value) for value in school_ids]
    student_uuid = [value for value in student_uuid if value is not None]
    school_uuid = [value for value in school_uuid if value is not None]
    if student_uuid:
        stmt = stmt.where(AdmissionEvent.student_id.in_(student_uuid))
    if school_uuid:
        stmt = stmt.where(AdmissionEvent.school_id.in_(school_uuid))
    result = await session.execute(stmt.order_by(AdmissionEvent.created_at.asc()))
    return list(result.scalars().all())


async def _load_existing_source_keys(
    session: AsyncSession,
    model,
    *,
    student_ids: Iterable[str],
    school_ids: Iterable[str],
) -> set[str]:
    stmt = select(model)
    student_uuid = [_as_uuid(value) for value in student_ids]
    school_uuid = [_as_uuid(value) for value in school_ids]
    student_uuid = [value for value in student_uuid if value is not None]
    school_uuid = [value for value in school_uuid if value is not None]
    if student_uuid:
        stmt = stmt.where(model.student_id.in_(student_uuid))
    if school_uuid:
        stmt = stmt.where(model.school_id.in_(school_uuid))
    result = await session.execute(stmt)
    rows = list(result.scalars().all())
    keys: set[str] = set()
    for row in rows:
        meta = getattr(row, "metadata_", None) or {}
        if isinstance(meta, dict):
            source_key = str(meta.get("source_key") or "").strip()
            if source_key:
                keys.add(source_key)
        if not isinstance(meta, dict) or not meta.get("source_key"):
            feature_payload = getattr(row, "feature_payload", None) or {}
            if isinstance(feature_payload, dict):
                feature_meta = feature_payload.get("metadata") or {}
                if isinstance(feature_meta, dict):
                    source_key = str(feature_meta.get("source_key") or "").strip()
                    if source_key:
                        keys.add(source_key)
    return keys


def _offer_status_to_stage(status: str) -> str | None:
    low = str(status or "").strip().lower()
    if low in {"admitted", "committed"}:
        return "admit" if low == "admitted" else "commit"
    if low in {"rejected", "denied", "declined"}:
        return "reject"
    if low == "waitlisted":
        return "waitlist"
    if low == "deferred":
        return "deferred"
    return None


def _stage_creates_truth(stage: str) -> bool:
    return str(stage or "").strip().lower() in _TRUTH_STAGES


def _normalize_import_row(
    row: dict[str, Any],
    *,
    default_cycle_year: int | None,
) -> dict[str, Any]:
    student_id = str(row.get("student_id") or "").strip() or None
    school_id = str(row.get("school_id") or "").strip() or None
    if not student_id or not school_id:
        return {}
    source_key = str(
        row.get("source_key")
        or row.get("record_id")
        or row.get("id")
        or f"{student_id}:{school_id}:{row.get('stage') or 'unknown'}:{row.get('happened_at') or ''}"
    ).strip()
    cycle_year = row.get("cycle_year")
    if cycle_year is None:
        cycle_year = default_cycle_year or datetime.now(timezone.utc).year
    happened_at = row.get("happened_at")
    if isinstance(happened_at, str) and happened_at.strip():
        try:
            happened_at = datetime.fromisoformat(happened_at.replace("Z", "+00:00"))
        except ValueError:
            happened_at = datetime.now(timezone.utc)
    elif happened_at is None:
        happened_at = datetime.now(timezone.utc)
    stage = str(row.get("stage") or "").strip().lower()
    if not stage:
        stage = "submitted"
    return {
        "student_id": student_id,
        "school_id": school_id,
        "cycle_year": int(cycle_year),
        "stage": stage,
        "happened_at": happened_at,
        "major_bucket": row.get("major_bucket"),
        "source_name": str(row.get("source_name") or "import"),
        "source_type": str(row.get("source_type") or "user_upload"),
        "source_url": row.get("source_url"),
        "content_text": row.get("content_text"),
        "evidence_ref": row.get("evidence_ref"),
        "source_key": source_key,
    }


def _as_uuid(value: str | UUID | None) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return UUID(text)
    except ValueError:
        return None
