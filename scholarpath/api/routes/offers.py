"""Offer routes -- recording and comparing admission offers."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from scholarpath.api.deps import LLMDep, SessionDep
from scholarpath.api.models.offer import (
    OfferComparisonResponse,
    OfferCreate,
    OfferResponse,
    OfferUpdate,
)
from scholarpath.db.models.offer import Offer
from scholarpath.db.models.school import School
from scholarpath.db.models.student import Student
from scholarpath.exceptions import ScholarPathError
from scholarpath.services.offer_service import (
    compare_offers as compare_offers_service,
)

router = APIRouter(prefix="/offers", tags=["offers"])


def _compute_totals(
    tuition: int | None,
    room_and_board: int | None,
    books_supplies: int | None,
    personal_expenses: int | None,
    transportation: int | None,
    merit_scholarship: int,
    need_based_grant: int,
    loan_offered: int,
    work_study: int,
    school_tuition_fallback: int | None = None,
) -> tuple[int, int | None, int | None]:
    """Return (total_aid, total_cost, net_cost)."""
    total_aid = merit_scholarship + need_based_grant + loan_offered + work_study

    cost_parts = [
        tuition if tuition is not None else school_tuition_fallback,
        room_and_board,
        books_supplies,
        personal_expenses,
        transportation,
    ]
    known = [p for p in cost_parts if p is not None]
    total_cost = sum(known) if known else None
    net_cost = total_cost - total_aid if total_cost is not None else None

    return total_aid, total_cost, net_cost


async def _require_student(session, student_id: uuid.UUID) -> Student:
    student = await session.get(Student, student_id)
    if student is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student {student_id} not found",
        )
    return student


@router.post(
    "/students/{student_id}/offers",
    response_model=OfferResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_offer(
    student_id: uuid.UUID,
    payload: OfferCreate,
    session: SessionDep,
) -> OfferResponse:
    """Record a new admission offer for a student."""
    await _require_student(session, student_id)

    school = await session.get(School, payload.school_id)
    if school is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"School {payload.school_id} not found",
        )

    total_aid, total_cost, net_cost = _compute_totals(
        tuition=payload.tuition,
        room_and_board=payload.room_and_board,
        books_supplies=payload.books_supplies,
        personal_expenses=payload.personal_expenses,
        transportation=payload.transportation,
        merit_scholarship=payload.merit_scholarship,
        need_based_grant=payload.need_based_grant,
        loan_offered=payload.loan_offered,
        work_study=payload.work_study,
        school_tuition_fallback=school.tuition_oos,
    )

    offer = Offer(
        student_id=student_id,
        school_id=payload.school_id,
        status=payload.status,
        tuition=payload.tuition,
        room_and_board=payload.room_and_board,
        books_supplies=payload.books_supplies,
        personal_expenses=payload.personal_expenses,
        transportation=payload.transportation,
        merit_scholarship=payload.merit_scholarship,
        need_based_grant=payload.need_based_grant,
        loan_offered=payload.loan_offered,
        work_study=payload.work_study,
        total_aid=total_aid,
        total_cost=total_cost,
        net_cost=net_cost,
        honors_program=payload.honors_program,
        conditions=payload.conditions,
        decision_deadline=payload.decision_deadline,
        notes=payload.notes,
    )
    session.add(offer)
    await session.flush()
    await session.refresh(offer)

    resp = OfferResponse.model_validate(offer)
    resp.school_name = school.name
    return resp


@router.get(
    "/students/{student_id}/offers",
    response_model=list[OfferResponse],
)
async def list_offers(
    student_id: uuid.UUID,
    session: SessionDep,
) -> list[OfferResponse]:
    """List all offers for a student."""
    await _require_student(session, student_id)

    stmt = (
        select(Offer, School.name)
        .join(School, Offer.school_id == School.id, isouter=True)
        .where(Offer.student_id == student_id)
        .order_by(Offer.created_at.desc())
    )
    result = await session.execute(stmt)
    rows = result.all()
    responses = []
    for offer, school_name in rows:
        resp = OfferResponse.model_validate(offer)
        resp.school_name = school_name
        responses.append(resp)
    return responses


@router.put("/{offer_id}", response_model=OfferResponse)
async def update_offer(
    offer_id: uuid.UUID,
    payload: OfferUpdate,
    session: SessionDep,
) -> OfferResponse:
    """Update an existing offer."""
    offer = await session.get(Offer, offer_id)
    if offer is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Offer {offer_id} not found",
        )

    update_data = payload.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(offer, field, value)

    # Recalculate totals if any cost or aid field changed
    cost_or_aid_fields = {
        "tuition", "room_and_board", "books_supplies", "personal_expenses",
        "transportation", "merit_scholarship", "need_based_grant",
        "loan_offered", "work_study",
    }
    if cost_or_aid_fields & update_data.keys():
        offer.total_aid, offer.total_cost, offer.net_cost = _compute_totals(
            tuition=offer.tuition,
            room_and_board=offer.room_and_board,
            books_supplies=offer.books_supplies,
            personal_expenses=offer.personal_expenses,
            transportation=offer.transportation,
            merit_scholarship=offer.merit_scholarship,
            need_based_grant=offer.need_based_grant,
            loan_offered=offer.loan_offered,
            work_study=offer.work_study,
        )

    await session.flush()
    await session.refresh(offer)
    resp = OfferResponse.model_validate(offer)
    return resp


@router.get(
    "/students/{student_id}/offers/compare",
    response_model=OfferComparisonResponse,
)
async def compare_offers(
    student_id: uuid.UUID,
    session: SessionDep,
    llm: LLMDep,
) -> OfferComparisonResponse:
    """Compare all admitted offers side-by-side."""
    await _require_student(session, student_id)

    stmt = (
        select(Offer, School.name)
        .join(School, Offer.school_id == School.id, isouter=True)
        .where(Offer.student_id == student_id, Offer.status == "admitted")
        .order_by(Offer.net_cost.asc().nulls_last())
    )
    result = await session.execute(stmt)
    rows = result.all()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No admitted offers found for comparison",
        )

    offer_responses = []
    for offer, school_name in rows:
        resp = OfferResponse.model_validate(offer)
        resp.school_name = school_name
        offer_responses.append(resp)

    try:
        comparison = await compare_offers_service(
            session=session,
            llm=llm,
            student_id=student_id,
        )
    except ScholarPathError as exc:
        detail = str(exc)
        status_code = (
            status.HTTP_404_NOT_FOUND
            if "not found" in detail.lower()
            else status.HTTP_400_BAD_REQUEST
        )
        raise HTTPException(status_code=status_code, detail=detail) from exc

    comparison_scores = list(comparison.get("offers") or [])
    matrix = comparison.get("comparison_matrix")
    causal_matrix = matrix if isinstance(matrix, dict) else {}

    causal_engine_version = None
    causal_model_version = None
    estimate_confidence_values: list[float] = []
    fallback_used = False
    for score_row in comparison_scores:
        if not isinstance(score_row, dict):
            continue
        meta = score_row.get("causal_meta")
        if not isinstance(meta, dict):
            continue
        if causal_engine_version is None and meta.get("causal_engine_version"):
            causal_engine_version = str(meta.get("causal_engine_version"))
        if causal_model_version is None and meta.get("causal_model_version"):
            causal_model_version = str(meta.get("causal_model_version"))
        if meta.get("fallback_used") is True:
            fallback_used = True
        confidence_raw = meta.get("estimate_confidence")
        if isinstance(confidence_raw, (int, float)):
            estimate_confidence_values.append(float(confidence_raw))

    avg_confidence = (
        round(sum(estimate_confidence_values) / len(estimate_confidence_values), 4)
        if estimate_confidence_values
        else None
    )

    return OfferComparisonResponse(
        offers=offer_responses,
        comparison_scores=comparison_scores,
        causal_comparison_matrix=causal_matrix,
        recommendation=(
            str(comparison.get("recommendation"))
            if comparison.get("recommendation") is not None
            else None
        ),
        causal_engine_version=causal_engine_version,
        causal_model_version=causal_model_version,
        estimate_confidence=avg_confidence,
        fallback_used=fallback_used,
    )
