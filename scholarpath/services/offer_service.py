"""Offer CRUD and causal-engine-powered comparison."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.causal import AdmissionDAGBuilder, NoisyORPropagator
from scholarpath.db.models import Offer, OfferStatus, School
from scholarpath.exceptions import ScholarPathError
from scholarpath.llm.client import LLMClient
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)


async def create_offer(
    session: AsyncSession,
    student_id: uuid.UUID,
    data: dict[str, Any],
) -> Offer:
    """Create an admission offer record for a student.

    Parameters
    ----------
    session:
        Active async database session.
    student_id:
        UUID of the student who received the offer.
    data:
        Offer attributes (``school_id``, ``status``, ``merit_scholarship``, etc.).
    """
    offer = Offer(student_id=student_id, **data)
    session.add(offer)
    await session.flush()
    logger.info("Created offer %s for student %s", offer.id, student_id)
    return offer


async def list_offers(
    session: AsyncSession,
    student_id: uuid.UUID,
) -> list[Offer]:
    """Return all offers for a student, ordered by creation date."""
    result = await session.execute(
        select(Offer)
        .where(Offer.student_id == student_id)
        .options(selectinload(Offer.school))
        .order_by(Offer.created_at.desc())
    )
    return list(result.scalars().all())


async def update_offer(
    session: AsyncSession,
    offer_id: uuid.UUID,
    data: dict[str, Any],
) -> Offer:
    """Update an existing offer record."""
    offer = await session.get(Offer, offer_id)
    if offer is None:
        raise ScholarPathError(f"Offer {offer_id} not found")

    for key, value in data.items():
        if hasattr(offer, key):
            setattr(offer, key, value)
    await session.flush()
    return offer


async def compare_offers(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
) -> dict[str, Any]:
    """Side-by-side comparison of admitted offers using the causal engine.

    Only offers with status ``admitted`` or ``committed`` are compared.

    Returns
    -------
    dict
        ``offers``: list of offer summaries with causal-engine scores.
        ``comparison_matrix``: dimension-by-dimension comparison table.
        ``recommendation``: LLM-generated recommendation text.
    """
    student = await get_student(session, student_id)
    offers = await list_offers(session, student_id)

    # Filter to actionable offers
    actionable = [
        o
        for o in offers
        if o.status in (OfferStatus.ADMITTED.value, OfferStatus.COMMITTED.value)
    ]
    if len(actionable) < 1:
        raise ScholarPathError("No admitted offers to compare")

    builder = AdmissionDAGBuilder()
    propagator = NoisyORPropagator()

    offer_summaries: list[dict[str, Any]] = []
    for offer in actionable:
        school = await session.get(School, offer.school_id)
        school_name = school.name if school else str(offer.school_id)

        # Build a causal DAG for outcome estimation
        student_profile = {"gpa": student.gpa, "sat": student.sat_total or 1100}
        school_data: dict[str, Any] = {}
        if school and school.acceptance_rate is not None:
            school_data["acceptance_rate"] = school.acceptance_rate
        if school and school.avg_net_price is not None:
            school_data["avg_aid"] = max(0, (school.avg_net_price or 0) - (offer.net_cost or 0))

        try:
            dag = builder.build_admission_dag(student_profile, school_data)
            dag = propagator.propagate(dag)
            career_score = dag.nodes.get("career_outcome", {}).get("propagated_belief", 0.5)
            life_score = dag.nodes.get("life_satisfaction", {}).get("propagated_belief", 0.5)
            academic_score = dag.nodes.get("academic_outcome", {}).get("propagated_belief", 0.5)
        except Exception:
            logger.warning("Causal scoring failed for offer %s", offer.id, exc_info=True)
            career_score = life_score = academic_score = 0.5

        offer_summaries.append(
            {
                "offer_id": str(offer.id),
                "school": school_name,
                "status": offer.status,
                "net_cost": offer.net_cost,
                "total_aid": offer.total_aid,
                "merit_scholarship": offer.merit_scholarship,
                "honors_program": offer.honors_program,
                "decision_deadline": str(offer.decision_deadline) if offer.decision_deadline else None,
                "causal_scores": {
                    "career_outcome": round(career_score, 3),
                    "life_satisfaction": round(life_score, 3),
                    "academic_outcome": round(academic_score, 3),
                },
            }
        )

    # Build comparison matrix
    dimensions = ["net_cost", "total_aid", "career_outcome", "life_satisfaction", "academic_outcome"]
    comparison_matrix: dict[str, dict[str, Any]] = {}
    for dim in dimensions:
        comparison_matrix[dim] = {}
        for os_ in offer_summaries:
            if dim in ("career_outcome", "life_satisfaction", "academic_outcome"):
                comparison_matrix[dim][os_["school"]] = os_["causal_scores"].get(dim)
            else:
                comparison_matrix[dim][os_["school"]] = os_.get(dim)

    # LLM recommendation
    messages = [
        {
            "role": "system",
            "content": (
                "You are a college admissions advisor. Compare the following "
                "admission offers and provide a clear recommendation. "
                "Consider financial fit, career outcomes, academic fit, and "
                "student preferences. Be concise and actionable."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Student: {student.name}, intended majors: {student.intended_majors}, "
                f"budget: ${student.budget_usd}/yr\n\n"
                f"Offers:\n{json.dumps(offer_summaries, ensure_ascii=False, indent=2)}"
            ),
        },
    ]
    recommendation = await llm.complete(messages, temperature=0.5, max_tokens=1024)

    return {
        "offers": offer_summaries,
        "comparison_matrix": comparison_matrix,
        "recommendation": recommendation,
    }
