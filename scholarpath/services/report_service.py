"""Go/No-Go report generation service."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.causal import (
    AdmissionDAGBuilder,
    NoisyORPropagator,
)
from scholarpath.db.models import (
    GoNoGoReport,
    Offer,
    Recommendation,
    School,
)
from scholarpath.exceptions import ScholarPathError
from scholarpath.llm.client import LLMClient
from scholarpath.llm.prompts import GO_NO_GO_PROMPT, format_go_no_go
from scholarpath.services.evaluation_service import evaluate_school_fit
from scholarpath.services.portfolio_service import (
    get_student_canonical_preferences,
    get_student_sat_equivalent,
)
from scholarpath.services.simulation_service import run_what_if
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)


async def generate_go_no_go(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    offer_id: uuid.UUID,
) -> GoNoGoReport:
    """Generate a comprehensive Go/No-Go report for a specific offer.

    Pipeline
    --------
    1. Load student + offer + school data.
    2. Build a causal graph for the student-school pair.
    3. Run the multi-dimensional evaluation.
    4. Run automatic what-if scenarios:
       - Aid appeal: ``financial_aid = 0.9``
       - Major change: swap intended major signal
       - Location weight zeroed: ``location_effect = 0.0``
    5. Compute a Go/No-Go score via Noisy-OR propagation.
    6. Generate a narrative explanation via LLM.
    7. Persist and return the report.

    Returns
    -------
    GoNoGoReport
        The persisted report record.
    """
    student = await get_student(session, student_id)

    offer = await session.get(Offer, offer_id)
    if offer is None:
        raise ScholarPathError(f"Offer {offer_id} not found")
    if offer.student_id != student_id:
        raise ScholarPathError("Offer does not belong to this student")

    school = await session.get(School, offer.school_id)
    if school is None:
        raise ScholarPathError(f"School for offer {offer_id} not found")

    # --- Step 2-3: Evaluation ---
    evaluation = await evaluate_school_fit(session, llm, student_id, offer.school_id)

    # --- Step 4: Automatic what-if scenarios ---
    what_if_results: dict[str, Any] = {}

    # 4a. Aid appeal
    try:
        what_if_results["aid_appeal"] = await run_what_if(
            session, llm, student_id, offer.school_id,
            {"financial_aid": 0.9},
        )
    except Exception:
        logger.warning("Aid appeal what-if failed", exc_info=True)
        what_if_results["aid_appeal"] = {"error": "simulation failed"}

    # 4b. Major change (boost research opportunities as proxy)
    try:
        what_if_results["major_change"] = await run_what_if(
            session, llm, student_id, offer.school_id,
            {"research_opportunities": 0.8},
        )
    except Exception:
        logger.warning("Major change what-if failed", exc_info=True)
        what_if_results["major_change"] = {"error": "simulation failed"}

    # 4c. Location weight = 0
    try:
        what_if_results["ignore_location"] = await run_what_if(
            session, llm, student_id, offer.school_id,
            {"location_effect": 0.0},
        )
    except Exception:
        logger.warning("Location what-if failed", exc_info=True)
        what_if_results["ignore_location"] = {"error": "simulation failed"}

    # --- Step 5: Go/No-Go score ---
    builder = AdmissionDAGBuilder()
    propagator = NoisyORPropagator()
    student_profile = {"gpa": student.gpa, "sat": get_student_sat_equivalent(student)}
    school_data: dict[str, Any] = {}
    if school.acceptance_rate is not None:
        school_data["acceptance_rate"] = school.acceptance_rate

    dag = builder.build_admission_dag(student_profile, school_data)
    dag = propagator.propagate(dag)

    ci = propagator.compute_confidence_intervals(dag, n_samples=500)

    # Overall Go/No-Go score = weighted Noisy-OR of the four dimension scores
    academic_score = evaluation.academic_fit
    financial_score = evaluation.financial_fit
    career_score = evaluation.career_fit
    life_score = evaluation.life_fit
    overall_score = evaluation.overall_score

    # Confidence bounds from the causal CI on key outcome nodes
    career_ci = ci.get("career_outcome", {"ci_lower": 0.3, "ci_upper": 0.7})
    confidence_lower = round(max(0.0, overall_score - 0.15, career_ci["ci_lower"]), 4)
    confidence_upper = round(min(1.0, overall_score + 0.15, career_ci["ci_upper"] + 0.2), 4)

    # Determine recommendation
    recommendation = _score_to_recommendation(overall_score)

    # Top factors and risks
    top_factors = _identify_top_factors(evaluation)
    risks = _identify_risks(evaluation, offer, school)

    # --- Step 6: LLM narrative ---
    offer_details = {
        "student": student.name,
        "school": school.name,
        "net_cost": offer.net_cost,
        "total_aid": offer.total_aid,
        "merit_scholarship": offer.merit_scholarship,
        "honors_program": offer.honors_program,
        "decision_deadline": str(offer.decision_deadline) if offer.decision_deadline else None,
        "preferences": get_student_canonical_preferences(student),
    }
    fit_scores = {
        "academic_fit": academic_score,
        "financial_fit": financial_score,
        "career_fit": career_score,
        "life_fit": life_score,
        "overall_score": overall_score,
    }
    causal_summary = {
        "recommendation": recommendation,
        "top_factors": top_factors,
        "risks": risks,
    }
    what_if_list = [
        {"scenario": k, **(v if isinstance(v, dict) else {"error": str(v)})}
        for k, v in what_if_results.items()
    ]
    user_prompt = format_go_no_go(offer_details, fit_scores, causal_summary, what_if_list)
    messages = [
        {"role": "system", "content": GO_NO_GO_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    narrative = await llm.complete(messages, temperature=0.5, max_tokens=2048)

    # --- Step 7: Persist ---
    report = GoNoGoReport(
        student_id=student_id,
        offer_id=offer_id,
        overall_score=round(overall_score, 4),
        confidence_lower=confidence_lower,
        confidence_upper=confidence_upper,
        academic_score=round(academic_score, 4),
        financial_score=round(financial_score, 4),
        career_score=round(career_score, 4),
        life_score=round(life_score, 4),
        recommendation=recommendation,
        top_factors=top_factors,
        risks=risks,
        what_if_results=what_if_results,
        narrative=narrative,
    )
    session.add(report)
    await session.flush()

    logger.info(
        "Generated Go/No-Go report %s for student %s / offer %s: %s (%.2f)",
        report.id,
        student_id,
        offer_id,
        recommendation,
        overall_score,
    )
    return report


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _score_to_recommendation(overall_score: float) -> str:
    """Map the overall score to a recommendation label."""
    if overall_score >= 0.80:
        return Recommendation.STRONGLY_RECOMMEND.value
    if overall_score >= 0.60:
        return Recommendation.RECOMMEND.value
    if overall_score >= 0.40:
        return Recommendation.NEUTRAL.value
    return Recommendation.NOT_RECOMMEND.value


def _identify_top_factors(evaluation: Any) -> list[str]:
    """Return the top positive factors driving the evaluation."""
    factors: list[tuple[str, float]] = [
        ("Strong academic fit", evaluation.academic_fit),
        ("Good financial fit", evaluation.financial_fit),
        ("Career outcome alignment", evaluation.career_fit),
        ("Lifestyle / community match", evaluation.life_fit),
    ]
    factors.sort(key=lambda x: x[1], reverse=True)
    return [f for f, score in factors if score >= 0.5]


def _identify_risks(evaluation: Any, offer: Offer, school: School) -> list[str]:
    """Return a list of risk factors."""
    risks: list[str] = []

    if evaluation.financial_fit < 0.4:
        risks.append("Financial fit is low -- net cost may exceed budget.")
    if evaluation.academic_fit < 0.4:
        risks.append("Academic mismatch -- student profile may not align with school expectations.")
    if offer.net_cost and offer.net_cost > 60_000:
        risks.append(f"High net cost (${offer.net_cost:,}/yr) could cause debt burden.")
    if school.acceptance_rate and school.acceptance_rate < 0.15:
        risks.append("Highly selective school -- limited margin for error in application.")
    if evaluation.life_fit < 0.4:
        risks.append("Location or campus environment may not match preferences.")

    return risks
