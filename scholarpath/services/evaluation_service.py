"""School evaluation, tiering, and application strategy services."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from scholarpath.language import ResponseLanguage, language_instruction
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.causal import AdmissionDAGBuilder, NoisyORPropagator
from scholarpath.db.models import CareerOutcomeProxy, School, SchoolEvaluation, Student, Tier
from scholarpath.db.models.metro_area import MetroAreaProfile
from scholarpath.exceptions import ScholarPathError
from scholarpath.llm.client import LLMClient
from scholarpath.llm.prompts import (
    SCHOOL_EVALUATION_PROMPT,
    STRATEGY_ADVICE_PROMPT,
    format_school_evaluation,
    format_strategy_advice,
)
from scholarpath.services.portfolio_service import (
    get_student_canonical_preferences,
    get_student_sat_equivalent,
)
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)

# Weight vectors per degree level for overall score computation.
_WEIGHTS_BY_DEGREE: dict[str, dict[str, float]] = {
    "undergraduate": {"academic_fit": 0.30, "financial_fit": 0.25, "career_fit": 0.25, "life_fit": 0.20},
    "masters":       {"academic_fit": 0.25, "financial_fit": 0.25, "career_fit": 0.30, "life_fit": 0.20},
    "phd":           {"academic_fit": 0.20, "financial_fit": 0.15, "career_fit": 0.40, "life_fit": 0.25},
}


def _get_dimension_weights(degree_level: str) -> dict[str, float]:
    return _WEIGHTS_BY_DEGREE.get(degree_level, _WEIGHTS_BY_DEGREE["undergraduate"])


async def evaluate_school_fit(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    school_id: uuid.UUID,
) -> SchoolEvaluation:
    """Compute a multi-dimensional fit evaluation for one student-school pair.

    Scoring dimensions
    ------------------
    - **academic_fit**: SAT/GPA match against school ranges, program strength,
      curriculum alignment.
    - **financial_fit**: student budget vs. estimated net cost, aid availability,
      endowment per student.
    - **career_fit**: career outcome proxies, internship pipeline, OPT/H1B
      friendliness for international students.
    - **life_fit**: location / climate match, campus safety, community and
      student-body composition preferences.

    The *overall_score* is a weighted average of the four dimensions.

    Tier assignment follows admission probability thresholds:
        reach  (< 20%), target (20-60%), safety (60-85%), likely (> 85%).

    The LLM generates a free-text *reasoning* narrative explaining the scores.

    Returns
    -------
    SchoolEvaluation
        The persisted evaluation record.
    """
    student = await get_student(session, student_id)
    school = await session.get(
        School,
        school_id,
        options=[selectinload(School.programs), selectinload(School.metro_area)],
    )
    if school is None:
        raise ScholarPathError(f"School {school_id} not found")

    # Load career outcome proxies for this school
    proxy_result = await session.execute(
        select(CareerOutcomeProxy).where(CareerOutcomeProxy.school_id == school_id)
    )
    career_proxies = list(proxy_result.scalars().all())

    # --- Compute dimension scores ---
    academic = _compute_academic_fit(student, school)
    financial = _compute_financial_fit(student, school)
    career, orientation_scores = _compute_career_fit(
        student, school, school.metro_area, career_proxies,
    )
    life = _compute_life_fit(student, school)

    weights = _get_dimension_weights(getattr(student, "degree_level", "undergraduate"))
    overall = (
        weights["academic_fit"] * academic
        + weights["financial_fit"] * financial
        + weights["career_fit"] * career
        + weights["life_fit"] * life
    )

    # --- Admission probability via causal DAG ---
    admission_prob = _estimate_admission_probability(student, school)

    # --- Tier ---
    tier = _assign_tier(admission_prob)

    # --- LLM reasoning ---
    student_profile_dict = {
        "student": student.name,
        "gpa": student.gpa,
        "sat": student.sat_total,
        "intended_majors": student.intended_majors,
        "budget": student.budget_usd,
    }
    school_data_dict = {
        "school": school.name,
        "acceptance_rate": school.acceptance_rate,
        "sat_range": f"{school.sat_25}-{school.sat_75}",
        "net_price": school.avg_net_price,
        "rank": school.us_news_rank,
        "computed_scores": {
            "academic": round(academic, 2),
            "financial": round(financial, 2),
            "career": round(career, 2),
            "life": round(life, 2),
            "overall": round(overall, 2),
            "admission_probability": round(admission_prob, 2),
            "tier": tier,
        },
    }
    user_prompt = format_school_evaluation(student_profile_dict, school_data_dict)
    messages = [
        {"role": "system", "content": SCHOOL_EVALUATION_PROMPT},
        {"role": "user", "content": user_prompt},
    ]
    reasoning = await llm.complete(messages, temperature=0.5, max_tokens=1024)

    # --- Persist ---
    evaluation = SchoolEvaluation(
        student_id=student_id,
        school_id=school_id,
        tier=tier,
        academic_fit=round(academic, 4),
        financial_fit=round(financial, 4),
        career_fit=round(career, 4),
        life_fit=round(life, 4),
        overall_score=round(overall, 4),
        admission_probability=round(admission_prob, 4),
        reasoning=reasoning,
        fit_details={
            "weights": weights,
            "raw_scores": {
                "academic_fit": academic,
                "financial_fit": financial,
                "career_fit": career,
                "life_fit": life,
            },
            "orientation_scores": {
                k: {"score": round(v.score, 4), "l1": round(v.layer1.value, 4), "l2": round(v.layer2.value, 4), "l3": round(v.layer3.value, 4)}
                for k, v in orientation_scores.items()
            },
            "metro_signals": _extract_metro_signals(school.metro_area),
        },
    )
    session.add(evaluation)
    await session.flush()

    logger.info(
        "Evaluated %s for student %s: overall=%.2f tier=%s",
        school.name,
        student_id,
        overall,
        tier,
    )
    return evaluation


async def get_tiered_list(
    session: AsyncSession,
    student_id: uuid.UUID,
) -> dict[str, list[SchoolEvaluation]]:
    """Group a student's school evaluations by tier.

    Returns
    -------
    dict
        Keys are tier names (``reach``, ``target``, ``safety``, ``likely``),
        values are lists of :class:`SchoolEvaluation` sorted by overall score
        descending.
    """
    result = await session.execute(
        select(SchoolEvaluation)
        .where(SchoolEvaluation.student_id == student_id)
        .options(selectinload(SchoolEvaluation.school))
        .order_by(SchoolEvaluation.overall_score.desc())
    )
    evaluations = list(result.scalars().all())

    # Deduplicate: keep only the highest-scored evaluation per school
    seen_schools: set[uuid.UUID] = set()
    unique_evaluations: list[SchoolEvaluation] = []
    for ev in evaluations:
        if ev.school_id not in seen_schools:
            seen_schools.add(ev.school_id)
            unique_evaluations.append(ev)

    tiered: dict[str, list[SchoolEvaluation]] = {
        Tier.REACH.value: [],
        Tier.TARGET.value: [],
        Tier.SAFETY.value: [],
        Tier.LIKELY.value: [],
    }
    for ev in unique_evaluations:
        bucket = tiered.get(ev.tier)
        if bucket is not None:
            bucket.append(ev)
        else:
            tiered.setdefault(ev.tier, []).append(ev)

    return tiered


async def generate_strategy(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    response_language: ResponseLanguage = "en",
    *,
    max_tokens: int = 640,
    per_tier_limit: int = 6,
) -> dict[str, Any]:
    """Generate an ED/EA/RD application strategy recommendation.

    Uses the student's tiered list and the LLM to produce actionable advice
    on Early Decision, Early Action, and Regular Decision planning.

    Returns
    -------
    dict
        ``ed_recommendation``: suggested ED school and rationale.
        ``ea_recommendations``: list of EA suggestions.
        ``rd_recommendations``: list of RD suggestions.
        ``risk_analysis``: overall risk assessment narrative.
        ``timeline``: recommended application timeline.
    """
    student = await get_student(session, student_id)
    tiered = await get_tiered_list(session, student_id)

    profile_summary = json.dumps(
        {
            "name": student.name,
            "gpa": student.gpa,
            "sat_total": student.sat_total,
            "intended_majors": student.intended_majors,
            "ed_preference": student.ed_preference,
            "target_year": student.target_year,
        },
        ensure_ascii=False,
    )

    student_profile_data = json.loads(profile_summary)
    tiered_schools_data: dict[str, list[dict[str, Any]]] = {}
    for tier_name, evals in tiered.items():
        limited = evals[: max(1, per_tier_limit)]
        tiered_schools_data[tier_name] = [
            {
                "name": ev.school.name if ev.school else str(ev.school_id),
                "composite_score": ev.overall_score,
                "admission_probability": ev.admission_probability,
            }
            for ev in limited
        ]
    user_prompt = format_strategy_advice(student_profile_data, tiered_schools_data)

    messages = [
        {
            "role": "system",
            "content": (
                f"{STRATEGY_ADVICE_PROMPT.strip()}\n\n"
                f"Additional language rule: {language_instruction(response_language)}"
            ),
        },
        {"role": "user", "content": user_prompt},
    ]

    result = await llm.complete_json(
        messages,
        schema={
            "ed_recommendation": {
                "school": "string",
                "rationale": "string",
            },
            "ea_recommendations": [
                {"school": "string", "rationale": "string"}
            ],
            "rd_recommendations": [
                {"school": "string", "rationale": "string"}
            ],
            "risk_analysis": "string",
            "timeline": "string",
        },
        temperature=0.4,
        max_tokens=max_tokens,
        caller="chat.strategy_json",
    )

    logger.info("Generated strategy for student %s", student_id)
    return result


# ---------------------------------------------------------------------------
# Internal scoring helpers
# ---------------------------------------------------------------------------

def _compute_academic_fit(student: Student, school: School) -> float:
    """Score academic fit (0-1) based on test scores and GPA alignment."""
    score = 0.5  # default when data is missing

    # SAT comparison
    if student.sat_total and school.sat_25 and school.sat_75:
        midpoint = (school.sat_25 + school.sat_75) / 2
        # How far above/below the midpoint the student is, normalised
        diff = (student.sat_total - midpoint) / max(school.sat_75 - school.sat_25, 1)
        # Sigmoid-style squish to [0, 1]
        score = max(0.0, min(1.0, 0.5 + diff * 0.4))

    # GPA bonus: assume 4.0 scale for simplicity
    if student.gpa:
        gpa_norm = min(student.gpa / 4.0, 1.0)
        score = 0.6 * score + 0.4 * gpa_norm

    return score


def _compute_financial_fit(student: Student, school: School) -> float:
    """Score financial fit (0-1): budget vs. net cost."""
    if not school.avg_net_price:
        return 0.5

    budget = student.budget_usd or 0
    if budget <= 0:
        return 0.3  # unknown budget is a mild negative signal

    ratio = budget / school.avg_net_price
    # ratio >= 1.5 -> great fit (1.0), ratio ~1.0 -> decent (0.7), ratio < 0.5 -> poor
    return max(0.0, min(1.0, ratio * 0.6 + 0.1))


def _compute_career_fit(
    student: Student,
    school: School,
    metro: MetroAreaProfile | None,
    proxies: list[CareerOutcomeProxy],
) -> tuple[float, dict]:
    """Score career fit (0-1) using the three-layer orientation scoring engine.

    Returns (career_fit_score, orientation_scores_dict).
    """
    from scholarpath.services.career_orientation import (
        CareerOrientation,
        compute_all_orientations,
    )

    scorecard_earnings: int | None = None
    if school.metadata_:
        scorecard_earnings = school.metadata_.get("scorecard_earnings")

    orientation_results = compute_all_orientations(
        school=school,
        programs=list(school.programs) if school.programs else [],
        metro=metro,
        proxies=proxies,
        scorecard_earnings=scorecard_earnings,
    )

    # If the student has a career_goal preference, weight it more heavily
    prefs = get_student_canonical_preferences(student)
    career_goal = prefs.get("career_goal")

    if career_goal and career_goal in orientation_results:
        primary = orientation_results[career_goal].score
        others = [v.score for k, v in orientation_results.items() if k != career_goal]
        others_avg = sum(others) / max(1, len(others))
        score = 0.60 * primary + 0.40 * others_avg
    else:
        score = sum(v.score for v in orientation_results.values()) / max(1, len(orientation_results))

    return max(0.0, min(1.0, score)), orientation_results


def _compute_life_fit(student: Student, school: School) -> float:
    """Score life fit (0-1): location, campus, community, and metro signals."""
    score = 0.5

    prefs = get_student_canonical_preferences(student)
    raw_locations = prefs.get("location")
    locations: list[str]
    if isinstance(raw_locations, list):
        locations = [str(v).lower() for v in raw_locations if str(v).strip()]
    elif isinstance(raw_locations, str):
        locations = [raw_locations.lower()]
    else:
        locations = []

    for location in locations:
        if school.state and location in school.state.lower():
            score += 0.2
            break
        if school.city and location in school.city.lower():
            score += 0.2
            break

    raw_sizes = prefs.get("size")
    sizes: list[str]
    if isinstance(raw_sizes, list):
        sizes = [str(v).lower() for v in raw_sizes if str(v).strip()]
    elif isinstance(raw_sizes, str):
        sizes = [raw_sizes.lower()]
    else:
        sizes = []

    for size in sizes:
        if school.size_category and size in school.size_category.lower():
            score += 0.15
            break

    # Metro area signals
    metro = school.metro_area if hasattr(school, "metro_area") else None
    if metro:
        # Cost of living alignment with budget
        if metro.cost_of_living_index and student.budget_usd:
            col_score = max(0.0, min(1.0, 1.0 - (metro.cost_of_living_index - 80) / 80))
            score = 0.75 * score + 0.25 * col_score

        # Safety
        if metro.safety_index:
            score = 0.85 * score + 0.15 * metro.safety_index

        # Cultural fit for international students
        citizenship = getattr(student, "citizenship", None)
        if citizenship and citizenship != "US" and metro.asian_population_pct:
            cultural_score = min(1.0, metro.asian_population_pct / 15.0)
            score = 0.85 * score + 0.15 * cultural_score

    return max(0.0, min(1.0, score))


def _extract_metro_signals(metro: MetroAreaProfile | None) -> dict | None:
    """Extract metro signals for fit_details JSON."""
    if not metro:
        return None
    return {
        "city": metro.city,
        "state": metro.state,
        "cost_of_living_index": metro.cost_of_living_index,
        "safety_index": metro.safety_index,
        "tech_employer_count": metro.tech_employer_count,
        "asian_population_pct": metro.asian_population_pct,
        "finance_hub_distance_km": metro.finance_hub_distance_km,
        "nsf_funding_total": metro.nsf_funding_total,
    }


def _estimate_admission_probability(student: Student, school: School) -> float:
    """Estimate admission probability using the causal DAG and Noisy-OR."""
    try:
        builder = AdmissionDAGBuilder()
        student_profile = {
            "gpa": student.gpa,
            "sat": get_student_sat_equivalent(student),
        }
        school_data: dict[str, Any] = {}
        if school.acceptance_rate is not None:
            school_data["acceptance_rate"] = school.acceptance_rate

        dag = builder.build_admission_dag(student_profile, school_data)
        propagator = NoisyORPropagator()
        dag = propagator.propagate(dag)

        prob = dag.nodes.get("admission_probability", {}).get(
            "propagated_belief", 0.3
        )
        return float(max(0.0, min(1.0, prob)))
    except Exception:
        logger.warning(
            "Causal DAG estimation failed; falling back to heuristic.",
            exc_info=True,
        )
        # Simple heuristic fallback
        if school.acceptance_rate is not None:
            return float(school.acceptance_rate)
        return 0.3


def _assign_tier(admission_probability: float) -> str:
    """Map admission probability to a tier label."""
    if admission_probability > 0.85:
        return Tier.LIKELY.value
    if admission_probability > 0.60:
        return Tier.SAFETY.value
    if admission_probability >= 0.20:
        return Tier.TARGET.value
    return Tier.REACH.value
