"""Recommendation service -- causal-engine-based school recommendations.

Combines vector similarity search, causal DAG analysis, mediation
decomposition, and Go/No-Go scoring to generate personalized,
explainable school recommendations.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.causal import (
    AdmissionDAGBuilder,
    GoNoGoScorer,
    MediationAnalyzer,
    NoisyORPropagator,
)
from scholarpath.db.models import School, SchoolEvaluation, Tier
from scholarpath.llm.client import LLMClient
from scholarpath.llm.embeddings import get_embedding_service
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)

# Tier thresholds (admission probability based)
_TIER_THRESHOLDS = [
    (0.85, Tier.LIKELY),
    (0.60, Tier.SAFETY),
    (0.20, Tier.TARGET),
    (0.00, Tier.REACH),
]


def _assign_tier(admission_prob: float) -> str:
    """Map admission probability to tier label."""
    for threshold, tier in _TIER_THRESHOLDS:
        if admission_prob >= threshold:
            return tier.value
    return Tier.REACH.value


async def generate_recommendations(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
) -> dict[str, Any]:
    """Generate personalized school recommendations using causal engine + vector similarity.

    Pipeline
    --------
    1. Load student profile + preferences.
    2. Vector similarity search: find top 15 matching schools from pgvector.
    3. For each candidate school, build causal DAG and compute fit scores.
    4. Use mediation analysis to explain WHY each school is recommended.
    5. Tier schools into reach/target/safety/likely.
    6. Generate LLM narrative summary.

    Returns
    -------
    dict
        - ``schools``: list of dicts with school_name, tier, overall_score,
          sub_scores, key_reasons, causal_pathways
        - ``strategy``: ED/EA/RD recommendations
        - ``narrative``: natural language summary
    """
    student = await get_student(session, student_id)
    preferences = student.preferences or {}

    # ------------------------------------------------------------------
    # Step 1: Vector similarity search for candidate schools
    # ------------------------------------------------------------------
    candidate_schools: list[School] = []

    if student.profile_embedding is not None:
        try:
            from sqlalchemy import literal_column, cast as sa_cast
            from pgvector.sqlalchemy import Vector as PgVector

            vec_str = "[" + ",".join(str(v) for v in student.profile_embedding) + "]"
            vec_param = sa_cast(literal_column(f"'{vec_str}'"), PgVector(len(student.profile_embedding)))
            vector_stmt = (
                select(School)
                .where(School.embedding.isnot(None))
                .order_by(School.embedding.cosine_distance(vec_param))
                .limit(15)
            )
            result = await session.execute(vector_stmt)
            candidate_schools = list(result.scalars().all())
            logger.info(
                "Vector search found %d candidate schools for student %s",
                len(candidate_schools),
                student_id,
            )
        except Exception:
            logger.warning(
                "Vector similarity search failed; falling back to rank-based selection",
                exc_info=True,
            )

    # Fallback: if no candidates from vector search, use top-ranked schools
    if not candidate_schools:
        fallback_stmt = (
            select(School)
            .order_by(School.us_news_rank.asc().nullslast())
            .limit(15)
        )
        result = await session.execute(fallback_stmt)
        candidate_schools = list(result.scalars().all())

    # ------------------------------------------------------------------
    # Step 2: Build causal DAGs and compute scores per school
    # ------------------------------------------------------------------
    builder = AdmissionDAGBuilder()
    propagator = NoisyORPropagator()
    mediator = MediationAnalyzer(propagator)
    scorer = GoNoGoScorer(propagator)

    student_profile_for_dag = {
        "gpa": student.gpa or 3.0,
        "sat": student.sat_total or 1100,
    }
    if student.budget_usd:
        student_profile_for_dag["family_income"] = student.budget_usd * 2  # rough proxy

    school_results: list[dict[str, Any]] = []

    for school in candidate_schools:
        try:
            school_result = _evaluate_school_with_causal(
                student=student,
                student_profile_for_dag=student_profile_for_dag,
                school=school,
                preferences=preferences,
                builder=builder,
                propagator=propagator,
                mediator=mediator,
                scorer=scorer,
            )
            school_results.append(school_result)
        except Exception:
            logger.warning(
                "Failed to evaluate school %s for student %s",
                school.name,
                student_id,
                exc_info=True,
            )

    # Sort by overall_score descending
    school_results.sort(key=lambda x: x["overall_score"], reverse=True)

    # ------------------------------------------------------------------
    # Step 3: Persist SchoolEvaluation records
    # ------------------------------------------------------------------
    for sr in school_results:
        try:
            evaluation = SchoolEvaluation(
                student_id=student_id,
                school_id=sr["school_id"],
                tier=sr["tier"],
                academic_fit=round(sr["sub_scores"].get("academic", 0.5), 4),
                financial_fit=round(sr["sub_scores"].get("financial", 0.5), 4),
                career_fit=round(sr["sub_scores"].get("career", 0.5), 4),
                life_fit=round(sr["sub_scores"].get("life", 0.5), 4),
                overall_score=round(sr["overall_score"], 4),
                admission_probability=round(sr.get("admission_probability", 0.3), 4),
                reasoning="; ".join(sr.get("key_reasons", [])[:3]),
                fit_details={
                    "causal_pathways": sr.get("causal_pathways", []),
                    "go_no_go_tier": sr.get("go_no_go_tier", "neutral"),
                },
            )
            session.add(evaluation)
        except Exception:
            logger.warning(
                "Failed to persist evaluation for school %s",
                sr.get("school_name"),
                exc_info=True,
            )

    await session.flush()

    # ------------------------------------------------------------------
    # Step 4: Generate strategy recommendation
    # ------------------------------------------------------------------
    strategy = _build_strategy(school_results, student, preferences)

    # ------------------------------------------------------------------
    # Step 5: Generate LLM narrative summary
    # ------------------------------------------------------------------
    narrative = await _generate_narrative(llm, student, school_results, strategy)

    return {
        "schools": school_results,
        "strategy": strategy,
        "narrative": narrative,
    }


def _evaluate_school_with_causal(
    *,
    student: Any,
    student_profile_for_dag: dict[str, Any],
    school: School,
    preferences: dict[str, Any],
    builder: AdmissionDAGBuilder,
    propagator: NoisyORPropagator,
    mediator: MediationAnalyzer,
    scorer: GoNoGoScorer,
) -> dict[str, Any]:
    """Run the full causal evaluation pipeline for one student-school pair."""

    # Build school data for the DAG
    school_data: dict[str, Any] = {}
    if school.acceptance_rate is not None:
        school_data["acceptance_rate"] = school.acceptance_rate
    if school.avg_net_price is not None:
        school_data["avg_aid"] = max(0, (school.tuition_oos or 60000) - school.avg_net_price)
    if school.endowment_per_student is not None:
        school_data["research_expenditure"] = school.endowment_per_student * 50  # rough proxy
    if school.campus_setting:
        setting_map = {"urban": 4, "suburban": 3, "rural": 2}
        school_data["location_tier"] = setting_map.get(school.campus_setting.lower(), 3)

    # Build and propagate the causal DAG
    dag = builder.build_admission_dag(student_profile_for_dag, school_data)
    dag = propagator.propagate(dag)

    # Get admission probability from the DAG
    admission_prob = dag.nodes.get("admission_probability", {}).get(
        "propagated_belief", 0.3
    )
    admission_prob = float(max(0.0, min(1.0, admission_prob)))

    # Compute Go/No-Go score
    # Adjust weights based on student preferences
    weights = _adjust_weights_for_preferences(preferences)
    go_no_go = scorer.compute_score({}, dag, weights=weights)

    # Mediation analysis: explain the key causal pathways
    causal_pathways: list[dict[str, Any]] = []
    key_reasons: list[str] = []

    try:
        # Analyze pathway from school selectivity to career outcome
        pathways = mediator.decompose_pathways(
            dag, "school_selectivity", "career_outcome", max_length=4
        )
        for p in pathways[:3]:
            causal_pathways.append({
                "path": " -> ".join(p["path"]),
                "effect": round(p["effect"], 3),
                "percentage": round(p["percentage"], 1),
                "mechanism": p["mechanism"],
            })
    except Exception:
        logger.debug("Mediation analysis failed for %s", school.name)

    # Generate key reasons from Go/No-Go factors
    try:
        factors = scorer.generate_key_factors(go_no_go, dag)
        for f in factors[:3]:
            direction = "+" if f["direction"] == "positive" else "-"
            key_reasons.append(
                f"{direction} {f['label']}: {f['belief']:.0%}"
            )
    except Exception:
        pass

    # Additional reason based on financial fit
    if student.budget_usd and school.avg_net_price:
        if student.budget_usd >= school.avg_net_price:
            key_reasons.append("+ Budget covers estimated net cost")
        else:
            key_reasons.append("- Net cost may exceed budget")

    tier = _assign_tier(admission_prob)

    return {
        "school_id": school.id,
        "school_name": school.name,
        "school_name_cn": school.name_cn,
        "tier": tier,
        "overall_score": go_no_go["overall_score"],
        "admission_probability": admission_prob,
        "sub_scores": go_no_go["sub_scores"],
        "go_no_go_tier": go_no_go["tier"],
        "key_reasons": key_reasons,
        "causal_pathways": causal_pathways,
        "confidence_interval": go_no_go.get("confidence_interval", {}),
        "school_info": {
            "rank": school.us_news_rank,
            "acceptance_rate": school.acceptance_rate,
            "location": f"{school.city}, {school.state}",
            "type": school.school_type,
            "avg_net_price": school.avg_net_price,
        },
    }


def _adjust_weights_for_preferences(preferences: dict[str, Any]) -> dict[str, float]:
    """Adjust Go/No-Go dimension weights based on student preferences."""
    weights = {
        "academic": 0.30,
        "financial": 0.25,
        "career": 0.25,
        "life": 0.20,
    }

    career_goal = preferences.get("career_goal", "")
    if isinstance(career_goal, str):
        career_lower = career_goal.lower()
        if "phd" in career_lower or "研究" in career_lower or "博" in career_lower:
            weights["academic"] = 0.40
            weights["career"] = 0.15
        elif "startup" in career_lower or "创业" in career_lower:
            weights["career"] = 0.35
            weights["academic"] = 0.20

    if preferences.get("location_preference") or preferences.get("campus_culture"):
        weights["life"] = 0.25
        # Rebalance
        total = sum(weights.values())
        weights = {k: v / total for k, v in weights.items()}

    return weights


def _build_strategy(
    school_results: list[dict[str, Any]],
    student: Any,
    preferences: dict[str, Any],
) -> dict[str, Any]:
    """Build ED/EA/RD strategy recommendation from scored schools."""
    tiered: dict[str, list[dict[str, Any]]] = {
        "reach": [],
        "target": [],
        "safety": [],
        "likely": [],
    }
    for sr in school_results:
        bucket = tiered.get(sr["tier"], [])
        bucket.append(sr)

    strategy: dict[str, Any] = {
        "tier_counts": {k: len(v) for k, v in tiered.items()},
    }

    # ED recommendation: highest-scored target or reach school
    ed_candidates = tiered.get("target", []) + tiered.get("reach", [])
    ed_candidates.sort(key=lambda x: x["overall_score"], reverse=True)

    # Check if student has an ED preference
    ed_pref = preferences.get("ed_strategy") or student.ed_preference
    if ed_pref and isinstance(ed_pref, str) and "no" not in ed_pref.lower():
        if ed_candidates:
            strategy["ed_recommendation"] = {
                "school": ed_candidates[0]["school_name"],
                "rationale": (
                    f"Highest overall fit score ({ed_candidates[0]['overall_score']:.0%}) "
                    f"among your reach/target schools"
                ),
            }

    # EA recommendations: top safety/target schools
    ea_candidates = tiered.get("target", []) + tiered.get("safety", [])
    ea_candidates.sort(key=lambda x: x["overall_score"], reverse=True)
    strategy["ea_recommendations"] = [
        {"school": s["school_name"], "score": s["overall_score"]}
        for s in ea_candidates[:3]
    ]

    # RD: the rest
    strategy["rd_recommendations"] = [
        {"school": s["school_name"], "score": s["overall_score"]}
        for s in school_results
        if s["school_name"] not in {
            strategy.get("ed_recommendation", {}).get("school"),
            *(ea["school"] for ea in strategy.get("ea_recommendations", [])),
        }
    ]

    return strategy


async def _generate_narrative(
    llm: LLMClient,
    student: Any,
    school_results: list[dict[str, Any]],
    strategy: dict[str, Any],
) -> str:
    """Generate a natural language summary of the recommendations."""
    # Build a concise summary for the LLM
    schools_summary = []
    for sr in school_results:
        schools_summary.append({
            "name": sr["school_name"],
            "tier": sr["tier"],
            "score": round(sr["overall_score"], 2),
            "admission_prob": round(sr.get("admission_probability", 0), 2),
            "reasons": sr.get("key_reasons", [])[:2],
        })

    prompt_data = {
        "student_name": student.name,
        "intended_majors": student.intended_majors,
        "schools": schools_summary,
        "strategy": strategy,
    }

    messages = [
        {
            "role": "system",
            "content": (
                "You are ScholarPath, a college admissions advisor. Generate a "
                "brief, encouraging narrative summary (2-3 paragraphs) of the "
                "student's school recommendations. Mention the tier distribution, "
                "highlight 2-3 standout matches, and summarize the application "
                "strategy. Be warm but data-driven. The student may be Chinese -- "
                "respond in the same language they used previously, or default to "
                "English with Chinese school names in parentheses if available."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Generate a recommendation narrative for this data:\n"
                f"{__import__('json').dumps(prompt_data, ensure_ascii=False, default=str)}"
            ),
        },
    ]

    try:
        narrative = await llm.complete(
            messages,
            temperature=0.6,
            max_tokens=1024,
            caller="recommendation.narrative",
        )
        return narrative
    except Exception:
        logger.warning("Failed to generate recommendation narrative", exc_info=True)
        # Fallback: build a simple narrative
        reach = sum(1 for s in school_results if s["tier"] == "reach")
        target = sum(1 for s in school_results if s["tier"] == "target")
        safety = sum(1 for s in school_results if s["tier"] == "safety")
        likely = sum(1 for s in school_results if s["tier"] == "likely")
        return (
            f"Based on your profile, I've identified {len(school_results)} schools: "
            f"{reach} reach, {target} target, {safety} safety, and {likely} likely. "
            f"Your top match is {school_results[0]['school_name'] if school_results else 'N/A'}."
        )
