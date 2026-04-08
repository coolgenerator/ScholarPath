"""Recommendation service -- causal-engine-based school recommendations.

Combines vector similarity search, causal DAG analysis, mediation
decomposition, and Go/No-Go scoring to generate personalized,
explainable school recommendations.
"""

from __future__ import annotations

import math
import logging
import re
import time
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.language import (
    ResponseLanguage,
    language_instruction,
    select_localized_text,
)
from scholarpath.causal import (
    AdmissionDAGBuilder,
    GoNoGoScorer,
    MediationAnalyzer,
    NoisyORPropagator,
)
from scholarpath.db.models import School, SchoolEvaluation, Tier
from scholarpath.llm.client import LLMClient
from scholarpath.llm.embeddings import get_embedding_service
from scholarpath.services.portfolio_service import (
    get_student_canonical_preferences,
    get_student_sat_equivalent,
)
from scholarpath.services.recommendation_skills import (
    RecommendationSkillProfile,
    resolve_skill_id,
    profile_for_skill,
)
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)

_RISK_THRESHOLDS = {
    "safer": {"likely": 0.82, "safety": 0.67, "target": 0.45},
    "balanced": {"likely": 0.78, "safety": 0.60, "target": 0.35},
    "ambitious": {"likely": 0.72, "safety": 0.52, "target": 0.28},
}
_DEEPSEARCH_FALLBACK_WINDOW_SECONDS = 20 * 60
_DEEPSEARCH_FALLBACK_TOP_K_SCHOOLS = 8
_DEEPSEARCH_REQUIRED_FIELDS = [
    "avg_net_price",
    "state",
    "campus_setting",
    "programs_offered",
    "median_earnings_10y",
]
_DEEPSEARCH_FALLBACK_LAST_TRIGGER: dict[str, float] = {}
_SAT_SCALE_SECTION = "section"
_SAT_SCALE_TOTAL = "total"
_SAT_SCALE_NEUTRAL = "neutral"
_DEFAULT_TOP_N_MIN = 3
_DEFAULT_TOP_N_MAX = 20

_GEO_REGION_ALIASES: dict[str, set[str]] = {
    "west coast": {"ca", "wa", "or"},
    "northeast": {"ma", "ct", "ny", "nj", "pa", "ri", "nh", "vt", "me"},
    "midwest": {"il", "mi", "wi", "mn", "oh", "in", "ia", "mo", "ks", "ne"},
    "south": {"tx", "fl", "ga", "nc", "sc", "va", "tn", "al", "la", "ms"},
}
_STATE_NAME_TO_ABBR: dict[str, str] = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "district of columbia": "dc",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}
_TOP_N_PATTERNS = (
    re.compile(r"\btop\s*[- ]?(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bshortlist(?:\s+of)?\s+(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\b(\d{1,2})\s*(?:schools|school|colleges|options)\b", re.IGNORECASE),
    re.compile(r"前\s*(\d{1,2})\s*(?:所|个)?", re.IGNORECASE),
    re.compile(r"(\d{1,2})\s*所", re.IGNORECASE),
)


async def generate_recommendations(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    response_language: ResponseLanguage = "en",
    *,
    skill_id: str | None = None,
    user_message: str | None = None,
    route_bucket: str | None = None,
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
    preferences = get_student_canonical_preferences(student)
    resolved_skill_id = resolve_skill_id(
        explicit_skill_id=skill_id,
        message=user_message,
        bucket=route_bucket,
    )
    skill_profile = profile_for_skill(resolved_skill_id)
    top_n_used = _resolve_requested_top_n(
        user_message=user_message,
        default_top_n=skill_profile.top_n,
    )

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
                .options(selectinload(School.programs))
                .where(School.embedding.isnot(None))
                .order_by(School.embedding.cosine_distance(vec_param))
                .limit(skill_profile.candidate_pool_size)
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
            .options(selectinload(School.programs))
            .order_by(School.us_news_rank.asc().nullslast())
            .limit(skill_profile.candidate_pool_size)
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
        "sat": get_student_sat_equivalent(student),
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
                skill_profile=skill_profile,
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
    pool_multiplier = 4 if resolved_skill_id.endswith(("risk_first", "major_first", "geo_first", "roi_first")) else 3
    prefilter_pool_size = max(top_n_used * pool_multiplier, top_n_used + 12)
    if school_results:
        prefilter_pool_size = min(prefilter_pool_size, len(school_results))
    selected_results, prefilter_meta = _apply_budget_prefilter(
        school_results=school_results,
        budget_cap=student.budget_usd,
        top_n=prefilter_pool_size,
        stretch_slots=skill_profile.stretch_slots,
        budget_hard_gate=skill_profile.budget_hard_gate,
    )
    selected_results, scenario_execution = _apply_scenario_constraints(
        selected_results=selected_results,
        skill_profile=skill_profile,
        top_n=top_n_used,
    )
    scenario_validation = _validate_scenario_constraints(
        selected_results=selected_results,
        prefilter_meta=prefilter_meta,
        budget_cap=student.budget_usd,
        skill_profile=skill_profile,
        top_n=top_n_used,
        scenario_execution=scenario_execution,
    )
    deepsearch_status = _maybe_trigger_deepsearch_fallback(
        student_id=student_id,
        skill_profile=skill_profile,
        selected_results=selected_results,
        scenario_validation=scenario_validation,
    )

    # ------------------------------------------------------------------
    # Step 3: Persist SchoolEvaluation records
    # ------------------------------------------------------------------
    for sr in selected_results:
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
    strategy = _build_strategy(selected_results, student, preferences)

    # ------------------------------------------------------------------
    # Step 5: Generate LLM narrative summary
    # ------------------------------------------------------------------
    narrative = await _generate_narrative(
        llm,
        student,
        selected_results,
        strategy,
        response_language=response_language,
    )

    return {
        "skill_id_used": resolved_skill_id,
        "schools": selected_results,
        "strategy": strategy,
        "narrative": narrative,
        "prefilter_meta": prefilter_meta,
        "top_n_used": top_n_used,
        "scenario_validation": scenario_validation,
        "constraint_status": scenario_validation.get("constraint_status"),
        "constraint_fail_reasons": scenario_validation.get("constraint_fail_reasons", []),
        "next_steps": scenario_validation.get("next_steps", []),
        "deepsearch_pending": deepsearch_status.get("deepsearch_pending", False),
        "deepsearch_fallback_triggered": deepsearch_status.get("deepsearch_fallback_triggered", False),
        "deepsearch_fallback_reasons": deepsearch_status.get("trigger_reasons", []),
        "deepsearch_fallback_task_id": deepsearch_status.get("task_id"),
        "deepsearch_debounced": deepsearch_status.get("debounced", False),
        "deepsearch_enqueue_error": deepsearch_status.get("enqueue_error"),
    }


def _evaluate_school_with_causal(
    *,
    student: Any,
    student_profile_for_dag: dict[str, Any],
    school: School,
    preferences: dict[str, Any],
    skill_profile: RecommendationSkillProfile,
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
    dag_admission_prob = dag.nodes.get("admission_probability", {}).get(
        "propagated_belief", 0.3
    )
    dag_admission_prob = float(max(0.0, min(1.0, dag_admission_prob)))

    # Compute Go/No-Go score
    # Adjust weights based on student preferences
    weights = _adjust_weights_for_preferences(preferences, base_weights=skill_profile.weights)
    go_no_go = scorer.compute_score({}, dag, weights=weights)
    major_match, major_match_evidence = _compute_major_match_score_with_evidence(
        intended_majors=student.intended_majors or [],
        school=school,
    )
    if not major_match_evidence:
        major_match = _major_proxy_score_without_program_evidence(
            intended_majors=student.intended_majors or [],
            school=school,
            go_no_go_sub_scores=go_no_go.get("sub_scores", {}),
        )
    geo_match = _compute_geo_match_score(preferences=preferences, school=school)
    program_data_available = bool(school.programs)
    region_data_available = bool(
        str(school.state or "").strip()
        or str(school.city or "").strip()
        or str(school.campus_setting or "").strip()
    )
    acceptance_rate_pct = _normalize_acceptance_rate_pct(school.acceptance_rate)
    effective_acceptance_rate_pct, acceptance_rate_capped = _effective_acceptance_rate_pct(
        raw_acceptance_rate_pct=acceptance_rate_pct,
        school_rank=school.us_news_rank,
    )
    sat_fit, sat_scale_mode = _compute_sat_fit_with_mode(
        student_sat=get_student_sat_equivalent(student),
        school=school,
    )
    admission_prob = _calibrate_admission_probability(
        dag_prob=dag_admission_prob,
        sat_fit=sat_fit,
        acceptance_rate_pct=effective_acceptance_rate_pct,
    )

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

    if sat_fit >= 0.8:
        key_reasons.append("+ SAT profile is above this school's typical range")
    elif sat_fit <= 0.35:
        key_reasons.append("- SAT profile is below this school's typical range")
    if major_match >= 0.7:
        key_reasons.append("+ Strong major/program match signals")
    elif major_match <= 0.25:
        key_reasons.append("- Major/program match signals are limited")
    if not major_match_evidence:
        key_reasons.append("- Program coverage is limited; major-fit confidence reduced")
        key_reasons.append("- Major-fit uses school-level proxy signals until program data is enriched")
    if geo_match >= 0.7:
        key_reasons.append("+ Location aligns with your stated preference")
    if acceptance_rate_capped:
        key_reasons.append("- Selectivity guard adjusted anomalous acceptance data")

    budget_bonus = 0.0
    if student.budget_usd and school.avg_net_price:
        if student.budget_usd >= school.avg_net_price:
            budget_bonus = 0.03
            key_reasons.append("+ Budget covers estimated net cost")
        else:
            budget_bonus = -0.07
            key_reasons.append("- Net cost may exceed budget")
    elif student.budget_usd and school.avg_net_price is None:
        budget_bonus = -0.04
        key_reasons.append("- Net cost data missing for strict budget screening")

    risk_mode = str(skill_profile.risk_mode or "balanced")
    pref_risk = str(preferences.get("risk_preference") or "").lower()
    if "safe" in pref_risk or "保守" in pref_risk:
        risk_mode = "safer"
    elif "ambitious" in pref_risk or "aggressive" in pref_risk or "激进" in pref_risk:
        risk_mode = "ambitious"

    assigned_tier = _assign_tier(
        calibrated_prob=admission_prob,
        acceptance_rate_pct=effective_acceptance_rate_pct,
        sat_fit=sat_fit,
        risk_mode=risk_mode,
        return_meta=True,
    )
    tier, tier_cap_triggered = assigned_tier
    overall_score = _clamp01(
        float(go_no_go["overall_score"])
        + (skill_profile.major_boost * major_match)
        + (skill_profile.geo_boost * geo_match)
        + budget_bonus
    )
    key_reasons.append(f"+ Calibrated admission confidence: {admission_prob:.0%}")

    return {
        "school_id": school.id,
        "school_name": school.name,
        "school_name_cn": school.name_cn,
        "tier": tier,
        "overall_score": overall_score,
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
            "campus_setting": school.campus_setting,
        },
        "major_match": round(major_match, 4),
        "geo_match": round(geo_match, 4),
        "major_match_evidence": bool(major_match_evidence),
        "sat_scale_mode": sat_scale_mode,
        "acceptance_rate_effective": (
            round(float(effective_acceptance_rate_pct), 4)
            if effective_acceptance_rate_pct is not None
            else None
        ),
        "acceptance_rate_capped": bool(acceptance_rate_capped),
        "tier_cap_triggered": bool(tier_cap_triggered),
        "program_data_available": program_data_available,
        "region_data_available": region_data_available,
        "prefilter_tag": "unfiltered",
        "is_stretch": False,
    }


def _adjust_weights_for_preferences(
    preferences: dict[str, Any],
    *,
    base_weights: dict[str, float] | None = None,
) -> dict[str, float]:
    """Adjust Go/No-Go dimension weights based on student preferences."""
    weights = dict(base_weights or {})
    if not weights:
        weights = {"academic": 0.30, "financial": 0.25, "career": 0.25, "life": 0.20}

    career_goal = preferences.get("career_goal", "")
    if isinstance(career_goal, str):
        career_lower = career_goal.lower()
        if "phd" in career_lower or "研究" in career_lower or "博" in career_lower:
            weights["academic"] = 0.40
            weights["career"] = 0.15
        elif "startup" in career_lower or "创业" in career_lower:
            weights["career"] = 0.35
            weights["academic"] = 0.20

    if preferences.get("location") or preferences.get("culture"):
        weights["life"] = 0.25
        # Rebalance
        total = sum(weights.values()) or 1.0
        weights = {k: v / total for k, v in weights.items()}

    return weights


def _normalize_acceptance_rate_pct(value: float | None) -> float | None:
    if value is None:
        return None
    raw = float(value)
    if raw <= 1.0:
        raw = raw * 100.0
    return max(0.0, min(100.0, raw))


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _to_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _compute_sat_fit_score(*, student_sat: int, school: School) -> float:
    score, _ = _compute_sat_fit_with_mode(student_sat=student_sat, school=school)
    return score


def _compute_sat_fit_with_mode(*, student_sat: int, school: School) -> tuple[float, str]:
    sat = max(400, min(1600, int(student_sat)))
    sat_25 = school.sat_25
    sat_75 = school.sat_75
    if sat_25 is None or sat_75 is None or sat_75 <= sat_25:
        return 0.5, _SAT_SCALE_NEUTRAL
    if sat_75 <= 800 and sat_25 <= 800:
        sat = max(200, min(800, int(round(sat / 2.0))))
        mode = _SAT_SCALE_SECTION
    elif sat_75 > 800 and sat_25 > 800:
        mode = _SAT_SCALE_TOTAL
    else:
        return 0.5, _SAT_SCALE_NEUTRAL
    if sat >= sat_75:
        return 1.0, mode
    if sat <= sat_25:
        return 0.2, mode
    span = float(sat_75 - sat_25)
    return 0.2 + 0.8 * ((sat - sat_25) / span), mode


def _compute_major_match_score(*, intended_majors: list[str], school: School) -> float:
    score, _ = _compute_major_match_score_with_evidence(
        intended_majors=intended_majors,
        school=school,
    )
    return score


def _compute_major_match_score_with_evidence(
    *,
    intended_majors: list[str],
    school: School,
) -> tuple[float, bool]:
    majors = [str(item).strip().lower() for item in intended_majors if str(item).strip()]
    if not majors:
        return 0.5, False
    program_texts: list[str] = []
    for program in school.programs or []:
        merged = " ".join(
            [
                str(program.name or ""),
                str(program.department or ""),
                str(program.description or ""),
            ],
        ).strip().lower()
        if merged:
            program_texts.append(merged)
    if not program_texts:
        return 0.35, False
    full_text = " || ".join(program_texts)
    hits = 0
    for major in majors:
        major_tokens = [tok for tok in major.replace("/", " ").split() if tok]
        if major in full_text:
            hits += 1
            continue
        if any(tok in full_text for tok in major_tokens):
            hits += 1
    return max(0.0, min(1.0, hits / max(1, len(majors)))), True


def _major_proxy_score_without_program_evidence(
    *,
    intended_majors: list[str],
    school: School,
    go_no_go_sub_scores: dict[str, Any],
) -> float:
    majors = [str(item).strip().lower() for item in intended_majors if str(item).strip()]
    if not majors:
        return 0.42

    school_name = str(school.name or "").lower()
    school_type = str(school.school_type or "").lower()
    rank = int(school.us_news_rank or 9999)
    academic_score = _to_float(go_no_go_sub_scores.get("academic"), default=0.5)
    career_score = _to_float(go_no_go_sub_scores.get("career"), default=0.5)
    base = 0.34 + (0.18 * academic_score) + (0.12 * career_score)

    category_bonus = 0.0
    for major in majors:
        major_norm = major.replace("/", " ")
        major_tokens = set(tok for tok in major_norm.split() if tok)
        if major_tokens & {
            "computer", "science", "cs", "software", "engineering", "ai", "data", "informatics",
        }:
            if school_type in {"technical", "university"}:
                category_bonus = max(category_bonus, 0.10)
            if any(word in school_name for word in ("technology", "tech", "polytechnic", "institute")):
                category_bonus = max(category_bonus, 0.14)
        elif major_tokens & {"economics", "finance", "business", "accounting"}:
            if school_type == "university":
                category_bonus = max(category_bonus, 0.09)
            if any(word in school_name for word in ("business", "management", "commerce")):
                category_bonus = max(category_bonus, 0.12)
        elif major_tokens & {"psychology", "biology", "biological", "neuroscience", "public", "health"}:
            if school_type in {"lac", "university"}:
                category_bonus = max(category_bonus, 0.08)
        else:
            if school_type in {"university", "lac"}:
                category_bonus = max(category_bonus, 0.06)

    rank_bonus = 0.0
    if rank <= 30:
        rank_bonus = 0.06
    elif rank <= 80:
        rank_bonus = 0.04
    elif rank <= 150:
        rank_bonus = 0.02

    return max(0.32, min(0.62, base + category_bonus + rank_bonus))


def _compute_geo_match_score(*, preferences: dict[str, Any], school: School) -> float:
    geo_values: list[str] = []
    raw_geo = preferences.get("location") or preferences.get("preferred_region")
    if isinstance(raw_geo, str):
        geo_values = [raw_geo.strip().lower()]
    elif isinstance(raw_geo, list):
        geo_values = [str(item).strip().lower() for item in raw_geo if str(item).strip()]
    if not geo_values:
        return 0.5

    state_raw = _normalize_geo_text(str(school.state or ""))
    city_raw = _normalize_geo_text(str(school.city or ""))
    setting_raw = _normalize_geo_text(str(school.campus_setting or ""))
    state_abbr = _state_to_abbr(state_raw)

    state_tokens = set(state_raw.split())
    city_tokens = set(city_raw.split())

    for pref in geo_values:
        pref_norm = _normalize_geo_text(pref)
        if not pref_norm:
            continue
        region_states = _GEO_REGION_ALIASES.get(pref_norm, set())
        if region_states and state_abbr in region_states:
            return 0.95
        if pref_norm in {"urban", "suburban", "rural"} and setting_raw == pref_norm:
            return 1.0

        pref_state_abbr = _state_to_abbr(pref_norm)
        if pref_state_abbr and state_abbr == pref_state_abbr:
            return 1.0

        if pref_norm == state_raw or pref_norm == city_raw:
            return 1.0

        pref_tokens = set(pref_norm.split())
        if pref_tokens and (pref_tokens.issubset(state_tokens) or pref_tokens.issubset(city_tokens)):
            return 0.9
    return 0.2


def _calibrate_admission_probability(
    *,
    dag_prob: float,
    sat_fit: float,
    acceptance_rate_pct: float | None,
) -> float:
    acceptance_component = 0.45
    if acceptance_rate_pct is not None:
        acceptance_component = max(0.02, min(0.95, acceptance_rate_pct / 100.0))
    calibrated = (0.55 * _clamp01(dag_prob)) + (0.28 * _clamp01(sat_fit)) + (0.17 * acceptance_component)
    if acceptance_rate_pct is not None:
        if acceptance_rate_pct < 6:
            cap = 0.28 if sat_fit < 0.98 else 0.38
            calibrated = min(calibrated, cap)
        elif acceptance_rate_pct < 10:
            cap = 0.40
            if sat_fit >= 0.95 and dag_prob >= 0.75:
                cap = 0.52
            calibrated = min(calibrated, cap)
        elif acceptance_rate_pct < 15:
            cap = 0.52 if sat_fit < 0.90 else 0.60
            calibrated = min(calibrated, cap)
    return _clamp01(calibrated)


def _assign_tier(
    *,
    calibrated_prob: float,
    acceptance_rate_pct: float | None,
    sat_fit: float,
    risk_mode: str,
    return_meta: bool = False,
) -> str | tuple[str, bool]:
    thresholds = _RISK_THRESHOLDS.get(risk_mode, _RISK_THRESHOLDS["balanced"])
    prob = _clamp01(calibrated_prob)
    cap_triggered = False

    if prob >= thresholds["likely"]:
        tier = Tier.LIKELY.value
    elif prob >= thresholds["safety"]:
        tier = Tier.SAFETY.value
    elif prob >= thresholds["target"]:
        tier = Tier.TARGET.value
    else:
        tier = Tier.REACH.value

    if acceptance_rate_pct is None:
        return (tier, cap_triggered) if return_meta else tier
    if acceptance_rate_pct < 6.0 and tier in {Tier.SAFETY.value, Tier.LIKELY.value}:
        cap_triggered = True
        tier = Tier.TARGET.value
        return (tier, cap_triggered) if return_meta else tier
    if 6.0 <= acceptance_rate_pct < 10.0 and tier in {Tier.SAFETY.value, Tier.LIKELY.value}:
        if sat_fit >= 0.95 and prob >= 0.82:
            tier = Tier.SAFETY.value
            return (tier, cap_triggered) if return_meta else tier
        cap_triggered = True
        tier = Tier.TARGET.value
        return (tier, cap_triggered) if return_meta else tier
    if 10.0 <= acceptance_rate_pct < 15.0 and tier == Tier.LIKELY.value:
        cap_triggered = True
        tier = Tier.SAFETY.value
        return (tier, cap_triggered) if return_meta else tier
    return (tier, cap_triggered) if return_meta else tier


def _apply_budget_prefilter(
    *,
    school_results: list[dict[str, Any]],
    budget_cap: int | None,
    top_n: int,
    stretch_slots: int,
    budget_hard_gate: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ranked = sorted(school_results, key=lambda item: float(item.get("overall_score", 0.0)), reverse=True)
    if not budget_hard_gate or budget_cap is None:
        selected = ranked[: max(1, top_n)]
        for item in selected:
            item["prefilter_tag"] = "eligible"
            item["is_stretch"] = False
        return selected, {
            "budget_cap_used": budget_cap,
            "eligible_count": len(selected),
            "stretch_count": 0,
            "excluded_count": max(0, len(ranked) - len(selected)),
            "excluded_reasons_summary": {},
        }

    eligible: list[dict[str, Any]] = []
    over_budget: list[dict[str, Any]] = []
    missing_price: list[dict[str, Any]] = []

    for item in ranked:
        net_price = item.get("school_info", {}).get("avg_net_price")
        if isinstance(net_price, (int, float)):
            if float(net_price) <= float(budget_cap):
                item["prefilter_tag"] = "eligible"
                item["is_stretch"] = False
                eligible.append(item)
            else:
                over_budget.append(item)
        else:
            missing_price.append(item)

    stretch = over_budget[: max(0, stretch_slots)]
    for item in stretch:
        item["prefilter_tag"] = "stretch"
        item["is_stretch"] = True

    selected = sorted(eligible + stretch, key=lambda item: float(item.get("overall_score", 0.0)), reverse=True)
    selected = selected[: max(1, top_n)]

    excluded_count = max(0, len(ranked) - len(selected))
    return selected, {
        "budget_cap_used": budget_cap,
        "eligible_count": len(eligible),
        "stretch_count": len(stretch),
        "excluded_count": excluded_count,
        "excluded_reasons_summary": {
            "over_budget": max(0, len(over_budget) - len(stretch)),
            "missing_net_price": len(missing_price),
        },
    }


def _validate_scenario_constraints(
    *,
    selected_results: list[dict[str, Any]],
    prefilter_meta: dict[str, Any],
    budget_cap: int | None,
    skill_profile: RecommendationSkillProfile,
    top_n: int,
    scenario_execution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    total = len(selected_results)
    constraints: dict[str, dict[str, Any]] = {}
    fail_reasons: list[str] = []
    effective_min_results = min(max(1, int(skill_profile.min_results)), max(1, int(top_n)))
    effective_tier_confidence_min = min(
        max(1, int(skill_profile.tier_confidence_min_count)),
        max(1, int(top_n) - 1),
    )

    minimum_results_ok = total >= effective_min_results
    constraints["minimum_results"] = {
        "passed": minimum_results_ok,
        "required": int(effective_min_results),
        "actual": total,
    }
    if not minimum_results_ok:
        fail_reasons.append("insufficient_recommendation_count")

    tier_confident = sum(
        1
        for item in selected_results
        if str(item.get("tier")) in {"target", "safety", "likely"}
        and float(item.get("admission_probability") or 0.0) >= 0.35
    )
    tier_confidence_ok = tier_confident >= effective_tier_confidence_min
    constraints["tier_confidence"] = {
        "passed": tier_confidence_ok,
        "required": int(effective_tier_confidence_min),
        "actual": tier_confident,
    }
    if not tier_confidence_ok:
        fail_reasons.append("tier_confidence_insufficient")

    budget_relevant = skill_profile.skill_id.endswith("budget_first") or budget_cap is not None
    if budget_relevant:
        stretch_count = int(prefilter_meta.get("stretch_count") or 0)
        tags_ok = all(
            str(item.get("prefilter_tag") or "") in {"eligible", "stretch"}
            for item in selected_results
        )
        stretch_ok = stretch_count <= max(0, skill_profile.stretch_slots)
        priced_eligible_ok = True
        if budget_cap is not None:
            for item in selected_results:
                if str(item.get("prefilter_tag")) != "eligible":
                    continue
                net_price = item.get("school_info", {}).get("avg_net_price")
                if not isinstance(net_price, (int, float)) or float(net_price) > float(budget_cap):
                    priced_eligible_ok = False
                    break
        budget_ok = budget_cap is not None and tags_ok and stretch_ok and priced_eligible_ok
        constraints["budget_hard_gate"] = {
            "passed": budget_ok,
            "budget_cap_used": budget_cap,
            "stretch_count": stretch_count,
            "stretch_limit": int(skill_profile.stretch_slots),
            "eligible_count": int(prefilter_meta.get("eligible_count") or 0),
        }
        if not budget_ok:
            fail_reasons.append("budget_hard_gate_failed")

    if skill_profile.skill_id.endswith("risk_first"):
        tier_counts = {
            "reach": sum(1 for item in selected_results if str(item.get("tier")) == "reach"),
            "target": sum(1 for item in selected_results if str(item.get("tier")) == "target"),
            "safety": sum(1 for item in selected_results if str(item.get("tier")) in {"safety", "likely"}),
        }
        required = dict(skill_profile.risk_min_tier_counts)
        risk_ok = (
            tier_counts["reach"] >= required.get("reach", 0)
            and tier_counts["target"] >= required.get("target", 0)
            and tier_counts["safety"] >= required.get("safety", 0)
        )
        constraints["risk_tier_mix"] = {
            "passed": risk_ok,
            "required": required,
            "actual": tier_counts,
            "shortfall_total": int((scenario_execution or {}).get("risk_quota_shortfall_count") or 0),
        }
        if not risk_ok:
            fail_reasons.append("risk_tier_mix_insufficient")

    if skill_profile.skill_id.endswith("major_first"):
        major_hits = sum(
            1
            for item in selected_results
            if float(item.get("major_match") or 0.0) >= skill_profile.major_match_threshold
        )
        major_ratio = major_hits / max(1, total)
        major_ok = major_ratio >= skill_profile.major_match_min_ratio
        constraints["major_alignment"] = {
            "passed": major_ok,
            "required_ratio": round(float(skill_profile.major_match_min_ratio), 4),
            "required_threshold": round(float(skill_profile.major_match_threshold), 4),
            "actual_ratio": round(float(major_ratio), 4),
            "backfill_count": int((scenario_execution or {}).get("major_backfill_count") or 0),
        }
        if not major_ok:
            fail_reasons.append("major_alignment_low")

    if skill_profile.skill_id.endswith("geo_first"):
        geo_hits = sum(
            1
            for item in selected_results
            if float(item.get("geo_match") or 0.0) >= skill_profile.geo_match_threshold
        )
        geo_ratio = geo_hits / max(1, total)
        geo_ok = geo_ratio >= skill_profile.geo_match_min_ratio
        constraints["geo_alignment"] = {
            "passed": geo_ok,
            "required_ratio": round(float(skill_profile.geo_match_min_ratio), 4),
            "required_threshold": round(float(skill_profile.geo_match_threshold), 4),
            "actual_ratio": round(float(geo_ratio), 4),
            "backfill_count": int((scenario_execution or {}).get("geo_backfill_count") or 0),
        }
        if not geo_ok:
            fail_reasons.append("geo_alignment_low")

    if skill_profile.skill_id.endswith("roi_first"):
        mean_career = _mean(
            [
                float(item.get("sub_scores", {}).get("career") or 0.0)
                for item in selected_results
            ],
        )
        roi_ok = mean_career >= skill_profile.roi_career_min_mean
        constraints["roi_career_floor"] = {
            "passed": roi_ok,
            "required_mean": round(float(skill_profile.roi_career_min_mean), 4),
            "actual_mean": round(float(mean_career), 4),
            "backfill_count": int((scenario_execution or {}).get("roi_backfill_count") or 0),
        }
        if not roi_ok:
            fail_reasons.append("roi_signal_low")

    constraint_status = "pass" if not fail_reasons else "degraded"
    return {
        "scenario": skill_profile.skill_id,
        "constraint_status": constraint_status,
        "constraints": constraints,
        "constraint_fail_reasons": fail_reasons,
        "next_steps": _build_constraint_next_steps(fail_reasons),
        "execution": scenario_execution or {},
    }


def _apply_scenario_constraints(
    *,
    selected_results: list[dict[str, Any]],
    skill_profile: RecommendationSkillProfile,
    top_n: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ranked = sorted(
        selected_results,
        key=lambda item: float(item.get("overall_score", 0.0)),
        reverse=True,
    )
    scenario_execution: dict[str, Any] = {
        "risk_quota_shortfall_count": 0,
        "geo_backfill_count": 0,
        "major_backfill_count": 0,
        "roi_backfill_count": 0,
    }
    if not ranked:
        return ranked, scenario_execution
    top_n = max(1, int(top_n))
    skill_id = skill_profile.skill_id
    if skill_id.endswith("risk_first"):
        return _apply_risk_constraints(
            ranked=ranked,
            top_n=top_n,
            required=skill_profile.risk_min_tier_counts,
            risk_mode=skill_profile.risk_mode,
        )
    if skill_id.endswith("geo_first"):
        required_hits = max(1, int(math.ceil(top_n * float(skill_profile.geo_match_min_ratio))))
        return _apply_threshold_constraints(
            ranked=ranked,
            top_n=top_n,
            metric_key="geo_match",
            threshold=float(skill_profile.geo_match_threshold),
            required_hits=required_hits,
            backfill_key="geo_backfill_count",
        )
    if skill_id.endswith("major_first"):
        required_hits = max(1, int(math.ceil(top_n * float(skill_profile.major_match_min_ratio))))
        return _apply_threshold_constraints(
            ranked=ranked,
            top_n=top_n,
            metric_key="major_match",
            threshold=float(skill_profile.major_match_threshold),
            required_hits=required_hits,
            backfill_key="major_backfill_count",
        )
    if skill_id.endswith("roi_first"):
        required_hits = max(1, int(math.ceil(top_n * 0.6)))
        return _apply_threshold_constraints(
            ranked=ranked,
            top_n=top_n,
            metric_key="career_subscore",
            threshold=float(skill_profile.roi_career_min_mean),
            required_hits=required_hits,
            backfill_key="roi_backfill_count",
        )
    return ranked[:top_n], scenario_execution


def _apply_risk_constraints(
    *,
    ranked: list[dict[str, Any]],
    top_n: int,
    required: dict[str, int],
    risk_mode: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    reach_pool = [item for item in ranked if str(item.get("tier")) == "reach"]
    target_pool = [item for item in ranked if str(item.get("tier")) == "target"]
    safety_pool = [
        item for item in ranked
        if str(item.get("tier")) in {"safety", "likely"}
    ]
    selected: list[dict[str, Any]] = []
    used_ids: set[str] = set()

    def _pick(pool: list[dict[str, Any]], count: int) -> int:
        picked = 0
        for item in pool:
            sid = _school_identity(item)
            if sid in used_ids:
                continue
            selected.append(item)
            used_ids.add(sid)
            picked += 1
            if picked >= count:
                break
        return picked

    need_reach = max(0, int(required.get("reach", 0)))
    need_target = max(0, int(required.get("target", 0)))
    need_safety = max(0, int(required.get("safety", 0)))
    got_reach = _pick(reach_pool, need_reach)
    got_target = _pick(target_pool, need_target)
    got_safety = _pick(safety_pool, need_safety)
    shortfall = (
        max(0, need_reach - got_reach)
        + max(0, need_target - got_target)
        + max(0, need_safety - got_safety)
    )
    reach_cap_ratio = 0.25 if risk_mode == "safer" else 0.35
    reach_cap = max(need_reach, int(math.ceil(top_n * reach_cap_ratio)))

    primary_fill_order = target_pool + safety_pool + reach_pool + ranked
    for item in primary_fill_order:
        sid = _school_identity(item)
        if sid in used_ids:
            continue
        if str(item.get("tier")) == "reach":
            current_reach = sum(1 for picked in selected if str(picked.get("tier")) == "reach")
            if current_reach >= reach_cap:
                continue
        selected.append(item)
        used_ids.add(sid)
        if len(selected) >= top_n:
            break
    if len(selected) < top_n:
        for item in ranked:
            sid = _school_identity(item)
            if sid in used_ids:
                continue
            selected.append(item)
            used_ids.add(sid)
            if len(selected) >= top_n:
                break
    selected = selected[:top_n]
    return selected, {
        "risk_quota_shortfall_count": shortfall,
        "geo_backfill_count": 0,
        "major_backfill_count": 0,
        "roi_backfill_count": 0,
    }


def _metric_value(item: dict[str, Any], metric_key: str) -> float:
    if metric_key == "career_subscore":
        return float(item.get("sub_scores", {}).get("career") or 0.0)
    return float(item.get(metric_key) or 0.0)


def _school_identity(item: dict[str, Any]) -> str:
    school_id = item.get("school_id")
    if school_id is not None:
        return f"id:{school_id}"
    return f"name:{str(item.get('school_name') or '')}"


def _apply_threshold_constraints(
    *,
    ranked: list[dict[str, Any]],
    top_n: int,
    metric_key: str,
    threshold: float,
    required_hits: int,
    backfill_key: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    hits = [item for item in ranked if _metric_value(item, metric_key) >= threshold]
    misses = [item for item in ranked if _metric_value(item, metric_key) < threshold]
    selected: list[dict[str, Any]] = []
    selected.extend(hits[:required_hits])
    used_ids = {_school_identity(item) for item in selected}
    remaining_hits = hits[required_hits:]
    for item in remaining_hits + misses:
        if len(selected) >= top_n:
            break
        sid = _school_identity(item)
        if sid in used_ids:
            continue
        selected.append(item)
        used_ids.add(sid)
    selected = selected[:top_n]
    backfill_count = max(0, required_hits - len(hits))
    return selected, {
        "risk_quota_shortfall_count": 0,
        "geo_backfill_count": backfill_count if backfill_key == "geo_backfill_count" else 0,
        "major_backfill_count": backfill_count if backfill_key == "major_backfill_count" else 0,
        "roi_backfill_count": backfill_count if backfill_key == "roi_backfill_count" else 0,
    }


def _build_constraint_next_steps(fail_reasons: list[str]) -> list[str]:
    if not fail_reasons:
        return []
    reason_to_step = {
        "insufficient_recommendation_count": "Increase candidate pool or loosen one secondary preference.",
        "tier_confidence_insufficient": "Add 3-5 schools with stronger admissions confidence to stabilize the list.",
        "budget_hard_gate_failed": "Provide a clearer budget cap or financial-aid preference to enforce affordability.",
        "risk_tier_mix_insufficient": "Add more target/safety schools to satisfy a safer application mix.",
        "major_alignment_low": "Specify 1-2 intended majors more precisely so major-fit ranking can tighten.",
        "geo_alignment_low": "Narrow preferred regions or states to improve geographic alignment.",
        "roi_signal_low": "Provide preferred career outcomes (industry/salary target) to strengthen ROI ranking.",
    }
    ordered: list[str] = []
    for reason in fail_reasons:
        step = reason_to_step.get(reason)
        if step and step not in ordered:
            ordered.append(step)
    return ordered


def _maybe_trigger_deepsearch_fallback(
    *,
    student_id: uuid.UUID,
    skill_profile: RecommendationSkillProfile,
    selected_results: list[dict[str, Any]],
    scenario_validation: dict[str, Any],
) -> dict[str, Any]:
    missing_rates = _compute_missing_field_rates(selected_results)
    trigger_reasons: list[str] = []
    threshold = float(skill_profile.missing_field_trigger_threshold)
    for field_name, ratio in missing_rates.items():
        if ratio >= threshold:
            trigger_reasons.append(f"missing_{field_name}_high")

    if str(scenario_validation.get("constraint_status")) != "pass":
        trigger_reasons.append("scenario_constraints_failed")

    tier_confidence = (
        scenario_validation.get("constraints", {})
        .get("tier_confidence", {})
        .get("actual", 0)
    )
    if int(tier_confidence or 0) < int(skill_profile.tier_confidence_min_count):
        trigger_reasons.append("tier_confidence_low")

    if not trigger_reasons:
        return {
            "deepsearch_fallback_triggered": False,
            "deepsearch_pending": False,
            "debounced": False,
            "trigger_reasons": [],
            "missing_field_rates": missing_rates,
            "task_id": None,
        }

    debounce_key = f"{student_id}:{skill_profile.skill_id}"
    now = time.time()
    last = _DEEPSEARCH_FALLBACK_LAST_TRIGGER.get(debounce_key)
    if last is not None and now - last < _DEEPSEARCH_FALLBACK_WINDOW_SECONDS:
        return {
            "deepsearch_fallback_triggered": True,
            "deepsearch_pending": True,
            "debounced": True,
            "trigger_reasons": trigger_reasons,
            "missing_field_rates": missing_rates,
            "task_id": None,
        }

    task_id, enqueue_error = _enqueue_deepsearch_fallback(
        student_id=student_id,
        selected_results=selected_results,
        skill_profile=skill_profile,
    )
    if task_id:
        _DEEPSEARCH_FALLBACK_LAST_TRIGGER[debounce_key] = now
    return {
        "deepsearch_fallback_triggered": True,
        "deepsearch_pending": bool(task_id),
        "debounced": False,
        "trigger_reasons": trigger_reasons,
        "missing_field_rates": missing_rates,
        "task_id": task_id,
        "enqueue_error": enqueue_error,
    }


def _compute_missing_field_rates(selected_results: list[dict[str, Any]]) -> dict[str, float]:
    total = max(1, len(selected_results))
    missing_program = sum(
        1 for item in selected_results if not bool(item.get("program_data_available"))
    )
    missing_net_price = sum(
        1
        for item in selected_results
        if not isinstance(item.get("school_info", {}).get("avg_net_price"), (int, float))
    )
    missing_region = sum(
        1 for item in selected_results if not bool(item.get("region_data_available"))
    )
    return {
        "program": round(missing_program / total, 4),
        "net_price": round(missing_net_price / total, 4),
        "region": round(missing_region / total, 4),
    }


def _enqueue_deepsearch_fallback(
    *,
    student_id: uuid.UUID,
    selected_results: list[dict[str, Any]],
    skill_profile: RecommendationSkillProfile,
) -> tuple[str | None, str | None]:
    school_names: list[str] = []
    for item in selected_results:
        name = str(item.get("school_name") or "").strip()
        if name and name not in school_names:
            school_names.append(name)
    school_names = school_names[: _DEEPSEARCH_FALLBACK_TOP_K_SCHOOLS]
    if not school_names:
        return None, "empty_school_candidates"

    try:
        from scholarpath.tasks.deep_search import run_deep_search

        async_result = run_deep_search.delay(
            student_id=str(student_id),
            school_names=school_names,
            required_fields=_DEEPSEARCH_REQUIRED_FIELDS,
            freshness_days=120,
            max_internal_websearch_calls_per_school=1,
            budget_mode=str(skill_profile.risk_mode or "balanced"),
            eval_run_id=f"reco-fallback-{int(time.time())}",
        )
        return str(getattr(async_result, "id", "") or ""), None
    except ModuleNotFoundError as exc:  # pragma: no cover - local runtime fallback
        logger.info(
            "DeepSearch fallback skipped because task runtime dependencies are unavailable: %s",
            exc,
        )
        return None, "celery_unavailable"
    except Exception as exc:  # pragma: no cover - worker/runtime defensive branch
        logger.warning("Failed to enqueue DeepSearch fallback", exc_info=True)
        return None, str(exc)


def _normalize_geo_text(raw: str) -> str:
    text = raw.lower().strip()
    text = re.sub(r"[^a-z0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _state_to_abbr(value: str) -> str | None:
    if not value:
        return None
    if len(value) == 2 and value.isalpha():
        return value.lower()
    return _STATE_NAME_TO_ABBR.get(value.lower())


def _resolve_requested_top_n(*, user_message: str | None, default_top_n: int) -> int:
    if not user_message:
        return int(default_top_n)
    text = str(user_message)
    for pattern in _TOP_N_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        try:
            requested = int(match.group(1))
        except (TypeError, ValueError):
            continue
        return max(_DEFAULT_TOP_N_MIN, min(_DEFAULT_TOP_N_MAX, requested))
    return int(default_top_n)


def _rank_acceptance_prior_pct(rank: int | None) -> float:
    if rank is None:
        return 65.0
    if rank <= 10:
        return 5.0
    if rank <= 25:
        return 9.0
    if rank <= 50:
        return 18.0
    if rank <= 100:
        return 32.0
    if rank <= 200:
        return 50.0
    return 68.0


def _rank_acceptance_cap_pct(rank: int | None) -> float:
    if rank is None:
        return 95.0
    if rank <= 10:
        return 7.5
    if rank <= 20:
        return 10.0
    if rank <= 50:
        return 22.0
    if rank <= 100:
        return 45.0
    if rank <= 200:
        return 70.0
    return 95.0


def _effective_acceptance_rate_pct(
    *,
    raw_acceptance_rate_pct: float | None,
    school_rank: int | None,
) -> tuple[float, bool]:
    if raw_acceptance_rate_pct is None:
        return _rank_acceptance_prior_pct(school_rank), False
    cap = _rank_acceptance_cap_pct(school_rank)
    effective = min(float(raw_acceptance_rate_pct), cap)
    return effective, bool(effective < float(raw_acceptance_rate_pct))


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
    ed_pref = student.ed_preference
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
    response_language: ResponseLanguage = "en",
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
                "strategy. Be warm but data-driven. "
                f"{language_instruction(response_language)}"
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
        top_match = school_results[0]["school_name"] if school_results else "N/A"
        return select_localized_text(
            (
                f"基于你的档案，我筛出了 {len(school_results)} 所学校："
                f"{reach} 所冲刺、{target} 所主申、{safety} 所保底、{likely} 所较稳。"
                f"当前最匹配的是 {top_match}。"
            ),
            (
                f"Based on your profile, I've identified {len(school_results)} schools: "
                f"{reach} reach, {target} target, {safety} safety, and {likely} likely. "
                f"Your top match is {top_match}."
            ),
            response_language,
            mixed=(
                f"基于你的档案，我筛出了 {len(school_results)} 所学校，当前最匹配的是 {top_match}。\n"
                f"Based on your profile, I've identified {len(school_results)} schools. Your top match is {top_match}."
            ),
        )
