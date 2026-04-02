"""What-if simulation via causal do-interventions."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.causal_engine import CausalRuntime
from scholarpath.db.models import School
from scholarpath.exceptions import ScholarPathError
from scholarpath.llm.client import LLMClient
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)

# Outcome nodes we report on.
_OUTCOME_NODES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]


async def run_what_if(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    school_id: uuid.UUID,
    interventions: dict[str, float],
) -> dict[str, Any]:
    """Run a what-if simulation for a student-school pair.

    Parameters
    ----------
    session:
        Active async database session.
    llm:
        LLM client for generating the explanation narrative.
    student_id:
        The student whose profile forms the baseline.
    school_id:
        The school to evaluate against.
    interventions:
        Mapping of DAG node ids to ``do(node = value)`` assignments.
        Example: ``{"student_ability": 0.9, "financial_aid": 0.8}``.

    Returns
    -------
    dict
        ``original_scores``: baseline outcome scores.
        ``modified_scores``: scores after interventions.
        ``deltas``: per-outcome change.
        ``explanation``: LLM-generated narrative.
    """
    student = await get_student(session, student_id)
    school = await session.get(
        School,
        school_id,
        options=[selectinload(School.programs)],
    )
    if school is None:
        raise ScholarPathError(f"School {school_id} not found")

    causal_runtime = CausalRuntime(session)
    baseline_result, _ = await causal_runtime.estimate(
        student=student,
        school=school,
        offer=None,
        context="simulation",
        outcomes=_OUTCOME_NODES,
        metadata={"service": "simulation_service"},
    )
    intervention_result, _ = await causal_runtime.intervene(
        student=student,
        school=school,
        offer=None,
        context="simulation",
        interventions=interventions,
        outcomes=_OUTCOME_NODES,
        metadata={"service": "simulation_service"},
    )

    original_scores = {k: round(v, 4) for k, v in baseline_result.scores.items()}
    modified_scores = {k: round(v, 4) for k, v in intervention_result.modified_scores.items()}
    deltas = {k: round(v, 4) for k, v in intervention_result.deltas.items()}

    # --- LLM explanation ---
    messages = [
        {
            "role": "system",
            "content": (
                "You are a college admissions causal reasoning expert. "
                "Explain the results of a what-if simulation in plain language. "
                "Be specific about which interventions drove which changes and why."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Student: {student.name}, School: {school.name}\n"
                f"Interventions: {interventions}\n"
                f"Original scores: {original_scores}\n"
                f"Modified scores: {modified_scores}\n"
                f"Deltas: {deltas}\n\n"
                "Explain these results concisely."
            ),
        },
    ]
    explanation = await llm.complete(messages, temperature=0.5, max_tokens=768)

    logger.info(
        "What-if for student %s / school %s: %d interventions",
        student_id,
        school_id,
        len(interventions),
    )

    return {
        "original_scores": original_scores,
        "modified_scores": modified_scores,
        "deltas": deltas,
        "explanation": explanation,
        "causal_engine_version": intervention_result.causal_engine_version,
        "causal_model_version": intervention_result.causal_model_version,
        "estimate_confidence": intervention_result.estimate_confidence,
        "label_type": intervention_result.label_type,
        "label_confidence": intervention_result.label_confidence,
        "fallback_used": intervention_result.fallback_used,
        "fallback_reason": intervention_result.fallback_reason,
    }


async def compare_scenarios(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    scenarios: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run multiple what-if scenarios and compare the results.

    Parameters
    ----------
    session:
        Active async database session.
    llm:
        LLM client for generating the summary.
    student_id:
        The student whose profile forms the baseline.
    scenarios:
        List of scenario dicts, each with ``school_id`` (UUID) and
        ``interventions`` (dict[str, float]).

    Returns
    -------
    dict
        ``scenarios``: list of individual what-if results (labelled).
        ``summary``: LLM-generated comparative summary.
    """
    results: list[dict[str, Any]] = []
    for i, scenario in enumerate(scenarios):
        school_id = scenario.get("school_id")
        if school_id is None:
            raise ScholarPathError(f"Scenario {i} missing 'school_id'")
        interventions_raw = scenario.get("interventions", {})
        if not isinstance(interventions_raw, dict):
            raise ScholarPathError(f"Scenario {i} interventions must be an object")
        interventions = {
            str(k): float(v) for k, v in interventions_raw.items()
        }
        label = scenario.get("label", f"Scenario {i + 1}")

        result = await run_what_if(
            session, llm, student_id, uuid.UUID(str(school_id)), interventions
        )
        result["label"] = label
        result["school_id"] = str(school_id)
        results.append(result)

    # Comparative summary via LLM
    messages = [
        {
            "role": "system",
            "content": (
                "You are a college admissions advisor. Compare the following "
                "what-if simulation scenarios and provide a concise summary "
                "highlighting which scenario is most favorable and why."
            ),
        },
        {
            "role": "user",
            "content": "\n\n".join(
                f"**{r['label']}**\nModified scores: {r['modified_scores']}\nDeltas: {r['deltas']}"
                for r in results
            ),
        },
    ]
    summary = await llm.complete(messages, temperature=0.5, max_tokens=768)

    return {
        "scenarios": results,
        "summary": summary,
    }
