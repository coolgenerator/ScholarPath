"""What-if simulation via causal do-interventions."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from scholarpath.language import ResponseLanguage, language_instruction
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.causal import AdmissionDAGBuilder, NoisyORPropagator
from scholarpath.db.models import School
from scholarpath.exceptions import ScholarPathError
from scholarpath.llm.client import LLMClient
from scholarpath.services.portfolio_service import get_student_sat_equivalent
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
    response_language: ResponseLanguage = "en",
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

    builder = AdmissionDAGBuilder()
    propagator = NoisyORPropagator()

    student_profile = {"gpa": student.gpa, "sat": get_student_sat_equivalent(student)}
    school_data: dict[str, Any] = {}
    if school.acceptance_rate is not None:
        school_data["acceptance_rate"] = school.acceptance_rate

    # --- Baseline ---
    dag_base = builder.build_admission_dag(student_profile, school_data)
    dag_base = propagator.propagate(dag_base)
    original_scores = _extract_outcome_scores(dag_base)

    # --- Intervened ---
    dag_mod = builder.build_admission_dag(student_profile, school_data)
    # Apply do-interventions: clamp node priors and sever incoming edges
    for node_id, value in interventions.items():
        if node_id not in dag_mod:
            logger.warning("Intervention node '%s' not in DAG; skipping.", node_id)
            continue
        dag_mod.nodes[node_id]["prior_belief"] = float(value)
        dag_mod.nodes[node_id]["confidence"] = 1.0
        # Sever incoming edges (do-calculus: remove parents)
        parents = list(dag_mod.predecessors(node_id))
        for parent in parents:
            dag_mod.remove_edge(parent, node_id)

    dag_mod = propagator.propagate(dag_mod)
    modified_scores = _extract_outcome_scores(dag_mod)

    # --- Deltas ---
    deltas = {
        k: round(modified_scores[k] - original_scores[k], 4)
        for k in original_scores
    }

    # --- LLM explanation ---
    messages = [
        {
            "role": "system",
            "content": (
                "You are a college admissions causal reasoning expert. "
                "Explain the results of a what-if simulation in plain language. "
                "Be specific about which interventions drove which changes and why. "
                f"{language_instruction(response_language)}"
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
    explanation = await llm.complete(
        messages,
        temperature=0.5,
        max_tokens=768,
        caller="simulation.what_if.explain",
    )

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
        interventions = scenario.get("interventions", {})
        label = scenario.get("label", f"Scenario {i + 1}")

        result = await run_what_if(
            session, llm, student_id, uuid.UUID(str(school_id)), interventions
        )
        result["label"] = label
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
    summary = await llm.complete(
        messages,
        temperature=0.5,
        max_tokens=768,
        caller="simulation.compare_scenarios.summary",
    )

    return {
        "scenarios": results,
        "summary": summary,
    }


def _extract_outcome_scores(dag: Any) -> dict[str, float]:
    """Pull propagated beliefs for outcome nodes from a DAG."""
    scores: dict[str, float] = {}
    for node_id in _OUTCOME_NODES:
        if node_id in dag:
            scores[node_id] = round(
                dag.nodes[node_id].get("propagated_belief", 0.5), 4
            )
    return scores
