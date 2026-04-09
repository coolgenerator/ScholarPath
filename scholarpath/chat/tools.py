"""Tool definitions and handlers for the ReAct advisor agent."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.db.models.school import School
from scholarpath.llm.client import LLMClient

logger = logging.getLogger(__name__)


def _derive_tuition_tier(
    citizenship: str | None,
    residency_state: str | None,
    school_state: str | None = None,
) -> str:
    """Derive the applicable tuition tier from citizenship and residency.

    Returns one of: "international", "in_state", "out_of_state".
    If *school_state* is provided, checks whether the student qualifies for
    in-state tuition at that specific school.
    """
    country = (citizenship or "").strip().upper()
    if not country or country not in ("US", "PR", "GU", "VI", "AS", "MP"):
        return "international"
    # US citizen / permanent resident
    state = (residency_state or "").strip().upper()
    if school_state and state == school_state.strip().upper():
        return "in_state"
    if not school_state:
        # Generic — we know they're domestic but don't know the school yet
        return "in_state" if state else "out_of_state"
    return "out_of_state"

# ---------------------------------------------------------------------------
# OpenAI-format tool definitions
# ---------------------------------------------------------------------------

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_student_profile",
            "description": (
                "Retrieve the current student's full profile including GPA, test scores, "
                "intended majors, budget, extracurriculars, awards, preferences, and "
                "current phase (application/decision/waiting based on existing offers)."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "update_student_profile",
            "description": (
                "Update one or more fields on the student profile. "
                "Supported fields: name, gpa, gpa_scale, sat_total, sat_rw, sat_math, "
                "act_composite, toefl_total, curriculum_type, intended_majors (list), "
                "citizenship (ISO country code like CN/US/IN), residency_state (US state if US citizen), "
                "budget_usd, target_year, degree_level (undergraduate/masters/phd), "
                "preferences (dict), extracurriculars (dict), "
                "awards (dict), need_financial_aid (bool), ed_preference."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "updates": {
                        "type": "object",
                        "description": "Key-value pairs of fields to update.",
                    },
                },
                "required": ["updates"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_schools",
            "description": (
                "Search for schools by name, state, type, rank range, cost range, or program/major. "
                "Use the 'program' filter to find schools that offer a specific major. "
                "Returns a list of matching schools with key stats."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "q": {"type": "string", "description": "Free-text name search."},
                    "state": {"type": "string", "description": "US state abbreviation (e.g. 'CA')."},
                    "school_type": {
                        "type": "string",
                        "enum": ["university", "lac", "technical"],
                        "description": "Filter by school type.",
                    },
                    "program": {"type": "string", "description": "Filter by program/major name (e.g. 'Data Science', 'ECE')."},
                    "min_rank": {"type": "integer", "description": "Minimum US News rank."},
                    "max_rank": {"type": "integer", "description": "Maximum US News rank."},
                    "limit": {"type": "integer", "description": "Max results to return (default 10)."},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_school_detail",
            "description": (
                "Get detailed information about a specific school including programs, "
                "admission stats, costs, and available data points."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "school_id": {"type": "string", "description": "UUID of the school."},
                },
                "required": ["school_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "evaluate_school_fit",
            "description": (
                "Compute a multi-dimensional fit evaluation for the student against a specific school. "
                "Returns scores for academic_fit, financial_fit, career_fit, life_fit, "
                "overall_score, admission_probability, and tier classification."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "school_id": {"type": "string", "description": "UUID of the school to evaluate."},
                },
                "required": ["school_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_school_list",
            "description": (
                "Get the student's current school list grouped by tier "
                "(reach / target / safety / likely) with evaluation scores."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_recommendations",
            "description": (
                "Generate personalized school recommendations for the student based on "
                "their profile, budget, and preferences. Uses vector similarity and "
                "multi-dimensional scoring. Returns a ranked list with reasoning."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "preference_hints": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Additional preference hints (e.g. 'strong research', 'warm climate').",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_strategy",
            "description": (
                "Generate an application strategy for the student including "
                "ED/EA/RD recommendations, timeline, and risk assessment."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_offers",
            "description": (
                "List all admission offers the student has received, including "
                "status, costs, financial aid, and net cost."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_offers",
            "description": (
                "Side-by-side comparison of admitted/committed offers with "
                "scoring and recommendation narrative."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_what_if",
            "description": (
                "Run a what-if simulation: what would change if the student's "
                "profile metrics were different? Specify a school and the hypothetical changes."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "school_id": {"type": "string", "description": "UUID of the school."},
                    "interventions": {
                        "type": "object",
                        "description": (
                            "Hypothetical changes as key-value pairs. "
                            "Keys: sat_total, gpa, toefl_total, budget_usd. "
                            "Values: the hypothetical new value."
                        ),
                    },
                },
                "required": ["school_id", "interventions"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "lookup_school_scorecard",
            "description": (
                "Look up real-time school data from the US Dept of Education College Scorecard. "
                "Returns official tuition (in-state/out-of-state), acceptance rate, SAT/ACT ranges, "
                "graduation rate, average net price, and median earnings after graduation."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "school_name": {"type": "string", "description": "School name to look up."},
                },
                "required": ["school_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "lookup_program_earnings",
            "description": (
                "Look up earnings data by major/program at a specific school from College Scorecard. "
                "Returns median salary 3-4 years after graduation for each program offered."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "school_name": {"type": "string", "description": "School name."},
                },
                "required": ["school_name"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Tool executor — dispatches tool calls to service layer
# ---------------------------------------------------------------------------

class ToolExecutor:
    """Executes tool calls by delegating to the service layer.

    Includes a turn-level cache: identical (name, args) pairs within
    the same turn are served from cache instead of re-executing.
    """

    # Tools that are safe to cache within a turn (read-only, deterministic)
    _CACHEABLE_TOOLS = frozenset({
        "get_student_profile", "get_school_detail", "get_school_list",
        "list_offers", "lookup_school_scorecard", "lookup_program_earnings",
    })

    def __init__(
        self,
        session: AsyncSession,
        llm: LLMClient,
        student_id: uuid.UUID | None,
    ) -> None:
        self._session = session
        self._llm = llm
        self._student_id = student_id
        self._turn_cache: dict[str, str] = {}

    async def execute(self, name: str, arguments: dict[str, Any]) -> str:
        """Run a tool and return a JSON string result.

        If the tool is cacheable and was already called with identical args
        in this turn, returns the cached result immediately.
        """
        # Check turn-level cache for cacheable tools
        if name in self._CACHEABLE_TOOLS:
            cache_key = f"{name}:{json.dumps(arguments, sort_keys=True, default=str)}"
            cached = self._turn_cache.get(cache_key)
            if cached is not None:
                logger.debug("Cache hit for %s", name)
                return cached

        try:
            result = await self._dispatch(name, arguments)
            result_str = json.dumps(result, ensure_ascii=False, default=str)
        except Exception as exc:
            logger.exception("Tool %s failed", name)
            result_str = json.dumps({"error": str(exc)}, ensure_ascii=False)

        # Store in cache for cacheable tools
        if name in self._CACHEABLE_TOOLS:
            self._turn_cache[cache_key] = result_str  # noqa: F821 — cache_key always set above

        return result_str

    async def _dispatch(self, name: str, args: dict[str, Any]) -> Any:
        if name == "get_student_profile":
            return await self._get_student_profile()
        if name == "update_student_profile":
            return await self._update_student_profile(args.get("updates", {}))
        if name == "search_schools":
            return await self._search_schools(args)
        if name == "get_school_detail":
            return await self._get_school_detail(args["school_id"])
        if name == "evaluate_school_fit":
            return await self._evaluate_school_fit(args["school_id"])
        if name == "get_school_list":
            return await self._get_school_list()
        if name == "generate_recommendations":
            return await self._generate_recommendations(args.get("preference_hints"))
        if name == "generate_strategy":
            return await self._generate_strategy()
        if name == "list_offers":
            return await self._list_offers()
        if name == "compare_offers":
            return await self._compare_offers()
        if name == "run_what_if":
            return await self._run_what_if(args["school_id"], args["interventions"])
        if name == "lookup_school_scorecard":
            return await self._lookup_school_scorecard(args["school_name"])
        if name == "lookup_program_earnings":
            return await self._lookup_program_earnings(args["school_name"])
        raise ValueError(f"Unknown tool: {name}")

    def _require_student(self) -> uuid.UUID:
        if self._student_id is None:
            raise ValueError("No student profile linked to this session.")
        return self._student_id

    # -- Handlers --

    async def _get_student_profile(self) -> dict:
        from scholarpath.services.student_service import get_student, check_profile_completeness
        from scholarpath.services.offer_service import list_offers
        sid = self._require_student()
        student = await get_student(self._session, sid)
        completeness = await check_profile_completeness(student)

        # Determine application phase from offers
        offers = await list_offers(self._session, sid)
        admitted_count = sum(1 for o in offers if o.status in ("admitted", "committed"))
        pending_count = sum(1 for o in offers if o.status in ("waitlisted", "deferred"))
        if admitted_count > 0:
            phase = "decision"
            phase_detail = f"{admitted_count} admitted offer(s), {pending_count} pending"
        elif len(offers) > 0:
            phase = "waiting"
            phase_detail = f"{len(offers)} offer(s), all pending/denied"
        else:
            phase = "application"
            phase_detail = "No offers yet — focus on building school list and strategy"

        return {
            "id": str(student.id),
            "name": student.name,
            "degree_level": getattr(student, "degree_level", "undergraduate"),
            "gpa": student.gpa,
            "gpa_scale": student.gpa_scale,
            "sat_total": student.sat_total,
            "sat_rw": student.sat_rw,
            "sat_math": student.sat_math,
            "act_composite": student.act_composite,
            "toefl_total": student.toefl_total,
            "curriculum_type": student.curriculum_type,
            "intended_majors": student.intended_majors,
            "budget_usd": student.budget_usd,
            "target_year": student.target_year,
            "need_financial_aid": student.need_financial_aid,
            "extracurriculars": student.extracurriculars,
            "awards": student.awards,
            "preferences": student.preferences,
            "ed_preference": student.ed_preference,
            "citizenship": student.citizenship,
            "residency_state": student.residency_state,
            "tuition_tier": _derive_tuition_tier(student.citizenship, student.residency_state),
            "profile_completed": student.profile_completed,
            "completeness": completeness,
            "phase": phase,
            "phase_detail": phase_detail,
        }

    async def _update_student_profile(self, updates: dict) -> dict:
        from scholarpath.services.student_service import update_student
        sid = self._require_student()
        student = await update_student(self._session, sid, updates)
        return {"status": "updated", "updated_fields": list(updates.keys())}

    async def _search_schools(self, filters: dict) -> list[dict]:
        from scholarpath.services.school_service import search_schools
        schools = await search_schools(self._session, filters)
        return [
            {
                "id": str(s.id),
                "name": s.name,
                "name_cn": s.name_cn,
                "state": s.state,
                "school_type": s.school_type,
                "us_news_rank": s.us_news_rank,
                "acceptance_rate": s.acceptance_rate,
                "tuition_in_state": s.tuition_in_state,
                "tuition_oos": s.tuition_oos,
                "tuition_intl": s.tuition_intl,
                "avg_net_price": s.avg_net_price,
                "sat_25": s.sat_25,
                "sat_75": s.sat_75,
            }
            for s in schools[:int(filters.get("limit", 10))]
        ]

    async def _get_school_detail(self, school_id: str) -> dict:
        from scholarpath.services.school_service import get_school_detail
        return await get_school_detail(self._session, uuid.UUID(school_id))

    async def _evaluate_school_fit(self, school_id: str) -> dict:
        from scholarpath.services.evaluation_service import evaluate_school_fit
        from scholarpath.services.student_service import get_student
        sid = self._require_student()
        ev = await evaluate_school_fit(self._session, self._llm, sid, uuid.UUID(school_id))

        # Check program match for the student's intended majors
        student = await get_student(self._session, sid)
        majors = student.intended_majors or []
        school = await self._session.get(
            School, uuid.UUID(school_id),
            options=[selectinload(School.programs)],
        )
        program_matches = []
        missing_majors = []
        if school and school.programs and majors:
            for major in majors:
                ml = major.lower()
                match = next(
                    (p for p in school.programs
                     if ml in p.name.lower() or ml in p.department.lower()),
                    None,
                )
                if match:
                    program_matches.append({
                        "major": major,
                        "program_name": match.name,
                        "program_rank": match.us_news_rank,
                        "has_research": match.has_research_opps,
                        "has_coop": match.has_coop,
                    })
                else:
                    missing_majors.append(major)

        result = {
            "school_id": str(ev.school_id),
            "tier": ev.tier,
            "academic_fit": ev.academic_fit,
            "financial_fit": ev.financial_fit,
            "career_fit": ev.career_fit,
            "life_fit": ev.life_fit,
            "overall_score": ev.overall_score,
            "admission_probability": ev.admission_probability,
            "ed_ea_recommendation": ev.ed_ea_recommendation,
            "orientation_scores": (ev.fit_details or {}).get("orientation_scores"),
            "reasoning": ev.reasoning,
            "program_matches": program_matches,
        }
        if missing_majors:
            result["warning"] = f"This school does NOT appear to offer: {', '.join(missing_majors)}"
        return result

    async def _get_school_list(self) -> dict:
        from scholarpath.services.evaluation_service import get_tiered_list
        sid = self._require_student()
        tiers = await get_tiered_list(self._session, sid)
        result = {}
        for tier, evals in tiers.items():
            result[tier] = [
                {
                    "school_name": ev.school.name if ev.school else str(ev.school_id),
                    "overall_score": ev.overall_score,
                    "admission_probability": ev.admission_probability,
                    "academic_fit": ev.academic_fit,
                    "financial_fit": ev.financial_fit,
                }
                for ev in evals
            ]
        return result

    async def _generate_recommendations(self, hints: list[str] | None) -> dict:
        from scholarpath.services.recommendation_service import generate_recommendations
        sid = self._require_student()
        return await generate_recommendations(
            self._session, self._llm, sid,
            response_language="en",
            preference_hints=hints,
        )

    async def _generate_strategy(self) -> dict:
        from scholarpath.services.evaluation_service import generate_strategy
        sid = self._require_student()
        return await generate_strategy(self._session, self._llm, sid)

    async def _list_offers(self) -> list[dict]:
        from scholarpath.services.offer_service import list_offers
        sid = self._require_student()
        offers = await list_offers(self._session, sid)
        return [
            {
                "id": str(o.id),
                "school_name": o.school.name if o.school else str(o.school_id),
                "program": o.program,
                "status": o.status,
                "tuition": o.tuition,
                "total_cost": o.total_cost,
                "total_aid": o.total_aid,
                "net_cost": o.net_cost,
                "merit_scholarship": o.merit_scholarship,
                "need_based_grant": o.need_based_grant,
                "honors_program": o.honors_program,
                "decision_deadline": str(o.decision_deadline) if o.decision_deadline else None,
                "notes": o.notes,
            }
            for o in offers
        ]

    async def _compare_offers(self) -> dict:
        from scholarpath.services.offer_service import compare_offers
        sid = self._require_student()
        return await compare_offers(self._session, self._llm, sid, response_language="en")

    async def _run_what_if(self, school_id: str, interventions: dict) -> dict:
        from scholarpath.services.simulation_service import run_what_if
        sid = self._require_student()
        float_interventions = {k: float(v) for k, v in interventions.items()}
        return await run_what_if(
            self._session, self._llm, sid,
            uuid.UUID(school_id), float_interventions,
            response_language="en",
        )

    async def _lookup_school_scorecard(self, school_name: str) -> dict:
        from scholarpath.services.scorecard_service import get_school_by_name
        result = await get_school_by_name(school_name)
        if not result:
            return {"error": f"School '{school_name}' not found in College Scorecard."}
        return result

    async def _lookup_program_earnings(self, school_name: str) -> list[dict]:
        from scholarpath.services.scorecard_service import get_programs_by_school
        programs = await get_programs_by_school(school_name)
        if not programs:
            return [{"error": f"No program data found for '{school_name}'."}]
        # Return top programs by earnings, filter out nulls
        valid = [p for p in programs if p.get("earnings_3yr_median") or p.get("earnings_4yr_median")]
        valid.sort(key=lambda p: p.get("earnings_4yr_median") or p.get("earnings_3yr_median") or 0, reverse=True)
        return valid[:20]
