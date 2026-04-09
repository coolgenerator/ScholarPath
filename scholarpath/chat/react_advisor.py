"""ReAct-loop advisor agent using JSON-based tool dispatch + AsyncGenerator.

The advisor is an async generator that yields TurnEvent progress updates
and finally returns a TurnResult. This lets callers stream events to the
client as they happen rather than buffering through a callback.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import AsyncGenerator
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.api.models.chat import ChatBlock, TurnEvent, TurnResult
from scholarpath.chat.memory import ChatMemory
from scholarpath.chat.tools import TOOL_DEFINITIONS, ToolExecutor
from scholarpath.llm.client import LLMClient

logger = logging.getLogger(__name__)

MAX_TOOL_STEPS = 12

_ACTION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "description": "Tool name to call, or 'final_answer' to respond to the user.",
        },
        "args": {
            "type": "object",
            "description": "Arguments for the tool call. Omit for final_answer.",
        },
        "answer": {
            "type": "string",
            "description": "Your response to the user. Only used when action is 'final_answer'.",
        },
    },
    "required": ["action"],
}

# ── Tool filtering by phase + degree_level ─────────────────────────────

_ALWAYS_TOOLS = {"get_student_profile", "update_student_profile"}

_PHASE_TOOLS: dict[str, set[str]] = {
    "application": {
        "search_schools", "get_school_detail", "evaluate_school_fit",
        "get_school_list", "generate_recommendations", "generate_strategy",
        "lookup_school_scorecard", "lookup_program_earnings",
    },
    "decision": {
        "list_offers", "compare_offers", "run_what_if",
        "get_school_detail", "lookup_school_scorecard", "lookup_program_earnings",
        "evaluate_school_fit",
    },
    "waiting": {
        "get_school_list", "get_school_detail",
        "lookup_school_scorecard", "lookup_program_earnings",
        "list_offers",
    },
}


def _build_tool_block(tool_defs: list[dict[str, Any]]) -> str:
    lines = []
    for t in tool_defs:
        fn = t["function"]
        params = ", ".join(fn["parameters"].get("properties", {}).keys())
        lines.append(f"- {fn['name']}({params}): {fn['description']}")
    return "\n".join(lines)


def _filter_tools(phase: str | None) -> list[dict[str, Any]]:
    """Return tool definitions relevant to the given phase."""
    if phase is None:
        return TOOL_DEFINITIONS  # first call — show all
    allowed = _ALWAYS_TOOLS | _PHASE_TOOLS.get(phase, set())
    return [t for t in TOOL_DEFINITIONS if t["function"]["name"] in allowed]


# Full tool block for the initial system prompt (before phase is known)
_TOOL_BLOCK = _build_tool_block(TOOL_DEFINITIONS)

def _build_system_prompt(tool_block: str) -> str:
    return f"""\
You are ScholarPath Advisor, an expert US college admissions counselor.

TOOLS (call one at a time):
{tool_block}

RESPONSE FORMAT — always reply with exactly one JSON object:
To call a tool: {{"action": "<tool_name>", "args": {{<params>}}}}
To give final answer: {{"action": "final_answer", "answer": "<your complete response>"}}

PHASE AWARENESS:
The student profile includes a "phase" field. Adapt your analysis accordingly:

1. APPLICATION PHASE (phase="application"):
   Goal: build a balanced school list and application strategy.
   Key metrics: GPA/SAT fit vs school ranges, acceptance rate, admission probability,
   academic fit, ED/EA/RD strategy, tier balance (reach/target/safety).
   Key tools: search_schools, evaluate_school_fit, generate_recommendations,
   generate_strategy.

2. DECISION PHASE (phase="decision"):
   Goal: choose which admitted school to enroll in.
   IGNORE application metrics (GPA, SAT, acceptance rate, admission probability) —
   they no longer matter once admitted.
   Key metrics: net cost after aid, program strength and ranking for their major,
   career outcomes (employment rate, median salary, employer recruitment),
   PhD placement (faculty count, research funding, lab opportunities),
   location and cost of living, campus culture, alumni network,
   honors program benefits, financial aid sustainability (renewable?).
   Key tools: list_offers, compare_offers, run_what_if.
   Structure your comparison around: Cost → Career/Academic Outcomes → Lifestyle.

DEGREE LEVEL AWARENESS:
The student profile includes a "degree_level" field (undergraduate/masters/phd).
Adapt your analysis based on degree level:

1. UNDERGRADUATE (degree_level="undergraduate"):
   - Overall ranking matters more than program ranking.
   - Campus life, extracurriculars, and community fit are important.
   - SAT/ACT/GPA are primary academic metrics.
   - Career analysis: broad orientation scores (big_tech, startup, roi, etc.).

2. MASTERS (degree_level="masters"):
   - Program ranking > overall ranking.
   - ROI focus: expected salary uplift vs total cost (typically 1-2 years).
   - Co-op/internship availability and employer recruiting pipelines are key.
   - Industry location matters: proximity to target employers.
   - GRE may replace SAT; adjust academic metric references accordingly.
   - Alumni network strength in target career field.

3. PHD (degree_level="phd"):
   - Research fit is paramount: advisor matching, lab resources, publications.
   - Funding packages: stipend, tuition waiver, years guaranteed.
   - Faculty-to-student ratio and research opportunities are critical.
   - NSF/federal funding and nearby national labs signal research ecosystem.
   - Program ranking >> overall ranking.
   - City livability matters for 5+ year commitment.
   - Do NOT emphasize SAT, acceptance rate, or undergraduate metrics.

PROGRAM MATCHING:
After getting the student profile, you know their intended_majors. When evaluating
or recommending schools, ALWAYS check program availability:
- evaluate_school_fit returns "program_matches" and "warning" if a major is missing.
- If a school does NOT offer the student's intended major, flag this prominently
  in your response — it is a critical factor.
- Use search_schools with the "program" filter to find schools that offer specific
  majors (e.g. {{"action": "search_schools", "args": {{"program": "Data Science"}}}}).
- When comparing schools, mention program-level details: program ranking, co-op
  availability, and research opportunities for the student's specific major.

RULES:
- Always call get_student_profile first to detect the phase AND degree level.
- In decision phase, never reference GPA, SAT, or admission probability.
- If a metric is missing, use proxy indicators (e.g. faculty count and research
  funding as proxies for PhD placement quality).
- Be direct with concrete recommendations.
- Write final answer in the user's language (Chinese if they write Chinese).
- Never mention tools, JSON, or internals in your final answer.

DISAMBIGUATION:
When you encounter ambiguous data (e.g. a major like "计算机" or "CS" that could map to
multiple specific programs such as Computer Science, Computer Engineering, Data Science,
or Information Systems), include a disambiguation block in your final answer JSON:
{{"action": "final_answer", "answer": "<your response>", "disambiguation": {{
  "field": "major",
  "original_value": "<what user typed>",
  "title": "<ask user to clarify, in their language>",
  "options": [
    {{"label": "<option display name>", "value": "<canonical value>"}},
    ...
  ]
}}}}
Only include disambiguation when the ambiguity could meaningfully affect your advice
(e.g. different programs have different admission rates or career outcomes).
Do NOT disambiguate trivially clear inputs like "Computer Science" or "Economics".
"""


# Default prompt with full tool set (used for the initial call before phase is known)
SYSTEM_PROMPT = _build_system_prompt(_TOOL_BLOCK)


_TOOL_DISPLAY_NAMES: dict[str, str] = {
    "get_student_profile": "读取学生档案",
    "update_student_profile": "更新学生档案",
    "search_schools": "搜索学校",
    "get_school_detail": "查看学校详情",
    "evaluate_school_fit": "评估学校匹配度",
    "get_school_list": "获取选校清单",
    "generate_recommendations": "生成选校推荐",
    "generate_strategy": "制定申请策略",
    "list_offers": "查看录取结果",
    "compare_offers": "对比录取方案",
    "run_what_if": "模拟假设分析",
    "lookup_school_scorecard": "查询官方学校数据",
    "lookup_program_earnings": "查询专业薪资数据",
}


class ReactAdvisor:
    """JSON-based ReAct loop advisor implemented as an async generator."""

    def __init__(
        self,
        llm: LLMClient,
        session: AsyncSession,
        memory: ChatMemory,
    ) -> None:
        self._llm = llm
        self._session = session
        self._memory = memory

    async def run_turn(
        self,
        *,
        session_id: str,
        student_id: uuid.UUID | None,
        message: str,
    ) -> AsyncGenerator[TurnEvent | TurnResult, None]:
        """Execute one turn as an async generator.

        Yields ``TurnEvent`` for progress, then yields one final ``TurnResult``.
        """
        trace_id = str(uuid.uuid4())
        tool_executor = ToolExecutor(self._session, self._llm, student_id)

        yield TurnEvent(
            trace_id=trace_id,
            event="turn_started",
            data={
                "session_id": session_id,
                "step_id": f"turn-{trace_id[:8]}",
                "step_kind": "turn",
                "step_status": "running",
                "wave_index": 0,
            },
        )

        # Build conversation context from history
        history = await self._memory.get_history(session_id, limit=20)
        messages: list[dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT}]
        for entry in history:
            role = entry.get("role", "user")
            content = entry.get("content", "")
            if role in ("user", "assistant") and content:
                messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": message})

        # ReAct loop
        tool_calls_made = 0
        _phase_detected: str | None = None  # set after get_student_profile

        for step in range(MAX_TOOL_STEPS):
            try:
                decision = await self._llm.complete_json(
                    messages=messages,
                    schema=_ACTION_SCHEMA,
                    temperature=0.3,
                    max_tokens=4096,
                    caller="react_advisor",
                )
            except Exception:
                logger.exception("LLM call failed at step %d", step)
                break

            if not isinstance(decision, dict) or not decision:
                logger.warning("LLM returned empty/non-dict at step %d", step)
                break

            action = decision.get("action", "final_answer")

            # ── Final answer ──
            if action == "final_answer":
                final_content = str(decision.get("answer", ""))
                yield TurnEvent(
                    trace_id=trace_id,
                    event="turn_completed",
                    data={
                        "status": "ok",
                        "tool_calls": tool_calls_made,
                        "step_id": f"turn-{trace_id[:8]}",
                        "step_kind": "turn",
                        "step_status": "completed",
                        "wave_index": 0,
                    },
                )
                yield _build_result(trace_id, final_content, tool_calls_made)
                return

            # ── Tool call ──
            fn_name = action
            fn_args = decision.get("args", {})
            if not isinstance(fn_args, dict):
                fn_args = {}

            tool_calls_made += 1

            step_id = f"tool-{tool_calls_made}-{fn_name}"
            display_title = _TOOL_DISPLAY_NAMES.get(fn_name, fn_name)

            yield TurnEvent(
                trace_id=trace_id,
                event="capability_started",
                data={
                    "capability_id": fn_name,
                    "step": tool_calls_made,
                    "step_id": step_id,
                    "step_kind": "capability",
                    "step_status": "running",
                    "wave_index": 0,
                    "display": {"title": display_title},
                },
            )

            result_str = await tool_executor.execute(fn_name, fn_args)

            yield TurnEvent(
                trace_id=trace_id,
                event="capability_finished",
                data={
                    "capability_id": fn_name,
                    "step": tool_calls_made,
                    "step_id": step_id,
                    "step_kind": "capability",
                    "step_status": "completed",
                    "wave_index": 0,
                    "display": {"title": display_title},
                },
            )

            # Append action + result to conversation for next iteration
            messages.append({
                "role": "assistant",
                "content": json.dumps(decision, ensure_ascii=False),
            })
            messages.append({
                "role": "user",
                "content": f"[Tool result for {fn_name}]:\n{result_str}",
            })

            # After get_student_profile: filter tools + inject planning nudge
            if fn_name == "get_student_profile" and _phase_detected is None:
                try:
                    profile_data = json.loads(result_str)
                    _phase_detected = profile_data.get("phase", "application")
                    filtered = _filter_tools(_phase_detected)
                    messages[0] = {
                        "role": "system",
                        "content": _build_system_prompt(_build_tool_block(filtered)),
                    }
                    logger.info(
                        "Phase detected: %s — filtered tools to %d/%d",
                        _phase_detected, len(filtered), len(TOOL_DEFINITIONS),
                    )
                except (json.JSONDecodeError, TypeError):
                    pass

                # Planning nudge: ask LLM to outline approach before acting
                messages.append({
                    "role": "user",
                    "content": (
                        "[System] Now that you have the student profile, briefly plan your approach "
                        "in 1-2 sentences inside the 'answer' field before executing tools. "
                        "Then proceed with tool calls step by step."
                    ),
                })

        # ── Exhausted steps or LLM error — force a final answer ──
        logger.warning(
            "ReAct loop ended after %d tool calls for session %s",
            tool_calls_made, session_id,
        )
        messages.append({
            "role": "user",
            "content": (
                "Please provide your final answer now based on all information gathered. "
                'Respond with: {"action": "final_answer", "answer": "..."}'
            ),
        })
        try:
            decision = await self._llm.complete_json(
                messages=messages,
                schema=_ACTION_SCHEMA,
                temperature=0.3,
                max_tokens=4096,
                caller="react_advisor.final",
            )
            final_content = str(decision.get("answer", decision.get("content", "")))
        except Exception:
            logger.exception("Final answer call failed")
            final_content = ""

        yield TurnEvent(
            trace_id=trace_id,
            event="turn_completed",
            data={
                "status": "ok",
                "tool_calls": tool_calls_made,
                "step_id": f"turn-{trace_id[:8]}",
                "step_kind": "turn",
                "step_status": "completed",
                "wave_index": 0,
            },
        )
        yield _build_result(trace_id, final_content, tool_calls_made)


def _build_result(trace_id: str, content: str, tool_calls: int) -> TurnResult:
    if not content:
        content = "Sorry, I wasn't able to complete the analysis. Please try again."
    blocks = [
        ChatBlock(
            id=str(uuid.uuid4()),
            kind="text",
            capability_id="advisor",
            order=0,
            payload={"text": content},
        )
    ]
    return TurnResult(
        trace_id=trace_id,
        status="ok",
        content=content,
        blocks=blocks,
        actions=[],
        usage={"tool_calls": tool_calls},
    )
