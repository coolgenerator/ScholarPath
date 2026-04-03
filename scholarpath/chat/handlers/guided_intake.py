"""Guided step-by-step preference collection handler.

Replaces the basic profile_intake with a structured, multi-step
preference collection flow that walks the student through each
category of information needed for recommendations.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.language import (
    ResponseLanguage,
    detect_response_language,
    select_localized_text,
)
from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient
from scholarpath.services.portfolio_service import apply_portfolio_patch
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Structured options for interactive question cards
# ---------------------------------------------------------------------------

STEP_OPTIONS: dict[str, Any | None] = {
    "academics": None,  # Free-form input, no preset options
    "major_career": {
        "questions": [
            {
                "id": "intended_major",
                "title_en": "What major interests you?",
                "title_zh": "你对什么专业感兴趣？",
                "options": [
                    {"label": "Computer Science", "value": "Computer Science", "icon": "computer"},
                    {"label": "Data Science", "value": "Data Science", "icon": "analytics"},
                    {"label": "Electrical Engineering", "value": "Electrical Engineering", "icon": "memory"},
                    {"label": "Mathematics", "value": "Mathematics", "icon": "functions"},
                    {"label": "Physics", "value": "Physics", "icon": "science"},
                    {"label": "Business / Finance", "value": "Business", "icon": "account_balance"},
                    {"label": "Biology / Pre-Med", "value": "Biology", "icon": "biotech"},
                    {"label": "Economics", "value": "Economics", "icon": "trending_up"},
                ],
                "allow_custom": True,
                "custom_placeholder_en": "Or type your major...",
                "custom_placeholder_zh": "或输入你的专业...",
                "multi_select": True,
            },
            {
                "id": "career_goal",
                "title_en": "What's your career goal after graduation?",
                "title_zh": "毕业后的职业目标是什么？",
                "options": [
                    {"label": "\U0001f3e2 Big Tech (FAANG)", "value": "big_tech"},
                    {"label": "\U0001f393 PhD / Research", "value": "phd"},
                    {"label": "\U0001f680 Startup", "value": "startup"},
                    {"label": "\U0001f4bc Finance / Consulting", "value": "finance"},
                    {"label": "\U0001f393 Master's first", "value": "masters_then_work"},
                ],
                "allow_custom": True,
                "custom_placeholder_en": "Or describe your goal...",
                "custom_placeholder_zh": "或描述你的目标...",
            },
        ],
    },
    "activities": None,  # Free-form
    "location_culture": {
        "questions": [
            {
                "id": "location",
                "title_en": "Preferred location?",
                "title_zh": "偏好的地理位置？",
                "options": [
                    {"label": "\U0001f334 West Coast (CA, WA)", "value": "west_coast"},
                    {"label": "\U0001f5fd Northeast (MA, NY, PA)", "value": "northeast"},
                    {"label": "\U0001f33e Midwest (IL, MI, WI)", "value": "midwest"},
                    {"label": "\U0001f33a South (GA, TX, FL)", "value": "south"},
                    {"label": "\U0001f30e No preference", "value": "no_preference"},
                ],
                "multi_select": True,
                "allow_custom": False,
            },
            {
                "id": "campus_setting",
                "title_en": "Campus setting?",
                "title_zh": "校园环境偏好？",
                "options": [
                    {"label": "\U0001f3d9\ufe0f Urban / City", "value": "urban"},
                    {"label": "\U0001f3d8\ufe0f Suburban", "value": "suburban"},
                    {"label": "\U0001f333 College Town", "value": "college_town"},
                    {"label": "\U0001f30e No preference", "value": "no_preference"},
                ],
                "allow_custom": False,
            },
            {
                "id": "culture",
                "title_en": "What campus culture matters to you?",
                "title_zh": "你看重什么校园文化？",
                "options": [
                    {"label": "\U0001f30d Diverse & Multicultural", "value": "diverse"},
                    {"label": "\U0001f52c Research-focused", "value": "research"},
                    {"label": "\U0001f389 Active Social Scene", "value": "social"},
                    {"label": "\U0001f91d Strong Chinese Community", "value": "chinese_community"},
                    {"label": "\U0001f4bc Career-oriented", "value": "career_oriented"},
                ],
                "multi_select": True,
                "allow_custom": True,
                "custom_placeholder_en": "Other preferences...",
                "custom_placeholder_zh": "其他偏好...",
            },
        ],
    },
    "financial": {
        "questions": [
            {
                "id": "budget",
                "title_en": "Annual budget (tuition + living)?",
                "title_zh": "每年预算（学费+生活费）？",
                "options": [
                    {"label": "< $30K", "value": "30000"},
                    {"label": "$30K - $45K", "value": "45000"},
                    {"label": "$45K - $60K", "value": "55000"},
                    {"label": "$60K - $75K", "value": "70000"},
                    {"label": "$75K+", "value": "80000"},
                ],
                "allow_custom": True,
                "custom_placeholder_en": "Enter exact amount...",
                "custom_placeholder_zh": "输入具体金额...",
            },
            {
                "id": "financial_aid",
                "title_en": "Do you need financial aid?",
                "title_zh": "是否需要奖学金/助学金？",
                "options": [
                    {"label": "\u2705 Yes, need-based aid", "value": "need_based"},
                    {"label": "\U0001f3c6 Merit scholarship preferred", "value": "merit"},
                    {"label": "\U0001f4b0 Both types welcome", "value": "both"},
                    {"label": "\u274c No, budget is sufficient", "value": "no"},
                ],
                "allow_custom": False,
            },
        ],
    },
    "school_preferences": {
        "questions": [
            {
                "id": "school_size",
                "title_en": "Preferred school type?",
                "title_zh": "偏好的学校类型？",
                "options": [
                    {"label": "\U0001f3db\ufe0f Large Research University", "value": "large_university"},
                    {"label": "\U0001f4da Medium University", "value": "medium_university"},
                    {"label": "\U0001f393 Small Liberal Arts College", "value": "lac"},
                    {"label": "\u2699\ufe0f Technical Institute", "value": "technical"},
                    {"label": "\U0001f30e No preference", "value": "no_preference"},
                ],
                "allow_custom": False,
            },
        ],
    },
    "strategy": {
        "questions": [
            {
                "id": "ed_ea",
                "title_en": "Early application strategy?",
                "title_zh": "早申策略？",
                "options": [
                    {"label": "\U0001f3af ED (binding, one school)", "value": "ed"},
                    {"label": "\u26a1 EA (non-binding, multiple)", "value": "ea"},
                    {"label": "\U0001f512 REA (restrictive early action)", "value": "rea"},
                    {"label": "\U0001f4cb RD only (no early apps)", "value": "rd"},
                    {"label": "\U0001f914 Not sure yet", "value": "undecided"},
                ],
                "allow_custom": True,
                "custom_placeholder_en": "Or tell me your thinking...",
                "custom_placeholder_zh": "或说说你的想法...",
            },
        ],
    },
}

# ---------------------------------------------------------------------------
# Intake steps definition
# ---------------------------------------------------------------------------

INTAKE_STEPS: list[dict[str, Any]] = [
    {
        "id": "academics",
        "question_en": (
            "Let's start with your academic profile. What's your GPA and test scores? "
            "(SAT/ACT, TOEFL if applicable)"
        ),
        "question_zh": (
            "先聊聊你的学术背景吧。你的GPA和标化成绩是多少？"
            "(SAT/ACT，如果有的话TOEFL也说一下)"
        ),
        "fields": ["gpa", "sat_total", "toefl_total", "curriculum_type", "ap_courses"],
    },
    {
        "id": "major_career",
        "question_en": (
            "What major(s) are you interested in? And what's your career goal "
            "-- industry job, PhD, startup?"
        ),
        "question_zh": (
            "你对什么专业感兴趣？毕业后的目标是什么——工作、读博、还是创业？"
        ),
        "fields": ["intended_majors"],
        "context_key": "career_goal",
    },
    {
        "id": "activities",
        "question_en": (
            "Tell me about your extracurriculars, leadership roles, and any "
            "awards or competitions."
        ),
        "question_zh": (
            "说说你的课外活动、领导经历，还有获奖和竞赛情况吧。"
        ),
        "fields": ["extracurriculars", "awards"],
    },
    {
        "id": "location_culture",
        "question_en": (
            "Do you have preferences for location? (East/West Coast, urban/suburban, "
            "climate?) What kind of campus culture appeals to you?"
        ),
        "question_zh": (
            "你对学校位置有偏好吗？(东海岸/西海岸，城市/郊区，气候？) "
            "你喜欢什么样的校园文化？"
        ),
        "fields": [],
        "context_key": "location_culture",
    },
    {
        "id": "financial",
        "question_en": (
            "What's your annual budget for tuition and living? "
            "Do you need financial aid or merit scholarships?"
        ),
        "question_zh": (
            "你每年的学费和生活费预算大概多少？需要申请奖学金或助学金吗？"
        ),
        "fields": ["budget_usd", "need_financial_aid"],
    },
    {
        "id": "school_preferences",
        "question_en": (
            "Any preferences on school size (small LAC vs large university), "
            "research vs teaching focus, or specific schools you're considering?"
        ),
        "question_zh": (
            "你对学校规模有偏好吗？(小型文理学院 vs 大型综合大学？) "
            "更看重科研还是教学？有没有特别想去的学校？"
        ),
        "fields": [],
        "context_key": "school_size_type",
    },
    {
        "id": "strategy",
        "question_en": (
            "Are you considering Early Decision (ED) or Early Action (EA)? "
            "Any schools you'd want to apply ED to?"
        ),
        "question_zh": (
            "你有考虑早申(ED/EA)吗？有没有特别想ED的学校？"
        ),
        "fields": ["ed_preference"],
        "context_key": "ed_strategy",
    },
]

# ---------------------------------------------------------------------------
# LLM extraction prompt
# ---------------------------------------------------------------------------

_EXTRACTION_SYSTEM_PROMPT = """\
You are a college-admissions data extractor. The student is answering questions
in a guided intake flow. Extract ALL relevant data from their message.

IMPORTANT:
- The student may provide information for the CURRENT step AND future steps.
  Extract EVERYTHING you find, not just the current step.
- If a field is not mentioned, set it to null.
- Respond ONLY with valid JSON -- no markdown fences, no commentary.
- The user may write in Chinese or English. Preserve values in the original language.
- Numeric fields (gpa, sat_total, act_composite, toefl_total, budget_usd) must be numbers or null.
- For budget: convert to annual USD integer (e.g. "60k" -> 60000, "5万美金" -> 50000).
- For need_financial_aid: boolean or null.
- List fields must be JSON arrays.

Output schema:
{
  "gpa": <number | null>,
  "gpa_scale": <string | null>,
  "sat_total": <number | null>,
  "act_composite": <number | null>,
  "toefl_total": <number | null>,
  "curriculum_type": <string | null>,
  "ap_courses": <[string] | null>,
  "intended_majors": <[string] | null>,
  "career_goal": <string | null>,
  "extracurriculars": <[string or object] | null>,
  "awards": <[string or object] | null>,
  "location": <[string] | string | null>,
  "culture": <[string] | string | null>,
  "budget_usd": <number | null>,
  "need_financial_aid": <boolean | null>,
  "financial_aid_type": <"need_based" | "merit" | "both" | "no" | null>,
  "size": <[string] | string | null>,
  "research_vs_teaching": <string | null>,
  "target_schools": <[string] | null>,
  "ed_preference": <string | null>,
  "completed_step_ids": <[string]>
}

The "completed_step_ids" field MUST list the IDs of ALL intake steps that the
student has provided sufficient information for. The step IDs are:
  academics, major_career, activities, location_culture, financial,
  school_preferences, strategy
"""


def _format_extraction_prompt(
    message: str,
    current_step: dict[str, Any],
    conversation_context: str,
) -> str:
    """Build the user prompt for guided extraction."""
    return (
        f"Current intake step: {current_step['id']}\n"
        f"Question asked: {current_step['question_en']}\n\n"
        f"Recent conversation:\n{conversation_context}\n\n"
        f"Student's latest message:\n{message}"
    )


# ---------------------------------------------------------------------------
# Field mapping from LLM output to Student model
# ---------------------------------------------------------------------------

_DIRECT_FIELD_MAP: dict[str, tuple[str, str]] = {
    "gpa": ("academics", "gpa"),
    "gpa_scale": ("academics", "gpa_scale"),
    "sat_total": ("academics", "sat_total"),
    "act_composite": ("academics", "act_composite"),
    "toefl_total": ("academics", "toefl_total"),
    "curriculum_type": ("academics", "curriculum_type"),
    "ap_courses": ("academics", "ap_courses"),
    "intended_majors": ("academics", "intended_majors"),
    "extracurriculars": ("activities", "extracurriculars"),
    "awards": ("activities", "awards"),
    "budget_usd": ("finance", "budget_usd"),
    "need_financial_aid": ("finance", "need_financial_aid"),
    "ed_preference": ("strategy", "ed_preference"),
}

# Keys stored into Student.preferences JSON dict
_PREFERENCE_KEYS = [
    "interests",
    "risk_preference",
    "cost_priority",
    "location",
    "size",
    "culture",
    "career_goal",
    "financial_aid_type",
    "research_vs_teaching",
    "target_schools",
    "ui_preference_tags",
]

# Map step IDs to the fields they cover for skip detection
_STEP_REQUIRED_FIELDS: dict[str, list[str]] = {
    "academics": ["gpa", "sat_total", "act_composite"],
    "major_career": ["intended_majors", "career_goal"],
    "activities": ["extracurriculars", "awards"],
    "location_culture": ["location", "location_preference", "culture", "campus_culture"],
    "financial": ["budget_usd"],
    "school_preferences": ["size", "school_size_preference", "research_vs_teaching", "target_schools"],
    "strategy": ["ed_preference"],
}


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

async def handle_guided_intake(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    session_id: str,
    student_id: uuid.UUID,
    message: str,
) -> str:
    """Handle a message in the guided intake flow.

    Tracks which step the user is on, extracts data for the current (and
    potentially future) steps, updates the Student model, and advances
    to the next unanswered step.

    When all steps are complete, returns a completion signal so the caller
    can trigger recommendation generation.

    Parameters
    ----------
    llm:
        LLM client for extraction calls.
    session:
        SQLAlchemy async session.
    memory:
        Redis-backed conversation memory.
    student_id:
        UUID of the student.
    message:
        The user's latest message.

    Returns
    -------
    str
        Response text. If all steps are done the response starts with
        ``[INTAKE_COMPLETE]`` so the agent can detect completion.
    """
    response_lang = detect_response_language(message)

    # --- Determine current step ---
    context = await memory.get_context(session_id)
    step_index = context.get("intake_step", 0)
    if not isinstance(step_index, int) or step_index < 0:
        step_index = 0

    # Clamp to valid range
    if step_index >= len(INTAKE_STEPS):
        step_index = len(INTAKE_STEPS) - 1

    current_step = INTAKE_STEPS[step_index]

    # --- Build conversation context for extraction ---
    history = await memory.get_history(session_id, limit=6)
    conversation_text = "\n".join(
        f"{m['role'].capitalize()}: {m['content']}" for m in history
    )

    # --- Run LLM extraction ---
    user_prompt = _format_extraction_prompt(message, current_step, conversation_text)
    messages = [
        {"role": "system", "content": _EXTRACTION_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    try:
        extracted = await llm.complete_json(
            messages,
            temperature=0.2,
            max_tokens=1024,
            caller="chat.guided_intake",
        )
    except Exception:
        logger.warning("Guided intake extraction failed", exc_info=True)
        return select_localized_text(
            "我刚才没完全理解你的意思，可以换种说法再试一次吗？",
            "I had trouble understanding that. Could you try again?",
            response_lang,
            mixed="我刚才没完全理解你的意思，可以换种说法再试一次吗？\nI had trouble understanding that. Could you try again?",
        )

    # --- Update student portfolio with grouped patch contract ---
    portfolio_patch: dict[str, Any] = {}
    for llm_key, (group_key, field_key) in _DIRECT_FIELD_MAP.items():
        value = extracted.get(llm_key)
        if value is not None:
            # Coerce budget to int
            if llm_key == "budget_usd" and isinstance(value, str):
                parsed = _parse_budget(value)
                if parsed is not None:
                    portfolio_patch.setdefault(group_key, {})[field_key] = parsed
            else:
                portfolio_patch.setdefault(group_key, {})[field_key] = value

    need_aid, financial_aid_type = _normalize_financial_aid(
        extracted.get("need_financial_aid"),
        extracted.get("financial_aid_type", extracted.get("financial_aid")),
    )
    if need_aid is not None:
        portfolio_patch.setdefault("finance", {})["need_financial_aid"] = need_aid

    # --- Update canonical preference keys ---
    preferences_patch: dict[str, Any] = {}
    for pkey in _PREFERENCE_KEYS:
        value = _extract_preference_value(extracted, pkey)
        if value is not None:
            if pkey in {"interests", "location", "size", "culture", "target_schools", "ui_preference_tags"}:
                value = _as_string_list(value)
            preferences_patch[pkey] = value

    if financial_aid_type is not None:
        preferences_patch["financial_aid_type"] = financial_aid_type

    if preferences_patch:
        portfolio_patch["preferences"] = preferences_patch

    if portfolio_patch:
        await apply_portfolio_patch(session, student_id, portfolio_patch)
        await memory.save_context(session_id, "last_extracted", portfolio_patch)

    student = await get_student(session, student_id)

    # --- Determine which steps to skip ---
    completed_step_ids: list[str] = extracted.get("completed_step_ids", [])
    if not isinstance(completed_step_ids, list):
        completed_step_ids = []
    completed_step_ids = [sid for sid in completed_step_ids if isinstance(sid, str)]

    # Also check if extracted data covers other steps
    for sid in _STEP_REQUIRED_FIELDS:
        if sid in completed_step_ids:
            continue
        if _step_is_satisfied(sid, extracted):
            completed_step_ids.append(sid)

    # Track all completed steps persistently
    previously_completed: list[str] = context.get("completed_steps", [])
    if not isinstance(previously_completed, list):
        previously_completed = []
    all_completed = list(set(previously_completed + completed_step_ids))
    await memory.save_context(session_id, "completed_steps", all_completed)

    # --- Find the next unanswered step ---
    next_step_index = _find_next_step(step_index, all_completed)

    if next_step_index is None:
        # All steps done
        await memory.save_context(session_id, "intake_step", len(INTAKE_STEPS))
        await memory.save_context(session_id, "intake_complete", True)

        # Update profile embedding
        try:
            from scholarpath.llm.embeddings import get_embedding_service

            emb = get_embedding_service()
            profile_data = {
                "intended_majors": student.intended_majors,
                "gpa": student.gpa,
                "gpa_scale": student.gpa_scale,
                "sat_total": student.sat_total,
                "extracurriculars": student.extracurriculars,
                "awards": student.awards,
                "preferences": student.preferences,
                "budget_usd": student.budget_usd,
            }
            student.profile_embedding = await emb.embed_student_profile(profile_data)
            await session.flush()
        except Exception:
            logger.warning("Failed to embed profile on intake completion", exc_info=True)

        return "[INTAKE_COMPLETE]" + select_localized_text(
            "太好了，信息已经收集完毕！让我为你生成个性化的学校推荐...",
            "Great, I have all the information I need! Let me generate your personalized school recommendations now...",
            response_lang,
            mixed=(
                "太好了，信息已经收集完毕！让我为你生成个性化的学校推荐...\n"
                "Great, I have all the information I need. Let me generate your personalized school recommendations now..."
            ),
        )

    # Advance to next step
    await memory.save_context(session_id, "intake_step", next_step_index)
    next_step = INTAKE_STEPS[next_step_index]

    # Build a friendly response acknowledging what was captured
    ack = _build_acknowledgment(extracted, current_step, response_lang)

    # Ask the next question in the user's language
    response_text = f"{ack}\n{_localize_step_question(next_step, response_lang)}"

    # Attach structured options if the next step has them
    step_opts = STEP_OPTIONS.get(next_step["id"])
    if step_opts is not None:
        localized_questions = _localize_step_options(step_opts, response_lang)
        options_json = json.dumps(localized_questions, ensure_ascii=False)
        response_text += f"\n[GUIDED_OPTIONS]{options_json}"

    return response_text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _has_value(value: Any) -> bool:
    """Return whether a value should count as provided in intake."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return len(value) > 0
    return True


def _as_string_list(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else None
    if isinstance(value, list):
        items = [str(v).strip() for v in value if str(v).strip()]
        return items or None
    return None


def _extract_preference_value(extracted: dict[str, Any], key: str) -> Any:
    if key in extracted:
        return extracted.get(key)

    alias_map = {
        "location": "location_preference",
        "size": "school_size_preference",
        "culture": "campus_culture",
    }
    alias = alias_map.get(key)
    if alias:
        return extracted.get(alias)
    return None


def _step_is_satisfied(step_id: str, extracted: dict[str, Any]) -> bool:
    """Check if extracted values satisfy a given intake step."""
    if step_id == "academics":
        has_gpa = _has_value(extracted.get("gpa"))
        has_test = _has_value(extracted.get("sat_total")) or _has_value(
            extracted.get("act_composite")
        )
        return has_gpa and has_test

    required_fields = _STEP_REQUIRED_FIELDS.get(step_id, [])
    if not required_fields:
        return False
    return any(_has_value(extracted.get(rf)) for rf in required_fields)


def _normalize_financial_aid(
    raw_need_financial_aid: Any,
    raw_financial_aid_type: Any,
) -> tuple[bool | None, str | None]:
    """Normalize financial-aid signals into bool + canonical type."""
    canonical_type: str | None = None

    if isinstance(raw_financial_aid_type, str):
        normalized = raw_financial_aid_type.strip().lower()
        if normalized in {"need_based", "need-based", "need"}:
            canonical_type = "need_based"
        elif normalized in {"merit", "scholarship"}:
            canonical_type = "merit"
        elif normalized in {"both", "all"}:
            canonical_type = "both"
        elif normalized in {"no", "none", "not_needed", "self_fund"}:
            canonical_type = "no"

    if canonical_type is None and isinstance(raw_need_financial_aid, bool):
        canonical_type = "need_based" if raw_need_financial_aid else "no"

    need_financial_aid: bool | None = None
    if canonical_type in {"need_based", "merit", "both"}:
        need_financial_aid = True
    elif canonical_type == "no":
        need_financial_aid = False
    elif isinstance(raw_need_financial_aid, bool):
        need_financial_aid = raw_need_financial_aid

    return need_financial_aid, canonical_type


def _find_next_step(
    current_index: int,
    completed_ids: list[str],
) -> int | None:
    """Find the next uncompleted step index, or None if all done."""
    if INTAKE_STEPS[current_index]["id"] not in completed_ids:
        return current_index

    # First, look forward from current position
    for i in range(current_index + 1, len(INTAKE_STEPS)):
        if INTAKE_STEPS[i]["id"] not in completed_ids:
            return i
    # Then check if any earlier steps were skipped
    for i in range(0, current_index + 1):
        if INTAKE_STEPS[i]["id"] not in completed_ids:
            return i
    return None


def _build_acknowledgment(
    extracted: dict[str, Any],
    step: dict[str, Any],
    lang: ResponseLanguage = "en",
) -> str:
    """Build a short acknowledgment of what was captured."""
    parts: list[str] = []

    if extracted.get("gpa") is not None:
        parts.append(f"GPA: {extracted['gpa']}")
    if extracted.get("sat_total") is not None:
        parts.append(f"SAT: {extracted['sat_total']}")
    if extracted.get("act_composite") is not None:
        parts.append(f"ACT: {extracted['act_composite']}")
    if extracted.get("toefl_total") is not None:
        parts.append(f"TOEFL: {extracted['toefl_total']}")
    if extracted.get("intended_majors"):
        majors = extracted["intended_majors"]
        if isinstance(majors, list):
            label = select_localized_text("专业", "Majors", lang, mixed="专业 / Majors")
            parts.append(f"{label}: {', '.join(str(m) for m in majors)}")
    if extracted.get("budget_usd") is not None:
        budget = extracted["budget_usd"]
        label = select_localized_text("预算", "Budget", lang, mixed="预算 / Budget")
        parts.append(f"{label}: ${budget:,}/yr")
    if extracted.get("career_goal"):
        label = select_localized_text("职业目标", "Career goal", lang, mixed="职业目标 / Career goal")
        parts.append(f"{label}: {extracted['career_goal']}")

    if parts:
        prefix = select_localized_text(
            "收到！已记录: ",
            "Got it! I've noted: ",
            lang,
            mixed="收到！已记录 / Got it: ",
        )
        return prefix + "; ".join(parts) + "."
    return select_localized_text(
        "谢谢分享！",
        "Thanks for sharing!",
        lang,
        mixed="谢谢分享！ / Thanks for sharing!",
    )


def _localize_step_question(step: dict[str, Any], lang: ResponseLanguage) -> str:
    return select_localized_text(
        step["question_zh"],
        step["question_en"],
        lang,
        mixed=f"{step['question_zh']}\n{step['question_en']}",
    )


def _localize_step_options(step_opts: dict[str, Any], lang: ResponseLanguage) -> dict[str, Any]:
    """Localize step option titles and placeholders for the user's language."""
    result_questions: list[dict[str, Any]] = []
    for q in step_opts["questions"]:
        localized_q: dict[str, Any] = {
            "id": q["id"],
            "title": select_localized_text(
                q.get("title_zh", q.get("title_en", "")),
                q.get("title_en", ""),
                lang,
                mixed=f"{q.get('title_zh', q.get('title_en', ''))} / {q.get('title_en', '')}",
            ),
            "options": q.get("options", []),
            "allow_custom": q.get("allow_custom", True),
            "custom_placeholder": select_localized_text(
                q.get("custom_placeholder_zh", q.get("custom_placeholder_en", "")),
                q.get("custom_placeholder_en", ""),
                lang,
                mixed=(
                    f"{q.get('custom_placeholder_zh', q.get('custom_placeholder_en', ''))} / "
                    f"{q.get('custom_placeholder_en', '')}"
                ),
            ),
            "multi_select": q.get("multi_select", False),
        }
        result_questions.append(localized_q)
    return {"questions": result_questions}


def _parse_budget(raw: str) -> int | None:
    """Best-effort parse of a budget string to annual USD integer."""
    import re

    raw = raw.replace(",", "").replace("$", "").strip()
    # Handle "60k" or "60K"
    match = re.search(r"(\d+(?:\.\d+)?)\s*[kK]", raw)
    if match:
        return int(float(match.group(1)) * 1000)
    # Handle plain number
    match = re.search(r"(\d+)", raw)
    if match:
        value = int(match.group(1))
        if value < 500:
            return value * 1000
        return value
    return None
