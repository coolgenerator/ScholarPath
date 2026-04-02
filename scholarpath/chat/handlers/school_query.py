"""Handler for SCHOOL_QUERY intent -- answers questions about specific schools."""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.chat.memory import ChatMemory
from scholarpath.config import settings
from scholarpath.db.models import School, Student
from scholarpath.llm.client import LLMClient
from scholarpath.llm.client import get_llm_client
from scholarpath.search.canonical_merge import (
    PRD_EXPANDED_CRITICAL_FIELDS,
    normalise_variable_name,
)
from scholarpath.search.orchestrator import DeepSearchOrchestrator
from scholarpath.services.school_service import get_school_detail, search_schools

logger = logging.getLogger(__name__)


_SCHOOL_ALIAS_MAP: dict[str, str] = {
    "mit": "Massachusetts Institute of Technology",
    "massachusetts institute of technology": "Massachusetts Institute of Technology",
    "caltech": "California Institute of Technology",
    "stanford": "Stanford University",
    "harvard": "Harvard University",
    "princeton": "Princeton University",
    "yale": "Yale University",
    "columbia": "Columbia University",
    "upenn": "University of Pennsylvania",
    "u penn": "University of Pennsylvania",
    "cornell": "Cornell University",
    "duke": "Duke University",
    "nyu": "New York University",
    "ucla": "University of California, Los Angeles",
    "uc berkeley": "University of California, Berkeley",
    "berkeley": "University of California, Berkeley",
    "cmu": "Carnegie Mellon University",
    "carnegie mellon": "Carnegie Mellon University",
    "usc": "University of Southern California",
    "umich": "University of Michigan",
    "gatech": "Georgia Institute of Technology",
    "georgia tech": "Georgia Institute of Technology",
    "斯坦福": "Stanford University",
    "哈佛": "Harvard University",
    "普林斯顿": "Princeton University",
    "耶鲁": "Yale University",
    "哥大": "Columbia University",
    "哥伦比亚大学": "Columbia University",
    "宾大": "University of Pennsylvania",
    "康奈尔": "Cornell University",
    "杜克": "Duke University",
    "纽大": "New York University",
    "纽约大学": "New York University",
    "麻省理工": "Massachusetts Institute of Technology",
    "加州理工": "California Institute of Technology",
    "伯克利": "University of California, Berkeley",
    "卡梅": "Carnegie Mellon University",
    "卡耐基梅隆": "Carnegie Mellon University",
    "南加大": "University of Southern California",
    "密歇根安娜堡": "University of Michigan",
    "佐治亚理工": "Georgia Institute of Technology",
}


@dataclass(slots=True)
class SchoolQueryResult:
    text: str
    llm_calls: int
    school_name: str | None = None
    extraction_source: str = "none"
    deepsearch_triggered: bool = False
    deepsearch_missing_fields_before: list[str] | None = None


async def handle_school_query(
    llm: LLMClient,
    session: AsyncSession,
    memory: ChatMemory,
    session_id: str,
    student_id: uuid.UUID,
    message: str,
) -> SchoolQueryResult:
    """Identify the school being asked about, fetch data, and generate a response.

    The handler:
    1. Uses the LLM to identify which school the user is asking about.
    2. Searches the database for matching schools.
    3. Fetches detailed data (programs, data points, conflicts).
    4. Generates a Knowledge Card summary via the LLM.

    Returns
    -------
    SchoolQueryResult
        Response text plus extraction/LLM diagnostics.
    """
    llm_calls = 0
    undergrad_ctx = await memory.get_context(session_id, domain="undergrad")
    current_school_name = str(undergrad_ctx.get("current_school_name", "")).strip() or None

    # Step 1: Identify school name (heuristic first, LLM fallback)
    school_name, extraction_source = _extract_school_name_heuristic(
        message,
        current_school_name=current_school_name,
    )
    if not school_name:
        school_name = await _extract_school_name(
            llm,
            message,
            current_school_name=current_school_name,
        )
        llm_calls += 1
        extraction_source = "llm"

    if not school_name:
        return SchoolQueryResult(
            text=(
                "I'm not sure which school you're asking about. "
                "Could you provide the full name of the university?"
            ),
            llm_calls=llm_calls,
            school_name=None,
            extraction_source=extraction_source,
        )

    # Step 2: Search the database
    results = await search_schools(session, {"q": school_name, "limit": 3})
    if not results:
        return SchoolQueryResult(
            text=(
                f"I couldn't find \"{school_name}\" in our database. "
                "Please double-check the name, or I can search with different terms."
            ),
            llm_calls=llm_calls,
            school_name=None,
            extraction_source=extraction_source,
        )

    school = results[0]
    await memory.save_context(
        session_id,
        "current_school_id",
        str(school.id),
        domain="undergrad",
    )
    await memory.save_context(
        session_id,
        "current_school_name",
        school.name,
        domain="undergrad",
    )

    # Step 3: Get detailed data
    detail = await get_school_detail(session, school.id)
    programs = detail["programs"]
    data_points = detail["data_points"]
    conflicts = detail["conflicts"]
    if isinstance(session, AsyncSession):
        deepsearch_meta = await _maybe_refresh_with_internal_deepsearch(
            session=session,
            student_id=student_id,
            school=school,
            data_points=data_points,
        )
    else:
        deepsearch_meta = {
            "triggered": False,
            "missing_fields_before": _missing_critical_fields(
                data_points,
                freshness_days=max(0, int(settings.ADVISOR_INTERNAL_DEEPSEARCH_FRESHNESS_DAYS)),
            ),
        }
    if deepsearch_meta.get("triggered") and not deepsearch_meta.get("errors"):
        detail = await get_school_detail(session, school.id)
        programs = detail["programs"]
        data_points = detail["data_points"]
        conflicts = detail["conflicts"]

    # Step 4: Build Knowledge Card and generate response
    knowledge_card = _build_knowledge_card(school, programs, data_points, conflicts)
    if deepsearch_meta.get("triggered"):
        knowledge_card["internal_deepsearch"] = {
            "triggered": True,
            "missing_fields_before": deepsearch_meta.get("missing_fields_before", []),
            "errors": deepsearch_meta.get("errors", []),
            "eval_run_id": deepsearch_meta.get("eval_run_id"),
        }

    messages = [
        {
            "role": "system",
            "content": (
                "You are a college admissions advisor. Answer the student's "
                "question about a school using the provided Knowledge Card. "
                "Be informative, accurate, and conversational. If data has "
                "conflicts, mention the uncertainty. The student may write "
                "in Chinese or English -- respond in the same language."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Student question: {message}\n\n"
                f"Knowledge Card:\n{json.dumps(knowledge_card, ensure_ascii=False, indent=2)}"
            ),
        },
    ]
    response = await llm.complete(messages, temperature=0.6, max_tokens=1024, caller="chat.school_query")
    llm_calls += 1
    return SchoolQueryResult(
        text=response,
        llm_calls=llm_calls,
        school_name=school.name,
        extraction_source=extraction_source,
        deepsearch_triggered=bool(deepsearch_meta.get("triggered", False)),
        deepsearch_missing_fields_before=list(
            deepsearch_meta.get("missing_fields_before", []),
        ),
    )


async def _maybe_refresh_with_internal_deepsearch(
    *,
    session: AsyncSession,
    student_id: uuid.UUID,
    school: School,
    data_points: list[Any],
) -> dict[str, Any]:
    missing_fields = _missing_critical_fields(
        data_points,
        freshness_days=max(0, int(settings.ADVISOR_INTERNAL_DEEPSEARCH_FRESHNESS_DAYS)),
    )
    if not settings.ADVISOR_INTERNAL_DEEPSEARCH_ENABLED:
        return {"triggered": False, "missing_fields_before": missing_fields}
    if not missing_fields:
        return {"triggered": False, "missing_fields_before": missing_fields}

    eval_run_id = f"advisor-school-query-{uuid.uuid4().hex[:12]}"
    try:
        result = await _run_internal_deepsearch(
            session=session,
            student_id=student_id,
            school_name=school.name,
            required_fields=missing_fields,
            eval_run_id=eval_run_id,
        )
        errors = result.get("errors", []) if isinstance(result, dict) else []
        return {
            "triggered": True,
            "missing_fields_before": missing_fields,
            "eval_run_id": eval_run_id,
            "errors": errors,
            "result": result,
        }
    except Exception as exc:
        logger.warning(
            "Internal DeepSearch refresh failed for school query '%s': %s",
            school.name,
            exc,
        )
        return {
            "triggered": True,
            "missing_fields_before": missing_fields,
            "eval_run_id": eval_run_id,
            "errors": [str(exc)],
        }


async def _run_internal_deepsearch(
    *,
    session: AsyncSession,
    student_id: uuid.UUID,
    school_name: str,
    required_fields: list[str],
    eval_run_id: str,
) -> dict[str, Any]:
    student = await session.get(Student, student_id)
    if student is None:
        raise ValueError(f"Student {student_id} not found for internal DeepSearch refresh")

    scorecard_api_key = (settings.SCORECARD_API_KEY or "").strip()
    if not scorecard_api_key:
        raise ValueError(
            "SCORECARD_API_KEY is required for internal DeepSearch refresh",
        )

    student_profile = {
        "gpa": student.gpa,
        "sat_total": student.sat_total,
        "intended_major": (student.intended_majors or [None])[0],
        "budget_usd": student.budget_usd,
        "preferences": student.preferences,
    }

    llm = get_llm_client()
    orchestrator = DeepSearchOrchestrator(
        llm=llm,
        scorecard_api_key=scorecard_api_key,
        search_api_url=settings.WEB_SEARCH_API_URL,
        search_api_key=settings.WEB_SEARCH_API_KEY,
        school_concurrency=settings.DEEPSEARCH_SCHOOL_CONCURRENCY,
        source_http_concurrency=settings.DEEPSEARCH_SOURCE_HTTP_CONCURRENCY,
        self_extract_concurrency=settings.DEEPSEARCH_SELF_EXTRACT_CONCURRENCY,
        internal_websearch_concurrency=settings.DEEPSEARCH_INTERNAL_WEBSEARCH_CONCURRENCY,
    )

    suffix_token = llm.set_caller_suffix(eval_run_id)
    try:
        result = await orchestrator.search(
            student_profile=student_profile,
            target_schools=[school_name],
            required_fields=required_fields,
            freshness_days=max(0, int(settings.ADVISOR_INTERNAL_DEEPSEARCH_FRESHNESS_DAYS)),
            max_internal_websearch_calls_per_school=max(
                0,
                int(settings.ADVISOR_INTERNAL_DEEPSEARCH_MAX_INTERNAL_WEBSEARCH_PER_SCHOOL),
            ),
            budget_mode=str(settings.ADVISOR_INTERNAL_DEEPSEARCH_BUDGET_MODE or "balanced"),
            eval_run_id=eval_run_id,
        )
    finally:
        llm.reset_caller_suffix(suffix_token)

    return {
        "student_id": str(student_id),
        "schools_searched": [school_name],
        "schools_returned": len(result.schools),
        "conflicts_found": len(result.conflicts),
        "coverage_score": result.coverage_score,
        "schools": result.schools,
        "search_metadata": result.search_metadata,
        "errors": [],
    }


async def _extract_school_name(
    llm: LLMClient,
    message: str,
    *,
    current_school_name: str | None = None,
) -> str | None:
    """Use the LLM to extract a school name from a user message."""
    context_hint = f"Current school in session: {current_school_name}\n" if current_school_name else ""
    messages = [
        {
            "role": "system",
            "content": (
                "Extract the school/university name from the user message. "
                "Return ONLY a JSON object: {\"school_name\": \"...\"} or "
                "{\"school_name\": null} if no school is mentioned. "
                "Handle Chinese names (e.g. 斯坦福 -> Stanford). "
                "If user refers to 'this school/这所学校' and context has a current school, "
                "return the current school."
            ),
        },
        {"role": "user", "content": f"{context_hint}User message: {message}"},
    ]
    try:
        result = await llm.complete_json(messages, temperature=0.1, max_tokens=128, caller="chat.extract_school")
        school_name = result.get("school_name")
        if school_name is None:
            return None
        candidate = str(school_name).strip()
        return candidate or None
    except Exception:
        logger.warning("School name extraction failed", exc_info=True)
        return None


def _extract_school_name_heuristic(
    message: str,
    *,
    current_school_name: str | None,
) -> tuple[str | None, str]:
    text = message.strip()
    if not text:
        return None, "none"
    lowered = text.lower()

    if current_school_name and _contains_contextual_school_reference(text):
        return current_school_name, "context"

    alias_hits: set[str] = set()
    for alias, canonical in _SCHOOL_ALIAS_MAP.items():
        if _alias_match(text, lowered, alias):
            alias_hits.add(canonical)
    if len(alias_hits) == 1:
        return next(iter(alias_hits)), "alias"
    if len(alias_hits) > 1:
        return None, "ambiguous"

    quote_hits = _extract_quoted_candidates(text)
    if len(quote_hits) == 1:
        return quote_hits[0], "quote"
    if len(quote_hits) > 1:
        return None, "ambiguous"

    english_hits = _extract_english_school_candidates(text)
    if len(english_hits) == 1:
        return english_hits[0], "pattern"
    if len(english_hits) > 1:
        return None, "ambiguous"

    if current_school_name and current_school_name.lower() in lowered:
        return current_school_name, "context"

    return None, "none"


def _contains_contextual_school_reference(message: str) -> bool:
    lowered = message.lower()
    zh_refs = ("这所学校", "这个学校", "该校", "这学校", "它")
    en_refs = (
        "this school",
        "that school",
        "this university",
        "that university",
        "this college",
        "that college",
        "it",
    )
    if any(token in message for token in zh_refs):
        return True
    for token in en_refs:
        if token == "it":
            if re.search(r"\bit\b", lowered):
                return True
            continue
        if token in lowered:
            return True
    return False


def _alias_match(raw_text: str, lowered: str, alias: str) -> bool:
    if re.search(r"[\u4e00-\u9fff]", alias):
        return alias in raw_text
    return re.search(rf"\b{re.escape(alias.lower())}\b", lowered) is not None


def _extract_quoted_candidates(message: str) -> list[str]:
    patterns = [
        r"[\"“”'‘’](.{2,80}?)[\"“”'‘’]",
        r"《(.{2,80}?)》",
    ]
    results: list[str] = []
    for pattern in patterns:
        for match in re.findall(pattern, message):
            candidate = str(match).strip()
            if not candidate:
                continue
            if candidate.lower() in {"this school", "that school", "it"}:
                continue
            results.append(candidate)
    return _dedupe_preserve_order(results)


def _extract_english_school_candidates(message: str) -> list[str]:
    pattern = re.compile(
        r"\b([A-Z][A-Za-z&.\-]*(?:\s+[A-Z][A-Za-z&.\-]*){0,5}\s+"
        r"(?:University|College|Institute|School|Tech))\b"
    )
    matches = [m.group(1).strip() for m in pattern.finditer(message)]
    return _dedupe_preserve_order(matches)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _build_knowledge_card(
    school: School,
    programs: list,
    data_points: list,
    conflicts: list,
) -> dict[str, Any]:
    """Build a structured summary for the LLM to consume."""
    card: dict[str, Any] = {
        "name": school.name,
        "name_cn": school.name_cn,
        "location": f"{school.city}, {school.state}",
        "type": school.school_type,
        "us_news_rank": school.us_news_rank,
        "acceptance_rate": school.acceptance_rate,
        "sat_range": f"{school.sat_25}-{school.sat_75}" if school.sat_25 else None,
        "tuition_oos": school.tuition_oos,
        "avg_net_price": school.avg_net_price,
        "student_faculty_ratio": school.student_faculty_ratio,
        "graduation_rate_4yr": school.graduation_rate_4yr,
        "intl_student_pct": school.intl_student_pct,
        "campus_setting": school.campus_setting,
        "programs": [
            {
                "name": p.name,
                "department": p.department,
                "rank": p.us_news_rank,
                "has_research": p.has_research_opps,
                "has_coop": p.has_coop,
            }
            for p in programs[:10]
        ],
        "data_conflict_count": len(conflicts),
    }

    # Add a few key data points
    if data_points:
        card["key_data_points"] = [
            {
                "variable": dp.variable_name,
                "value": dp.value_text,
                "source": dp.source_name,
                "confidence": dp.confidence,
            }
            for dp in data_points[:5]
        ]

    if conflicts:
        card["data_conflicts"] = [
            {
                "variable": c.variable_name,
                "value_a": c.value_a,
                "value_b": c.value_b,
                "severity": c.severity,
            }
            for c in conflicts[:3]
        ]

    return card


def _missing_critical_fields(
    data_points: list[Any],
    *,
    freshness_days: int,
) -> list[str]:
    required = {normalise_variable_name(field) for field in PRD_EXPANDED_CRITICAL_FIELDS}
    if not required:
        return []

    cutoff = datetime.now(UTC) - timedelta(days=max(0, freshness_days))
    covered: set[str] = set()
    for point in data_points:
        variable = normalise_variable_name(getattr(point, "variable_name", ""))
        if variable not in required:
            continue
        crawled_at = _coerce_utc_datetime(getattr(point, "crawled_at", None))
        if crawled_at is None or crawled_at < cutoff:
            continue
        covered.add(variable)

    return sorted(required - covered)


def _coerce_utc_datetime(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
