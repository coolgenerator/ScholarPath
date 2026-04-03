"""Shared official-fact LLM extraction helpers."""

from __future__ import annotations

import logging
from typing import Any

from scholarpath.llm import get_llm_client
from scholarpath.search.sources.base import SearchResult

logger = logging.getLogger(__name__)


async def extract_official_results(
    *,
    source_name: str,
    source_type: str,
    school_name: str,
    fields: list[str] | None,
    snippets: str,
    caller: str,
    confidence: float,
    raw_data: dict[str, Any] | None = None,
    max_tokens: int = 1200,
) -> list[SearchResult]:
    """Convert official-source snippets into structured search results."""
    if not snippets.strip():
        return []

    llm = get_llm_client()
    fields_hint = ", ".join(fields or [])
    messages = [
        {
            "role": "system",
            "content": (
                "Extract admissions statistics from official school content. "
                "Return JSON object: {\"data\":[{\"variable_name\":\"...\","
                "\"value_text\":\"...\",\"value_numeric\":number|null,"
                "\"source_url\":\"...\"}]}. "
                "Only include facts present in snippets."
            ),
        },
        {
            "role": "user",
            "content": (
                f"School: {school_name}\n"
                f"Target fields: {fields_hint or 'admissions and cost stats'}\n\n"
                f"Snippets:\n{snippets}"
            ),
        },
    ]
    try:
        payload = await llm.complete_json(
            messages,
            temperature=0.0,
            max_tokens=max_tokens,
            caller=caller,
        )
    except Exception:
        logger.exception("Official extraction failed for %s (%s)", school_name, source_name)
        return []

    items = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return []

    results: list[SearchResult] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        variable = str(item.get("variable_name") or "").strip()
        value_text = str(item.get("value_text") or "").strip()
        if not variable or not value_text:
            continue
        raw_num = item.get("value_numeric")
        try:
            value_numeric = float(raw_num) if raw_num is not None else None
        except (TypeError, ValueError):
            value_numeric = None
        source_url = str(item.get("source_url") or "").strip()
        result_raw_data = {
            "school_name": school_name,
            "fetch_mode": (raw_data or {}).get("fetch_mode", "search_api"),
        }
        if raw_data:
            result_raw_data.update(raw_data)
        results.append(
            SearchResult(
                source_name=source_name,
                source_type=source_type,
                source_url=source_url,
                variable_name=variable,
                value_text=value_text,
                value_numeric=value_numeric,
                confidence=confidence,
                sample_size=None,
                temporal_range="latest",
                raw_data=result_raw_data,
            )
        )
    return results
