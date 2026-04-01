"""Internal model web search source using Responses API tools."""

from __future__ import annotations

import logging
from typing import Any

from scholarpath.llm import LLMClient, get_llm_client
from scholarpath.search.sources.base import BaseSource, SearchResult

logger = logging.getLogger(__name__)


class InternalWebSearchSource(BaseSource):
    """Web source powered by the model's built-in web_search tool."""

    name = "internal_web_search"
    source_type = "proxy"

    def __init__(self, llm: LLMClient | None = None) -> None:
        self._llm = llm or get_llm_client()

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        fields_hint = ", ".join(fields) if fields else (
            "acceptance_rate, tuition_out_of_state, avg_net_price, sat_math_mid, "
            "sat_reading_mid, overall_grade, intl_student_pct"
        )

        prompt = (
            "Search the web and return only verified facts.\n"
            f"School: {school_name}\n"
            f"Prioritise fields: {fields_hint}\n\n"
            "Return STRICT JSON object:\n"
            "{\n"
            '  "data": [\n'
            '    {"variable_name": "...", "value_text": "...", "value_numeric": null}\n'
            "  ]\n"
            "}\n"
            "No prose, only JSON."
        )

        try:
            extracted = await self._llm.complete_json_with_web_search(
                prompt=prompt,
                max_output_tokens=512,
                caller="search.internal_web_search",
            )
        except Exception:
            logger.exception("Internal web search extraction failed for %s", school_name)
            return []

        items: list[dict[str, Any]] = []
        if isinstance(extracted, dict):
            bucket = extracted.get("data")
            if isinstance(bucket, list):
                items = [item for item in bucket if isinstance(item, dict)]
        elif isinstance(extracted, list):
            items = [item for item in extracted if isinstance(item, dict)]

        results: list[SearchResult] = []
        for item in items:
            variable = str(item.get("variable_name", "")).strip()
            value_text = str(item.get("value_text", "")).strip()
            if not variable or not value_text:
                continue
            numeric_value = item.get("value_numeric")
            try:
                value_numeric = float(numeric_value) if numeric_value is not None else None
            except (TypeError, ValueError):
                value_numeric = None

            results.append(
                SearchResult(
                    source_name=self.name,
                    source_type=self.source_type,
                    source_url="",
                    variable_name=variable,
                    value_text=value_text,
                    value_numeric=value_numeric,
                    confidence=0.58,
                    sample_size=None,
                    temporal_range=None,
                    raw_data={
                        "internal_web_search": True,
                        "queried_school": school_name,
                    },
                )
            )
        return results
