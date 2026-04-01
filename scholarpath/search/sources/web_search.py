"""Generic web search source with LLM extraction."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from scholarpath.llm import get_llm_client
from scholarpath.search.sources.base import BaseSource, SearchResult

logger = logging.getLogger(__name__)

_EXTRACTION_SYSTEM_PROMPT = (
    "You are a data extraction assistant. Given raw web search results about "
    "a university, extract structured data points. Return a JSON array where "
    "each element has: {\"variable_name\": str, \"value_text\": str, "
    "\"value_numeric\": float|null}. Only include facts you can find in the "
    "provided text. Do NOT fabricate data."
)


class WebSearchSource(BaseSource):
    """Proxy source that queries a web search API and uses LLM extraction."""

    name = "web_search"
    source_type = "proxy"

    def __init__(
        self,
        search_api_url: str = "https://api.search.example.com/search",
        search_api_key: str = "",
    ) -> None:
        self._search_api_url = search_api_url
        self._search_api_key = search_api_key

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        raw_snippets = await self._fetch_search_results(school_name, fields)
        if not raw_snippets:
            return []
        return await self._extract_structured(school_name, raw_snippets, fields)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_search_results(
        self,
        school_name: str,
        fields: list[str] | None,
    ) -> list[dict[str, Any]]:
        """Call the configured search API and return raw result snippets."""
        fields_hint = " ".join(fields) if fields else "admissions stats tuition"
        query = f"{school_name} {fields_hint}"

        headers: dict[str, str] = {}
        if self._search_api_key:
            headers["Authorization"] = f"Bearer {self._search_api_key}"

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    self._search_api_url,
                    params={"q": query, "num": 5},
                    headers=headers,
                )
                resp.raise_for_status()
                data = resp.json()
                return data.get("results", data.get("items", []))
        except httpx.HTTPError as exc:
            logger.warning("Web search request failed: %s", exc)
            return []

    async def _extract_structured(
        self,
        school_name: str,
        raw_snippets: list[dict[str, Any]],
        fields: list[str] | None,
    ) -> list[SearchResult]:
        """Use the LLM to extract structured data from raw snippets."""
        snippets_text = "\n\n".join(
            f"Title: {s.get('title', '')}\nSnippet: {s.get('snippet', s.get('description', ''))}\n"
            f"URL: {s.get('url', s.get('link', ''))}"
            for s in raw_snippets
        )

        fields_instruction = ""
        if fields:
            fields_instruction = (
                f"\nFocus on extracting these variables: {', '.join(fields)}"
            )

        llm = get_llm_client()
        messages = [
            {"role": "system", "content": _EXTRACTION_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"School: {school_name}{fields_instruction}\n\n"
                    f"Search results:\n{snippets_text}"
                ),
            },
        ]

        try:
            extracted = await llm.complete_json(
                messages,
                temperature=0.1,
                max_tokens=2048,
            )
        except Exception:
            logger.exception("LLM extraction failed for web search results")
            return []

        items = extracted if isinstance(extracted, list) else extracted.get("data", [])
        results: list[SearchResult] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            results.append(
                SearchResult(
                    source_name=self.name,
                    source_type=self.source_type,
                    source_url=raw_snippets[0].get("url", "") if raw_snippets else "",
                    variable_name=item.get("variable_name", "unknown"),
                    value_text=str(item.get("value_text", "")),
                    value_numeric=item.get("value_numeric"),
                    confidence=0.50,
                    sample_size=None,
                    temporal_range=None,
                    raw_data={"snippets_count": len(raw_snippets)},
                )
            )
        return results

    async def health_check(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(self._search_api_url, params={"q": "test"})
                return resp.status_code < 500
        except httpx.HTTPError:
            return False
