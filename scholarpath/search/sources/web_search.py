"""Generic web search source with LLM extraction."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from scholarpath.llm import get_llm_client
from scholarpath.search.sources.base import BaseSource, SearchResult

logger = logging.getLogger(__name__)

_EXTRACTION_SYSTEM_PROMPT = (
    "You are a data extraction assistant. Given raw web search results about "
    "a university, extract structured data points. Return a JSON object in the "
    "shape {\"data\": [...]} where each element in data has: "
    "{\"variable_name\": str, \"value_text\": str, "
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

        headers: dict[str, str] = {
            # Some public APIs (e.g. Wikipedia) reject default anonymous agents.
            "User-Agent": "ScholarPath/1.0 (research-bot)",
            "Accept": "application/json",
        }
        if self._search_api_key:
            headers["Authorization"] = f"Bearer {self._search_api_key}"

        params: dict[str, Any] = {"q": query}
        if "wikipedia.org" in self._search_api_url:
            params = {
                "action": "query",
                "list": "search",
                "srsearch": query,
                "format": "json",
                "utf8": "1",
                "srlimit": "5",
            }
        elif "duckduckgo.com" in self._search_api_url:
            params.update({
                "format": "json",
                "no_redirect": "1",
                "no_html": "1",
                "skip_disambig": "1",
            })
        else:
            params["num"] = 5

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    self._search_api_url,
                    params=params,
                    headers=headers,
                )
                resp.raise_for_status()
                try:
                    data = resp.json()
                    return _normalise_search_items(data)
                except ValueError:
                    # Some providers return markdown/plaintext payloads.
                    return _normalise_text_items(resp.text)
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
                caller="search.web_extract",
            )
        except Exception:
            logger.exception("LLM extraction failed for web search results")
            return []

        items: list[dict[str, Any]] = []
        if isinstance(extracted, list):
            items = [item for item in extracted if isinstance(item, dict)]
        elif isinstance(extracted, dict):
            for key in ("data", "items", "results", "facts"):
                bucket = extracted.get(key)
                if isinstance(bucket, list):
                    items = [item for item in bucket if isinstance(item, dict)]
                    break
            if (
                not items
                and "variable_name" in extracted
                and "value_text" in extracted
            ):
                items = [extracted]

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
            params: dict[str, Any] = {"q": "test"}
            headers: dict[str, str] = {
                "User-Agent": "ScholarPath/1.0 (research-bot)",
                "Accept": "application/json",
            }
            if "wikipedia.org" in self._search_api_url:
                params = {
                    "action": "query",
                    "list": "search",
                    "srsearch": "test",
                    "format": "json",
                    "utf8": "1",
                    "srlimit": "1",
                }
            elif "duckduckgo.com" in self._search_api_url:
                params["format"] = "json"
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    self._search_api_url,
                    params=params,
                    headers=headers,
                )
                return resp.status_code < 500 and resp.status_code != 301
        except httpx.HTTPError:
            return False


def _normalise_search_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise heterogeneous search payloads to a unified snippet schema."""
    if not isinstance(payload, dict):
        return []

    data_obj = payload.get("data")
    if isinstance(data_obj, dict):
        content = data_obj.get("content")
        if isinstance(content, str) and content.strip():
            snippets = _normalise_text_items(content)
            if snippets:
                return snippets
        title = str(data_obj.get("title") or "").strip()
        description = str(data_obj.get("description") or "").strip()
        url = str(data_obj.get("url") or "").strip()
        if title or description:
            return [
                {
                    "title": title or "Search result",
                    "snippet": description or title,
                    "url": url,
                }
            ]

    if isinstance(payload.get("results"), list):
        return [item for item in payload["results"] if isinstance(item, dict)]
    if isinstance(payload.get("items"), list):
        return [item for item in payload["items"] if isinstance(item, dict)]
    if isinstance(payload.get("query"), dict):
        search_items = payload["query"].get("search")
        if isinstance(search_items, list):
            normalised: list[dict[str, Any]] = []
            for item in search_items:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title") or "").strip()
                snippet = str(item.get("snippet") or "").strip()
                if not title and not snippet:
                    continue
                page_title = title.replace(" ", "_")
                normalised.append({
                    "title": title,
                    "snippet": snippet,
                    "url": f"https://en.wikipedia.org/wiki/{page_title}" if page_title else "",
                })
            return normalised

    snippets: list[dict[str, Any]] = []

    abstract = str(payload.get("AbstractText") or "").strip()
    abstract_url = str(payload.get("AbstractURL") or "").strip()
    if abstract:
        snippets.append({
            "title": "Abstract",
            "snippet": abstract,
            "url": abstract_url,
        })

    def _flatten_related(items: list[Any]) -> None:
        for item in items:
            if not isinstance(item, dict):
                continue
            if isinstance(item.get("Topics"), list):
                _flatten_related(item["Topics"])
                continue
            text = str(item.get("Text") or "").strip()
            if not text:
                continue
            snippets.append({
                "title": text[:80],
                "snippet": text,
                "url": str(item.get("FirstURL") or "").strip(),
            })

    related = payload.get("RelatedTopics")
    if isinstance(related, list):
        _flatten_related(related)

    direct_results = payload.get("Results")
    if isinstance(direct_results, list):
        for item in direct_results:
            if not isinstance(item, dict):
                continue
            text = str(item.get("Text") or "").strip()
            if not text:
                continue
            snippets.append({
                "title": text[:80],
                "snippet": text,
                "url": str(item.get("FirstURL") or "").strip(),
            })

    return snippets[:10]


_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")
_MARKDOWN_RESULT_HEADING_RE = re.compile(r"^## \[(.+)\]\((https?://[^)]+)\)$")


def _normalise_text_items(payload_text: str) -> list[dict[str, Any]]:
    """Extract rough snippets from markdown/plaintext search responses."""
    text = (payload_text or "").strip()
    if not text:
        return []

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    snippets: list[dict[str, Any]] = []

    # Prefer explicit result headings from markdown search pages (e.g. Bing export).
    for idx, line in enumerate(lines):
        heading_match = _MARKDOWN_RESULT_HEADING_RE.match(line)
        if not heading_match:
            continue
        title = heading_match.group(1).strip()
        url = heading_match.group(2).strip()
        if not title or not url:
            continue

        snippet = title
        for next_idx in range(idx + 1, min(len(lines), idx + 5)):
            candidate = lines[next_idx]
            if not candidate:
                continue
            if candidate.startswith("## ["):
                break
            if "](" in candidate and candidate.startswith("["):
                continue
            snippet = candidate
            break

        snippets.append(
            {
                "title": title[:160],
                "snippet": snippet[:500],
                "url": url,
            }
        )
        if len(snippets) >= 10:
            return snippets

    if snippets:
        return snippets

    for idx, line in enumerate(lines):
        for match in _MARKDOWN_LINK_RE.finditer(line):
            title = match.group(1).strip()
            url = match.group(2).strip()
            if not title or not url:
                continue
            if title.lower() in {"all", "images", "videos", "news", "maps"}:
                continue

            snippet = line
            if idx + 1 < len(lines):
                next_line = lines[idx + 1]
                if "http://" not in next_line and "https://" not in next_line:
                    snippet = next_line

            snippets.append(
                {
                    "title": title[:160],
                    "snippet": snippet[:500],
                    "url": url,
                }
            )
            if len(snippets) >= 10:
                return snippets

    if snippets:
        return snippets

    return [
        {
            "title": "Web Search Response",
            "snippet": text[:1200],
            "url": "",
        }
    ]
