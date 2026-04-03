"""Official school admissions profile source (school websites)."""

from __future__ import annotations

import logging

import httpx

from scholarpath.search.sources.base import BaseSource, SearchResult
from scholarpath.search.sources.official_fact_extractor import extract_official_results

logger = logging.getLogger(__name__)


class SchoolOfficialProfileSource(BaseSource):
    """Searches official school pages and extracts structured admissions facts."""

    name = "school_official_profile"
    source_type = "official"

    def __init__(
        self,
        *,
        search_api_url: str = "",
        search_api_key: str = "",
    ) -> None:
        self._search_api_url = (search_api_url or "").strip()
        self._search_api_key = (search_api_key or "").strip()

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        snippets = await self._fetch(school_name, fields)
        if not snippets:
            return []
        try:
            return await extract_official_results(
                source_name=self.name,
                source_type=self.source_type,
                school_name=school_name,
                fields=fields,
                snippets=snippets,
                caller="search.official_profile_extract",
                confidence=0.78,
                raw_data={
                    "school_name": school_name,
                    "fetch_mode": "search_api",
                },
                max_tokens=1024,
            )
        except Exception:
            logger.exception("Official profile extraction failed for %s", school_name)
            return []

    async def _fetch(self, school_name: str, fields: list[str] | None) -> str:
        if not self._search_api_url:
            return ""
        query = f"site:.edu {school_name} undergraduate admissions class profile common data set"
        if fields:
            query += " " + " ".join(fields)
        headers = {"User-Agent": "ScholarPath/1.0", "Accept": "application/json"}
        if self._search_api_key:
            headers["Authorization"] = f"Bearer {self._search_api_key}"
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(self._search_api_url, params={"q": query, "num": 8}, headers=headers)
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            logger.warning("Official profile fetch failed for %s", school_name, exc_info=True)
            return ""
        rows = data.get("results") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            return ""
        chunks: list[str] = []
        for row in rows[:8]:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or "").strip()
            snippet = str(row.get("snippet") or row.get("description") or "").strip()
            url = str(row.get("url") or row.get("link") or "").strip()
            if not snippet:
                continue
            chunks.append(f"Title: {title}\nSnippet: {snippet}\nURL: {url}")
        return "\n\n".join(chunks)
