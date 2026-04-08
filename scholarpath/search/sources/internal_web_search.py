"""Internal web fallback source using external search + chat extraction (all-chat mode)."""

from __future__ import annotations

import logging

from scholarpath.config import settings
from scholarpath.search.sources.base import BaseSource, SearchResult
from scholarpath.search.sources.web_search import WebSearchSource

logger = logging.getLogger(__name__)


class InternalWebSearchSource(BaseSource):
    """Fallback web source with deterministic degradation when search API is unavailable."""

    name = "internal_web_search"
    source_type = "proxy"

    def __init__(
        self,
        search_api_url: str = "",
        search_api_key: str = "",
    ) -> None:
        resolved_url = (search_api_url or settings.WEB_SEARCH_API_URL or "").strip()
        resolved_key = (search_api_key or settings.WEB_SEARCH_API_KEY or "").strip()
        self._search_api_url = resolved_url
        self._search_api_key = resolved_key
        self._web_source: WebSearchSource | None = None
        if resolved_url:
            self._web_source = WebSearchSource(
                search_api_url=resolved_url,
                search_api_key=resolved_key,
            )
        self.last_status_code: str | None = None
        self.last_status_detail: str | None = None

    async def search(
        self,
        school_name: str,
        fields: list[str] | None = None,
    ) -> list[SearchResult]:
        self.last_status_code = None
        self.last_status_detail = None

        if self._web_source is None:
            self.last_status_code = "web_search_unavailable"
            self.last_status_detail = "WEB_SEARCH_API_URL is empty"
            return []

        try:
            extracted = await self._web_source.search(school_name, fields=fields)
        except Exception as exc:
            logger.exception("Internal web search failed for %s", school_name)
            self.last_status_code = "web_search_failed"
            self.last_status_detail = str(exc)
            return []

        if not extracted:
            self.last_status_code = "web_search_no_results"
            return []

        results: list[SearchResult] = []
        for item in extracted:
            raw_data = dict(item.raw_data or {})
            raw_data["internal_web_search"] = True
            raw_data["queried_school"] = school_name
            results.append(
                SearchResult(
                    source_name=self.name,
                    source_type=self.source_type,
                    source_url=item.source_url,
                    variable_name=item.variable_name,
                    value_text=item.value_text,
                    value_numeric=item.value_numeric,
                    confidence=max(item.confidence, 0.58),
                    sample_size=item.sample_size,
                    temporal_range=item.temporal_range,
                    raw_data=raw_data,
                ),
            )
        return results
