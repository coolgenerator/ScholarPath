"""Common App trend-only ingestion source.

This module is intentionally trend-only:
- It provides planning/reporting signals.
- It does NOT emit canonical facts.
- It does NOT emit outcome labels.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CommonAppTrendSignal:
    source_name: str
    metric: str
    period: str
    segment: str | None
    value_numeric: float | None
    value_text: str | None
    source_url: str | None
    metadata: dict[str, Any]


class CommonAppTrendSource:
    """Loads Common App aggregate trend rows from CSV/JSON."""

    name = "common_app"

    def __init__(
        self,
        *,
        dataset_url: str = "",
        dataset_path: str = "",
        timeout_seconds: float = 30.0,
    ) -> None:
        self._dataset_url = (dataset_url or "").strip()
        self._dataset_path = (dataset_path or "").strip()
        self._timeout_seconds = max(float(timeout_seconds), 5.0)
        self._lock = asyncio.Lock()
        self._cache: list[dict[str, Any]] | None = None

    async def load_signals(self, *, years: int = 5) -> list[CommonAppTrendSignal]:
        rows = await self._load_rows()
        if not rows:
            return []
        years = max(1, int(years))
        max_year = max((_safe_int(_pick(row, "year", "cycle_year", "period_year")) or 0) for row in rows)
        min_year = max_year - years + 1 if max_year > 0 else None

        out: list[CommonAppTrendSignal] = []
        for row in rows:
            period = str(_pick(row, "period", "year", "cycle_year") or "").strip()
            if not period:
                continue
            period_year = _safe_int(_pick(row, "year", "cycle_year", "period_year"))
            if min_year is not None and period_year is not None and period_year < min_year:
                continue

            metric = str(_pick(row, "metric", "measure", "name") or "").strip().lower()
            if not metric:
                continue

            segment = _pick(row, "segment", "group", "cohort")
            segment_text = str(segment).strip() if segment is not None else None
            value_raw = _pick(row, "value", "value_numeric", "metric_value")
            value_numeric = _safe_float(value_raw)
            value_text = str(value_raw).strip() if value_raw is not None else None
            source_url = str(_pick(row, "source_url", "url") or self._dataset_url or "").strip() or None
            out.append(
                CommonAppTrendSignal(
                    source_name=self.name,
                    metric=metric,
                    period=period,
                    segment=segment_text or None,
                    value_numeric=value_numeric,
                    value_text=value_text,
                    source_url=source_url,
                    metadata={"raw_row": row},
                )
            )
        return out

    async def _load_rows(self) -> list[dict[str, Any]]:
        async with self._lock:
            if self._cache is not None:
                return self._cache
            rows = await self._fetch_rows()
            self._cache = rows
            return rows

    async def _fetch_rows(self) -> list[dict[str, Any]]:
        path = Path(self._dataset_path) if self._dataset_path else None
        if path and path.exists():
            try:
                return _parse_blob(path.read_text(encoding="utf-8"), suffix=path.suffix.lower())
            except Exception:
                logger.warning("Failed to parse Common App trend file: %s", path, exc_info=True)
                return []

        if not self._dataset_url:
            return []
        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds) as client:
                response = await client.get(self._dataset_url)
                response.raise_for_status()
                content = response.text
        except Exception:
            logger.warning("Failed to fetch Common App trend dataset: %s", self._dataset_url, exc_info=True)
            return []
        suffix = Path(urlparse(self._dataset_url).path).suffix.lower()
        return _parse_blob(content, suffix=suffix)


def _parse_blob(content: str, *, suffix: str) -> list[dict[str, Any]]:
    text = (content or "").strip()
    if not text:
        return []
    if suffix == ".csv":
        return _parse_csv(text)
    if suffix == ".json":
        return _parse_json(text)
    if text.startswith("{") or text.startswith("["):
        return _parse_json(text)
    return _parse_csv(text)


def _parse_csv(text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader if isinstance(row, dict)]


def _parse_json(text: str) -> list[dict[str, Any]]:
    payload = json.loads(text)
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        rows = payload.get("rows") or payload.get("items") or payload.get("data") or payload.get("results")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _pick(row: dict[str, Any], *keys: str) -> Any:
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for key in keys:
        value = lowered.get(str(key).strip().lower())
        if value not in (None, ""):
            return value
    return None


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text.endswith("%"):
        text = text[:-1]
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _safe_int(value: Any) -> int | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    return int(round(numeric))
