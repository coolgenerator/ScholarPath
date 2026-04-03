"""Bounded direct fetch fallback for official school facts.

This module avoids search-api dependencies by probing a small set of official
pages directly from ``School.website_url`` / ``School.cds_url`` and extracting
snippets that can be fed into the same official-fact LLM extractor.
"""

from __future__ import annotations

import logging
import re
import zlib
from collections import deque
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models import School
from scholarpath.search.sources.base import SearchResult
from scholarpath.search.sources.official_fact_extractor import extract_official_results

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 15.0
_MAX_DOCUMENTS = 8
_COMMON_PROBE_PATHS = (
    "/admissions",
    "/admission",
    "/undergraduate-admissions",
    "/undergrad-admissions",
    "/financial-aid",
    "/financialaid",
    "/apply",
    "/common-data-set",
    "/common-data-set/",
    "/common-data-set.pdf",
    "/cds",
    "/facts",
)
_PROFILE_KEYWORDS = (
    "admission",
    "admissions",
    "undergrad",
    "undergraduate",
    "financial",
    "aid",
    "class profile",
    "profile",
)
_CDS_KEYWORDS = (
    "common data set",
    "common-data-set",
    "cds",
    "admissions",
    "financial aid",
    "pdf",
)
_PDF_STREAM_RE = re.compile(rb"stream\r?\n(.*?)endstream", re.S)
_PDF_LITERAL_RE = re.compile(r"\((?:\\.|[^\\()])*\)")
_PDF_TJ_ARRAY_RE = re.compile(r"\[(.*?)\]\s*TJ", re.S)


@dataclass(slots=True)
class _OfficialDocument:
    url: str
    fetch_mode: str
    text: str


class _HTMLProbeParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._skip_depth = 0
        self._text_parts: list[str] = []
        self._links: list[tuple[str, str]] = []
        self._anchor_href: str | None = None
        self._anchor_parts: list[str] = []

    @property
    def text(self) -> str:
        return _collapse_whitespace(" ".join(self._text_parts))

    @property
    def links(self) -> list[tuple[str, str]]:
        return list(self._links)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if tag in {"p", "div", "section", "article", "header", "footer", "main", "li", "tr", "br", "hr", "h1", "h2", "h3", "h4", "h5", "h6"}:
            self._text_parts.append("\n")
        if tag == "a":
            attrs_map = {key.lower(): (value or "") for key, value in attrs}
            href = attrs_map.get("href", "").strip()
            self._anchor_href = href or None
            self._anchor_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1
            return
        if tag in {"p", "div", "section", "article", "header", "footer", "main", "li", "tr", "br", "hr", "h1", "h2", "h3", "h4", "h5", "h6"}:
            self._text_parts.append("\n")
        if tag == "a" and self._anchor_href:
            anchor_text = _collapse_whitespace(" ".join(self._anchor_parts))
            self._links.append((self._anchor_href, anchor_text))
            self._anchor_href = None
            self._anchor_parts = []

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = unescape(data).strip()
        if not text:
            return
        self._text_parts.append(text)
        if self._anchor_href is not None:
            self._anchor_parts.append(text)


async def fetch_school_official_profile_direct(
    session: AsyncSession,
    *,
    school: School,
    fields: list[str] | None,
    run_id: str,
) -> list[SearchResult]:
    """Bounded direct-fetch fallback for official admissions pages."""
    documents = await _collect_profile_documents(session, school=school, fields=fields)
    return await _extract_documents(
        source_name="school_official_profile",
        source_type="official",
        school_name=school.name,
        fields=fields,
        documents=documents,
        run_id=run_id,
        caller="search.official_profile_direct_extract",
        fetch_mode=None,
    )


async def fetch_common_dataset_direct(
    session: AsyncSession,
    *,
    school: School,
    fields: list[str] | None,
    run_id: str,
) -> list[SearchResult]:
    """Bounded direct-fetch fallback for Common Data Set pages / PDFs."""
    documents = await _collect_cds_documents(session, school=school, fields=fields)
    return await _extract_documents(
        source_name="cds_parser",
        source_type="official",
        school_name=school.name,
        fields=fields,
        documents=documents,
        run_id=run_id,
        caller="search.cds_direct_extract",
        fetch_mode=None,
    )


async def _collect_profile_documents(
    session: AsyncSession,
    *,
    school: School,
    fields: list[str] | None,
) -> list[_OfficialDocument]:
    _ = session, fields
    start_url = _normalize_url(school.website_url)
    if not start_url:
        return []
    seed_urls = [start_url]
    seed_urls.extend(urljoin(start_url.rstrip("/") + "/", path.lstrip("/")) for path in _COMMON_PROBE_PATHS)
    return await _collect_documents(
        seed_urls=seed_urls,
        root_url=start_url,
        keywords=_PROFILE_KEYWORDS,
        allow_pdf=True,
    )


async def _collect_cds_documents(
    session: AsyncSession,
    *,
    school: School,
    fields: list[str] | None,
) -> list[_OfficialDocument]:
    _ = session, fields
    seed_urls: list[str] = []
    if school.cds_url:
        seed_urls.append(_normalize_url(school.cds_url))
    start_url = _normalize_url(school.website_url)
    if start_url:
        seed_urls.append(start_url)
        seed_urls.extend(urljoin(start_url.rstrip("/") + "/", path.lstrip("/")) for path in _COMMON_PROBE_PATHS)
    return await _collect_documents(
        seed_urls=seed_urls,
        root_url=start_url or (seed_urls[0] if seed_urls else ""),
        keywords=_CDS_KEYWORDS,
        allow_pdf=True,
    )


async def _collect_documents(
    *,
    seed_urls: list[str],
    root_url: str,
    keywords: tuple[str, ...],
    allow_pdf: bool,
) -> list[_OfficialDocument]:
    if not seed_urls:
        return []
    root_host = urlparse(root_url).netloc.lower().removeprefix("www.")
    queue = deque([url for url in seed_urls if url])
    seen: set[str] = set()
    docs: list[_OfficialDocument] = []

    while queue and len(docs) < _MAX_DOCUMENTS:
        url = _normalize_url(queue.popleft())
        if not url or url in seen:
            continue
        seen.add(url)
        try:
            document, links = await _fetch_document(url)
        except Exception:
            logger.debug("Direct official fetch failed for %s", url, exc_info=True)
            continue
        if document is None:
            continue
        docs.append(document)
        if document.fetch_mode != "direct_html":
            continue
        for href, anchor_text in links:
            if len(queue) + len(seen) >= _MAX_DOCUMENTS * 4:
                break
            candidate = _normalize_url(urljoin(url, href))
            if not candidate or candidate in seen:
                continue
            if not _is_same_domain(candidate, root_host):
                continue
            if not allow_pdf and candidate.lower().endswith(".pdf"):
                continue
            if not _is_relevant_candidate(candidate, anchor_text, keywords):
                continue
            queue.append(candidate)

    return docs


async def _fetch_document(url: str) -> tuple[_OfficialDocument | None, list[tuple[str, str]]]:
    content, content_type = await _fetch_url(url)
    if content is None:
        return None, []
    is_pdf = url.lower().endswith(".pdf") or "pdf" in (content_type or "").lower()
    if is_pdf:
        text = _extract_pdf_text(content)
        if not text:
            return None, []
        return _OfficialDocument(url=url, fetch_mode="direct_pdf", text=text), []
    text, links = _extract_html_and_links_from_bytes(content)
    if not text:
        return None, []
    return _OfficialDocument(url=url, fetch_mode="direct_html", text=text), links


async def _fetch_url(url: str) -> tuple[bytes | None, str | None]:
    headers = {"User-Agent": "ScholarPath/1.0", "Accept": "text/html,application/pdf;q=0.9,*/*;q=0.8"}
    async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, follow_redirects=True) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        return response.content, response.headers.get("content-type")


def _extract_html_and_links_from_bytes(content: bytes) -> tuple[str, list[tuple[str, str]]]:
    text = content.decode("utf-8", errors="ignore")
    parser = _HTMLProbeParser()
    parser.feed(text)
    parser.close()
    return parser.text, parser.links


def _extract_pdf_text(data: bytes) -> str:
    chunks: list[str] = []
    for match in _PDF_STREAM_RE.finditer(data):
        stream = match.group(1).strip(b"\r\n")
        payloads = [stream]
        for candidate in (stream,):
            try:
                payloads.insert(0, zlib.decompress(candidate))
            except Exception:
                continue
        for payload in payloads:
            text = payload.decode("latin-1", errors="ignore")
            chunks.extend(_extract_pdf_strings(text))
    if not chunks:
        fallback_text = data.decode("latin-1", errors="ignore")
        chunks.extend(_extract_pdf_strings(fallback_text))
    return _collapse_whitespace("\n".join(chunks))


def _extract_pdf_strings(text: str) -> list[str]:
    extracted: list[str] = []
    for literal in _PDF_LITERAL_RE.findall(text):
        decoded = _unescape_pdf_literal(literal[1:-1])
        if decoded.strip():
            extracted.append(decoded)
    for array in _PDF_TJ_ARRAY_RE.findall(text):
        for literal in _PDF_LITERAL_RE.findall(array):
            decoded = _unescape_pdf_literal(literal[1:-1])
            if decoded.strip():
                extracted.append(decoded)
    return extracted


def _unescape_pdf_literal(value: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(value):
        char = value[i]
        if char != "\\":
            out.append(char)
            i += 1
            continue
        i += 1
        if i >= len(value):
            break
        escape = value[i]
        if escape in "\\()":
            out.append(escape)
            i += 1
            continue
        if escape == "n":
            out.append("\n")
            i += 1
            continue
        if escape == "r":
            out.append("\r")
            i += 1
            continue
        if escape == "t":
            out.append("\t")
            i += 1
            continue
        if escape == "b":
            out.append("\b")
            i += 1
            continue
        if escape == "f":
            out.append("\f")
            i += 1
            continue
        if escape in "01234567":
            oct_digits = [escape]
            i += 1
            while i < len(value) and len(oct_digits) < 3 and value[i] in "01234567":
                oct_digits.append(value[i])
                i += 1
            try:
                out.append(chr(int("".join(oct_digits), 8)))
            except ValueError:
                pass
            continue
        out.append(escape)
        i += 1
    return "".join(out)


def _collapse_whitespace(text: str) -> str:
    return " ".join(text.split())


def _normalize_url(url: str | None) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw):
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    if not parsed.netloc:
        return ""
    return urlunparse(
        (
            parsed.scheme or "https",
            parsed.netloc,
            parsed.path or "/",
            "",
            parsed.query,
            "",
        )
    )


def _is_same_domain(candidate_url: str, root_host: str) -> bool:
    if not root_host:
        return True
    candidate_host = urlparse(candidate_url).netloc.lower().removeprefix("www.")
    if not candidate_host:
        return False
    return (
        candidate_host == root_host
        or candidate_host.endswith(f".{root_host}")
        or root_host.endswith(f".{candidate_host}")
    )


def _is_relevant_candidate(candidate_url: str, anchor_text: str, keywords: tuple[str, ...]) -> bool:
    haystack = f"{candidate_url} {anchor_text}".lower()
    return any(keyword in haystack for keyword in keywords) or candidate_url.lower().endswith(".pdf")


async def _extract_documents(
    *,
    source_name: str,
    source_type: str,
    school_name: str,
    fields: list[str] | None,
    documents: list[_OfficialDocument],
    run_id: str,
    caller: str,
    fetch_mode: str | None,
) -> list[SearchResult]:
    if not documents:
        return []

    grouped: dict[str, list[_OfficialDocument]] = {}
    for document in documents:
        grouped.setdefault(document.fetch_mode, []).append(document)

    results: list[SearchResult] = []
    for mode, docs in grouped.items():
        snippets = "\n\n".join(
            f"Mode: {doc.fetch_mode}\nURL: {doc.url}\nText:\n{doc.text}" for doc in docs if doc.text.strip()
        )
        if not snippets.strip():
            continue
        extracted = await extract_official_results(
            source_name=source_name,
            source_type=source_type,
            school_name=school_name,
            fields=fields,
            snippets=snippets,
            caller=caller,
            confidence=0.76 if mode == "direct_html" else 0.82,
            raw_data={
                "school_name": school_name,
                "fetch_mode": mode if fetch_mode is None else fetch_mode,
                "source_kind": "official_direct_fetch",
                "run_id": run_id,
                "source_urls": [doc.url for doc in docs],
            },
            max_tokens=1280,
        )
        results.extend(extracted)
    return results
