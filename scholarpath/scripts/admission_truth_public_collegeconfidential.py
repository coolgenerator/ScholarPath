"""Ingest public admission truth events from College Confidential (Discourse)."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import html
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models import (
    AdmissionEvent,
    CausalFeatureSnapshot,
    CausalOutcomeEvent,
    School,
    Student,
)
from scholarpath.db.session import async_session_factory
from scholarpath.scripts.admission_truth_public_reddit import (
    ParsedDecision,
    ParsedProfile,
    _is_noisy_autocreated_school_name,
    _looks_like_school_name,
    _split_school_candidates,
    parse_decisions,
    parse_profile,
)
from scholarpath.scripts.admission_truth_public_shared import (
    MetricsSchoolContext,
    RankedSchoolAllowlistContext,
    estimate_eligible_snapshots,
    load_ranked_school_allowlist,
    load_metrics_school_context,
    resolve_school_name_for_ingest,
)
from scholarpath.services.causal_data_service import register_admission_event
from scholarpath.services.causal_real_asset_service import backfill_real_admission_assets


@dataclass(slots=True)
class CCTopic:
    topic_id: int
    slug: str
    title: str
    created_at: datetime


@dataclass(slots=True)
class CCPost:
    post_id: str
    topic_id: int
    title: str
    body: str
    permalink: str
    created_at: datetime


@dataclass(slots=True)
class SchoolLookupResult:
    school: School | None
    created: bool
    skip_reason: str | None = None


_FALLBACK_STAGE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "admit",
        re.compile(
            r"\b(?:accepted|admitted|got\s+accepted|got\s+admitted|got\s+in|acceptance)\b"
            r"(?:[^\n]{0,40}?)\b(?:to|at|into)\s+(?P<school>[A-Z][^.,;:!?\n]{1,120})",
            re.I,
        ),
    ),
    (
        "reject",
        re.compile(
            r"\b(?:rejected|denied|got\s+rejected|got\s+denied)\b"
            r"(?:[^\n]{0,40}?)\b(?:from|by|at)\s+(?P<school>[A-Z][^.,;:!?\n]{1,120})",
            re.I,
        ),
    ),
    (
        "waitlist",
        re.compile(
            r"\b(?:waitlisted|waitlist)\b"
            r"(?:[^\n]{0,30}?)\b(?:at|for|by)\s+(?P<school>[A-Z][^.,;:!?\n]{1,120})",
            re.I,
        ),
    ),
    (
        "deferred",
        re.compile(
            r"\b(?:deferred|deferral)\b"
            r"(?:[^\n]{0,30}?)\b(?:from|at|by)\s+(?P<school>[A-Z][^.,;:!?\n]{1,120})",
            re.I,
        ),
    ),
    (
        "commit",
        re.compile(
            r"\b(?:committed|commit\s+to|enrolling\s+at|enroll\s+at)\b"
            r"(?:[^\n]{0,20}?)\b(?:to|at)?\s*(?P<school>[A-Z][^.,;:!?\n]{1,120})",
            re.I,
        ),
    ),
)


_STAGE_ONLY_KEYWORDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("admit", ("accepted", "admitted", "got accepted", "got admitted", "got in")),
    ("reject", ("rejected", "denied", "got rejected", "got denied")),
    ("waitlist", ("waitlisted", "waitlist")),
    ("deferred", ("deferred", "deferral")),
    ("commit", ("committed", "commit to", "enrolling at", "enroll at")),
)

_FIRST_PERSON_HINTS = (
    " i ",
    " my ",
    " son ",
    " daughter ",
    " kid ",
    " d25",
    " d26",
    " d27",
    " s25",
    " s26",
    " s27",
)

_HTTP_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_HTTP_RETRY_MAX_ATTEMPTS = 6
_HTTP_RETRY_BASE_SLEEP_SECONDS = 2.0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest public admission truth events from College Confidential.",
    )
    parser.add_argument(
        "--base-url",
        default="https://talk.collegeconfidential.com",
        help="Discourse base URL (default: College Confidential).",
    )
    parser.add_argument(
        "--listing-mode",
        default="latest",
        choices=["latest", "top"],
        help="Topic listing mode (default: latest).",
    )
    parser.add_argument(
        "--top-period",
        default="yearly",
        choices=["all", "yearly", "quarterly", "monthly", "weekly", "daily"],
        help="Top period when listing-mode=top (default: yearly).",
    )
    parser.add_argument(
        "--max-topics",
        type=int,
        default=1200,
        help="Maximum topics to fetch (default: 1200).",
    )
    parser.add_argument(
        "--topics-page-size",
        type=int,
        default=50,
        help="Topics page size hint (default: 50).",
    )
    parser.add_argument(
        "--max-posts-per-topic",
        type=int,
        default=80,
        help="Maximum posts fetched per topic (default: 80).",
    )
    parser.add_argument(
        "--target-events",
        type=int,
        default=2000,
        help="Stop once imported admission events reach this target (default: 2000).",
    )
    parser.add_argument(
        "--cycle-year",
        type=int,
        default=0,
        help="Force cycle year; 0 means infer from post timestamp.",
    )
    parser.add_argument(
        "--create-missing-schools",
        action="store_true",
        default=True,
        help="Create placeholder schools when parsed school is missing (default: true).",
    )
    parser.add_argument(
        "--no-create-missing-schools",
        dest="create_missing_schools",
        action="store_false",
        help="Skip events for schools not found in DB.",
    )
    parser.add_argument(
        "--only-metrics-schools",
        dest="only_metrics_schools",
        action="store_true",
        default=True,
        help="Only ingest decisions whose school has official school-year metric coverage (default: true).",
    )
    parser.add_argument(
        "--allow-missing-metrics-schools",
        dest="only_metrics_schools",
        action="store_false",
        help="Allow ingesting schools without school-year metric coverage.",
    )
    parser.add_argument(
        "--use-ranked-allowlist",
        dest="use_ranked_allowlist",
        action="store_true",
        default=True,
        help="Restrict ingest scope to versioned Top100 U + Top50 LAC allowlist (default: true).",
    )
    parser.add_argument(
        "--disable-ranked-allowlist",
        dest="use_ranked_allowlist",
        action="store_false",
        help="Disable ranked allowlist filter.",
    )
    parser.add_argument(
        "--ranked-allowlist-version",
        default="",
        help="Version for ranked allowlist (default: built-in pinned version).",
    )
    parser.add_argument(
        "--include-backfill",
        action="store_true",
        default=True,
        help="Run real-only backfill after importing events (default: true).",
    )
    parser.add_argument(
        "--no-include-backfill",
        dest="include_backfill",
        action="store_false",
        help="Skip post-import snapshot/outcome backfill.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=5400,
        help="Backfill lookback window days (default: 5400).",
    )
    parser.add_argument(
        "--min-true-per-outcome",
        type=int,
        default=1,
        help="Minimum true labels for dataset build (default: 1).",
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=0.35,
        help="Throttle interval between network requests (default: 0.35s).",
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="Optional run id; autogenerated when empty.",
    )
    parser.add_argument(
        "--output-dir",
        default=".benchmarks/admission_truth_public",
        help="Directory for report artifacts.",
    )
    parser.add_argument(
        "--cleanup-noisy-existing",
        action="store_true",
        default=False,
        help="Cleanup noisy placeholder schools/events from prior CC imports before ingest.",
    )
    parser.add_argument(
        "--cleanup-only",
        action="store_true",
        default=False,
        help="Run noisy cleanup only and skip new CC fetching/ingest.",
    )
    return parser


def _normalise_school_key(name: str) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(name or "").lower())
    return re.sub(r"\s+", " ", text).strip()


def _clean_cc_html(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|h[1-6]|blockquote|li|ul|ol|pre|table|tr)>", "\n", text)
    text = re.sub(r"(?i)<li[^>]*>", "- ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def _extract_school_from_topic_title(title: str) -> str | None:
    text = re.sub(r"[^a-zA-Z0-9&'.,/()\\-\\s]+", " ", str(title or ""))
    text = re.sub(r"\s+", " ", text).strip(" -")
    if not text:
        return None
    low = text.lower()
    boundaries = [
        " class of ",
        " official thread",
        " early action",
        " early decision",
        " regular decision",
        " waitlist thread",
        " transfer thread",
        " admissions thread",
    ]
    cut = len(text)
    for marker in boundaries:
        idx = low.find(marker)
        if idx > 0:
            cut = min(cut, idx)
    base = text[:cut].strip(" -")
    if not base or not _looks_like_school_name(base):
        return None
    return base[:300]


def _parse_decisions_cc(text: str, *, topic_title: str = "") -> list[ParsedDecision]:
    """Parse decisions from both sectioned markdown and free-form forum prose."""

    rows = list(parse_decisions(text))
    seen: set[tuple[str, str]] = {(row.stage, row.school_name.lower()) for row in rows}
    merged = str(text or "")
    topic_school = _extract_school_from_topic_title(topic_title) or ""

    for fragment in re.split(r"[\n\.\!\?]+", merged):
        sentence = fragment.strip()
        if len(sentence) < 12:
            continue
        sentence_low = f" {sentence.lower()} "
        matched_stage = False
        for stage, pattern in _FALLBACK_STAGE_PATTERNS:
            for match in pattern.finditer(sentence):
                payload = str(match.group("school") or "").strip()
                if not payload:
                    continue
                for school_name in _split_school_candidates(payload):
                    if not _looks_like_school_name(school_name):
                        continue
                    key = (stage, school_name.lower())
                    if key in seen:
                        continue
                    rows.append(ParsedDecision(stage=stage, school_name=school_name))
                    seen.add(key)
                    matched_stage = True

        # Many CC replies mention status only; infer school from thread title.
        if matched_stage or not topic_school:
            continue
        if "?" in sentence or not any(hint in sentence_low for hint in _FIRST_PERSON_HINTS):
            continue
        for stage, keywords in _STAGE_ONLY_KEYWORDS:
            if not any(token in sentence_low for token in keywords):
                continue
            key = (stage, topic_school.lower())
            if key in seen:
                break
            rows.append(ParsedDecision(stage=stage, school_name=topic_school))
            seen.add(key)
            break

    return rows


def _parse_iso_datetime(value: str | None) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.now(timezone.utc)
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_topic_url(base_url: str, topic_id: int, slug: str) -> str:
    safe_base = base_url.rstrip("/")
    clean_slug = str(slug or "").strip("/")
    if clean_slug:
        return f"{safe_base}/t/{clean_slug}/{topic_id}"
    return f"{safe_base}/t/{topic_id}"


def _origin_base_url(base_url: str) -> str:
    parsed = httpx.URL(str(base_url or "").strip() or "https://talk.collegeconfidential.com")
    scheme = parsed.scheme or "https"
    host = parsed.host or "talk.collegeconfidential.com"
    port = parsed.port
    if port is None:
        return f"{scheme}://{host}"
    return f"{scheme}://{host}:{int(port)}"


def _parse_retry_after_seconds(value: str | None) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


def _request_with_retry(
    client: httpx.Client,
    *,
    url: str,
    params: Any | None = None,
    max_attempts: int = _HTTP_RETRY_MAX_ATTEMPTS,
) -> httpx.Response | None:
    attempts = max(1, int(max_attempts))
    last_response: httpx.Response | None = None
    for attempt in range(attempts):
        try:
            response = client.get(url, params=params)
        except httpx.HTTPError:
            if attempt + 1 >= attempts:
                return None
            sleep_seconds = min(30.0, _HTTP_RETRY_BASE_SLEEP_SECONDS * (2**attempt))
            time.sleep(sleep_seconds)
            continue

        if response.status_code == 200:
            return response

        last_response = response
        if response.status_code not in _HTTP_RETRYABLE_STATUS:
            return response
        if attempt + 1 >= attempts:
            return response

        retry_after = _parse_retry_after_seconds(response.headers.get("Retry-After"))
        sleep_seconds = retry_after or min(30.0, _HTTP_RETRY_BASE_SLEEP_SECONDS * (2**attempt))
        time.sleep(max(0.0, sleep_seconds))

    return last_response


def _fetch_cc_topics(
    *,
    base_url: str,
    listing_mode: str,
    top_period: str,
    max_topics: int,
    topics_page_size: int,
    request_interval_seconds: float,
) -> list[CCTopic]:
    safe_base = base_url.rstrip("/")
    safe_listing_mode = str(listing_mode or "latest").strip().lower()
    if safe_listing_mode not in {"latest", "top"}:
        safe_listing_mode = "latest"

    client = httpx.Client(
        headers={"User-Agent": "ScholarPathDataBot/1.0 (public research; contact: support@scholarpath.local)"},
        timeout=20.0,
        follow_redirects=True,
    )
    topics: list[CCTopic] = []
    seen_topic_ids: set[int] = set()
    page = 0

    while len(topics) < max(1, int(max_topics)):
        if safe_listing_mode == "top":
            url = f"{safe_base}/top.json"
            params = {
                "period": str(top_period or "yearly").strip().lower() or "yearly",
                "page": page,
                "per_page": max(1, int(topics_page_size)),
            }
        else:
            url = f"{safe_base}/latest.json"
            params = {"page": page}

        resp = _request_with_retry(client, url=url, params=params)
        if resp is None or resp.status_code != 200:
            break

        payload = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        rows = ((payload or {}).get("topic_list") or {}).get("topics") or []
        if not isinstance(rows, list) or not rows:
            break

        appended = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            topic_id = int(row.get("id") or 0)
            if topic_id <= 0 or topic_id in seen_topic_ids:
                continue
            seen_topic_ids.add(topic_id)
            topics.append(
                CCTopic(
                    topic_id=topic_id,
                    slug=str(row.get("slug") or "").strip(),
                    title=str(row.get("title") or "").strip(),
                    created_at=_parse_iso_datetime(row.get("created_at") or row.get("last_posted_at")),
                )
            )
            appended += 1
            if len(topics) >= int(max_topics):
                break

        if appended == 0:
            break
        page += 1
        time.sleep(max(0.0, float(request_interval_seconds)))

    client.close()
    return topics


def _fetch_cc_topic_posts(
    *,
    base_url: str,
    topic: CCTopic,
    max_posts_per_topic: int,
    request_interval_seconds: float,
) -> list[CCPost]:
    api_base = _origin_base_url(base_url).rstrip("/")
    topic_url = _build_topic_url(api_base, topic.topic_id, topic.slug)

    client = httpx.Client(
        headers={"User-Agent": "ScholarPathDataBot/1.0 (public research; contact: support@scholarpath.local)"},
        timeout=20.0,
        follow_redirects=True,
    )

    def _post_from_row(row: dict[str, Any]) -> CCPost | None:
        post_id = int(row.get("id") or 0)
        if post_id <= 0:
            return None
        body = _clean_cc_html(str(row.get("cooked") or ""))
        if len(body.strip()) < 20:
            return None
        return CCPost(
            post_id=f"cc-{topic.topic_id}-{post_id}",
            topic_id=topic.topic_id,
            title=topic.title,
            body=body,
            permalink=f"{topic_url}/{post_id}",
            created_at=_parse_iso_datetime(row.get("created_at")),
        )

    posts: list[CCPost] = []
    seen_post_ids: set[int] = set()

    resp = _request_with_retry(client, url=f"{api_base}/t/{topic.topic_id}.json")
    if resp is None:
        client.close()
        return []
    if resp.status_code != 200:
        client.close()
        return []
    payload = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
    stream = ((payload or {}).get("post_stream") or {}).get("stream") or []
    first_rows = ((payload or {}).get("post_stream") or {}).get("posts") or []

    for row in first_rows:
        if not isinstance(row, dict):
            continue
        post_id = int(row.get("id") or 0)
        if post_id <= 0 or post_id in seen_post_ids:
            continue
        out = _post_from_row(row)
        seen_post_ids.add(post_id)
        if out is None:
            continue
        posts.append(out)
        if len(posts) >= int(max_posts_per_topic):
            client.close()
            return posts

    stream_ids = [int(pid) for pid in stream if isinstance(pid, int)]
    remaining = [pid for pid in stream_ids if pid not in seen_post_ids]
    chunk_size = 20

    for idx in range(0, len(remaining), chunk_size):
        if len(posts) >= int(max_posts_per_topic):
            break
        batch = remaining[idx : idx + chunk_size]
        if not batch:
            break
        params = [("post_ids[]", str(pid)) for pid in batch]
        batch_resp = _request_with_retry(
            client,
            url=f"{api_base}/t/{topic.topic_id}/posts.json",
            params=params,
        )
        if batch_resp is None:
            continue
        if batch_resp.status_code != 200:
            continue
        batch_payload = (
            batch_resp.json() if batch_resp.headers.get("content-type", "").startswith("application/json") else {}
        )
        batch_rows = ((batch_payload or {}).get("post_stream") or {}).get("posts") or []
        for row in batch_rows:
            if not isinstance(row, dict):
                continue
            post_id = int(row.get("id") or 0)
            if post_id <= 0 or post_id in seen_post_ids:
                continue
            out = _post_from_row(row)
            seen_post_ids.add(post_id)
            if out is None:
                continue
            posts.append(out)
            if len(posts) >= int(max_posts_per_topic):
                break
        time.sleep(max(0.0, float(request_interval_seconds)))

    client.close()
    return posts[: int(max_posts_per_topic)]


async def _load_school_index(session: AsyncSession) -> dict[str, School]:
    rows = list((await session.execute(select(School))).scalars().all())
    index: dict[str, School] = {}
    for row in rows:
        key = _normalise_school_key(row.name)
        if key and key not in index:
            index[key] = row
    return index


async def _get_or_create_school(
    session: AsyncSession,
    *,
    school_name: str,
    school_index: dict[str, School],
    create_missing: bool,
) -> SchoolLookupResult:
    key = _normalise_school_key(school_name)
    if not key:
        return SchoolLookupResult(school=None, created=False, skip_reason="empty_school_name")
    existing = school_index.get(key)
    if existing is not None:
        return SchoolLookupResult(school=existing, created=False)
    if not create_missing:
        return SchoolLookupResult(school=None, created=False, skip_reason="school_not_found")
    if _is_noisy_autocreated_school_name(school_name):
        return SchoolLookupResult(school=None, created=False, skip_reason="invalid_school_name")
    row = School(
        name=school_name.strip()[:300],
        city="Unknown",
        state="Unknown",
        school_type="university",
        size_category="unknown",
        metadata_={
            "source": "collegeconfidential_public",
            "note": "autocreated_placeholder_school",
        },
    )
    session.add(row)
    await session.flush()
    school_index[key] = row
    return SchoolLookupResult(school=row, created=True)


async def _get_or_create_student(
    session: AsyncSession,
    *,
    post: CCPost,
    profile: ParsedProfile,
    cycle_year: int,
) -> tuple[Student, bool]:
    email = f"cc_{post.post_id}@public-data.local"
    existing = await session.scalar(select(Student).where(Student.email == email))
    if existing is not None:
        return existing, False
    student = Student(
        name=f"CC Applicant {post.post_id}"[:200],
        email=email,
        gpa=profile.gpa,
        gpa_scale=profile.gpa_scale,
        sat_total=profile.sat_total,
        sat_rw=None,
        sat_math=None,
        act_composite=profile.act_composite,
        toefl_total=None,
        curriculum_type="other",
        ap_courses=None,
        extracurriculars={"source": "collegeconfidential_public"},
        awards=None,
        intended_majors=profile.intended_majors or None,
        budget_usd=max(1, int(profile.budget_usd)),
        need_financial_aid=bool(profile.need_financial_aid),
        preferences={
            "source": "collegeconfidential_public",
            "post_id": post.post_id,
            "post_url": post.permalink,
            "topic_id": post.topic_id,
        },
        ed_preference=None,
        target_year=cycle_year,
        profile_completed=True,
        profile_embedding=None,
    )
    session.add(student)
    await session.flush()
    return student, True


async def _count_core_tables(session: AsyncSession) -> dict[str, int]:
    return {
        "students": int((await session.scalar(select(func.count()).select_from(Student))) or 0),
        "schools": int((await session.scalar(select(func.count()).select_from(School))) or 0),
        "admission_events": int((await session.scalar(select(func.count()).select_from(AdmissionEvent))) or 0),
        "causal_outcome_events": int((await session.scalar(select(func.count()).select_from(CausalOutcomeEvent))) or 0),
        "causal_feature_snapshots": int((await session.scalar(select(func.count()).select_from(CausalFeatureSnapshot))) or 0),
    }


async def _execute_delete_count(session: AsyncSession, statement: Any) -> int:
    result = await session.execute(statement)
    return max(0, int(result.rowcount or 0))


async def _cleanup_noisy_cc_import(session: AsyncSession, *, run_id: str) -> dict[str, Any]:
    school_rows = list((await session.execute(select(School.id, School.name, School.metadata_))).all())
    candidates: list[tuple[Any, str]] = []
    noisy_rows: list[tuple[Any, str]] = []

    for row in school_rows:
        metadata = row.metadata_ if isinstance(row.metadata_, dict) else {}
        if str(metadata.get("source") or "").strip() != "collegeconfidential_public":
            continue
        if str(metadata.get("note") or "").strip() != "autocreated_placeholder_school":
            continue
        school_name = str(row.name or "").strip()
        candidates.append((row.id, school_name))
        if _is_noisy_autocreated_school_name(school_name):
            noisy_rows.append((row.id, school_name))

    deleted_schools = 0
    deleted_events = 0
    deleted_outcomes = 0
    deleted_snapshots = 0
    blocked_schools: list[str] = []

    for school_id, school_name in noisy_rows:
        try:
            async with session.begin_nested():
                deleted_snapshots += await _execute_delete_count(
                    session,
                    delete(CausalFeatureSnapshot).where(CausalFeatureSnapshot.school_id == school_id),
                )
                deleted_outcomes += await _execute_delete_count(
                    session,
                    delete(CausalOutcomeEvent).where(CausalOutcomeEvent.school_id == school_id),
                )
                deleted_events += await _execute_delete_count(
                    session,
                    delete(AdmissionEvent).where(
                        AdmissionEvent.school_id == school_id,
                        AdmissionEvent.source_name == "collegeconfidential_public",
                    ),
                )
                await session.execute(delete(School).where(School.id == school_id))
                school_remaining = int(
                    (await session.scalar(select(func.count()).select_from(School).where(School.id == school_id))) or 0
                )
                if school_remaining > 0:
                    blocked_schools.append(school_name)
                    continue
                deleted_schools += 1
        except IntegrityError:
            blocked_schools.append(school_name)
            continue

    orphan_student_ids = list(
        (
            await session.execute(
                select(Student.id).where(Student.email.like("cc_%@public-data.local")),
            )
        )
        .scalars()
        .all()
    )
    deleted_students = 0
    for student_id in orphan_student_ids:
        events_left = int(
            (
                await session.scalar(
                    select(func.count()).select_from(AdmissionEvent).where(AdmissionEvent.student_id == student_id)
                )
            )
            or 0
        )
        if events_left > 0:
            continue
        deleted_snapshots += await _execute_delete_count(
            session,
            delete(CausalFeatureSnapshot).where(CausalFeatureSnapshot.student_id == student_id),
        )
        deleted_outcomes += await _execute_delete_count(
            session,
            delete(CausalOutcomeEvent).where(CausalOutcomeEvent.student_id == student_id),
        )
        deleted_students += await _execute_delete_count(session, delete(Student).where(Student.id == student_id))

    return {
        "run_id": run_id,
        "status": "ok",
        "candidate_autocreated_schools": len(candidates),
        "noisy_schools_detected": len(noisy_rows),
        "schools_deleted": deleted_schools,
        "schools_blocked": len(blocked_schools),
        "blocked_school_samples": blocked_schools[:20],
        "admission_events_deleted": deleted_events,
        "causal_outcomes_deleted": deleted_outcomes,
        "feature_snapshots_deleted": deleted_snapshots,
        "students_deleted": deleted_students,
        "noisy_school_samples": [name for _, name in noisy_rows[:40]],
    }


def _fetch_posts(args: argparse.Namespace) -> tuple[list[CCTopic], list[CCPost]]:
    topics = _fetch_cc_topics(
        base_url=str(args.base_url),
        listing_mode=str(args.listing_mode),
        top_period=str(args.top_period),
        max_topics=max(1, int(args.max_topics)),
        topics_page_size=max(1, int(args.topics_page_size)),
        request_interval_seconds=float(args.request_interval_seconds),
    )

    posts: list[CCPost] = []
    for topic in topics:
        rows = _fetch_cc_topic_posts(
            base_url=str(args.base_url),
            topic=topic,
            max_posts_per_topic=max(1, int(args.max_posts_per_topic)),
            request_interval_seconds=float(args.request_interval_seconds),
        )
        posts.extend(rows)
        if len(posts) >= int(args.target_events) * 6:
            break
    return topics, posts


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id or f"admission-public-cc-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    should_cleanup = bool(args.cleanup_noisy_existing) or bool(args.cleanup_only)
    topics: list[CCTopic] = []
    posts: list[CCPost] = []

    if not bool(args.cleanup_only):
        topics, posts = _fetch_posts(args)

    output_root = Path(args.output_dir).expanduser().resolve() / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    async with async_session_factory() as session:
        before_counts = await _count_core_tables(session)
        eligible_before = await estimate_eligible_snapshots(session, lookback_days=540)
        cleanup_result: dict[str, Any] | None = None
        if should_cleanup:
            cleanup_result = await _cleanup_noisy_cc_import(session, run_id=run_id)
        school_index = await _load_school_index(session)
        metrics_context: MetricsSchoolContext | None = None
        ranked_allowlist_context: RankedSchoolAllowlistContext | None = None
        if bool(args.use_ranked_allowlist):
            ranked_allowlist_context = load_ranked_school_allowlist(
                version=(str(args.ranked_allowlist_version).strip() or None),
            )
        if bool(args.only_metrics_schools):
            metrics_context = await load_metrics_school_context(session)
        covered_school_ids = set(metrics_context.covered_school_ids if metrics_context else set())
        effective_create_missing = bool(args.create_missing_schools) and not bool(args.only_metrics_schools)

        created_students = 0
        created_schools = 0
        candidate_decisions = 0
        imported_events = 0
        alias_resolved_count = 0
        allowlist_matched_count = 0
        skipped_outside_ranked_scope = 0
        skipped_no_metrics_school = 0
        skipped_unknown_school = 0
        skipped_invalid_school_name = 0
        skipped_outside_ranked_scope_names: Counter[str] = Counter()
        skipped_no_metrics_school_names: Counter[str] = Counter()
        skipped_unknown_school_names: Counter[str] = Counter()
        skipped_invalid_school_names: Counter[str] = Counter()
        processed_posts = 0
        stage_counts: dict[str, int] = {}
        touched_student_ids: set[str] = set()
        touched_school_ids: set[str] = set()

        if not bool(args.cleanup_only):
            for post in posts:
                merged_text = f"{post.title}\n{post.body}"
                decisions: list[ParsedDecision] = _parse_decisions_cc(merged_text, topic_title=post.title)
                if not decisions:
                    continue
                profile: ParsedProfile = parse_profile(merged_text)
                cycle_year = int(args.cycle_year) if int(args.cycle_year) > 0 else int(post.created_at.year)
                student, student_created = await _get_or_create_student(
                    session,
                    post=post,
                    profile=profile,
                    cycle_year=cycle_year,
                )
                touched_student_ids.add(str(student.id))
                created_students += int(student_created)
                processed_posts += 1

                for decision in decisions:
                    candidate_decisions += 1
                    resolved_school_name, alias_changed = resolve_school_name_for_ingest(
                        decision.school_name,
                        context=metrics_context,
                        extra_alias_map=(
                            ranked_allowlist_context.alias_map if ranked_allowlist_context else None
                        ),
                    )
                    alias_resolved_count += int(alias_changed)
                    if ranked_allowlist_context is not None:
                        resolved_key = _normalise_school_key(resolved_school_name)
                        if resolved_key not in ranked_allowlist_context.allowed_keys:
                            skipped_outside_ranked_scope += 1
                            if resolved_school_name:
                                skipped_outside_ranked_scope_names.update([resolved_school_name])
                            continue
                        allowlist_matched_count += 1
                    school_lookup = await _get_or_create_school(
                        session,
                        school_name=resolved_school_name,
                        school_index=school_index,
                        create_missing=effective_create_missing,
                    )
                    if school_lookup.school is None:
                        if school_lookup.skip_reason == "invalid_school_name":
                            skipped_invalid_school_name += 1
                            if resolved_school_name:
                                skipped_invalid_school_names.update([resolved_school_name])
                        else:
                            skipped_unknown_school += 1
                            if resolved_school_name:
                                skipped_unknown_school_names.update([resolved_school_name])
                        continue
                    school = school_lookup.school
                    if bool(args.only_metrics_schools) and str(school.id) not in covered_school_ids:
                        skipped_no_metrics_school += 1
                        skipped_no_metrics_school_names.update([school.name or resolved_school_name])
                        continue
                    created_schools += int(school_lookup.created)
                    touched_school_ids.add(str(school.id))
                    source_key = f"cc:{post.post_id}:{decision.stage}:{_normalise_school_key(school.name)}"
                    before_event_count = int(
                        (
                            await session.scalar(
                                select(func.count()).select_from(AdmissionEvent).where(
                                    AdmissionEvent.student_id == student.id,
                                    AdmissionEvent.school_id == school.id,
                                    AdmissionEvent.cycle_year == cycle_year,
                                    AdmissionEvent.stage == decision.stage,
                                )
                            )
                        )
                        or 0
                    )
                    await register_admission_event(
                        session,
                        student_id=str(student.id),
                        school_id=str(school.id),
                        cycle_year=cycle_year,
                        stage=decision.stage,
                        major_bucket=(
                            str(profile.intended_majors[0]).strip()[:100] if profile.intended_majors else None
                        ),
                        happened_at=post.created_at,
                        evidence_ref=None,
                        source_name="collegeconfidential_public",
                        metadata={
                            "run_id": run_id,
                            "source_key": source_key,
                            "post_id": post.post_id,
                            "post_url": post.permalink,
                            "post_title": post.title[:300],
                            "topic_id": post.topic_id,
                        },
                    )
                    after_event_count = int(
                        (
                            await session.scalar(
                                select(func.count()).select_from(AdmissionEvent).where(
                                    AdmissionEvent.student_id == student.id,
                                    AdmissionEvent.school_id == school.id,
                                    AdmissionEvent.cycle_year == cycle_year,
                                    AdmissionEvent.stage == decision.stage,
                                )
                            )
                        )
                        or 0
                    )
                    created = int(after_event_count > before_event_count)
                    imported_events += created
                    if created:
                        stage_counts[decision.stage] = int(stage_counts.get(decision.stage) or 0) + 1
                    if imported_events >= int(args.target_events):
                        break
                if imported_events >= int(args.target_events):
                    break

        backfill_result: dict[str, Any] | None = None
        if bool(args.include_backfill) and not bool(args.cleanup_only) and touched_student_ids and touched_school_ids:
            backfill_result = await backfill_real_admission_assets(
                session,
                run_id=f"{run_id}:backfill",
                student_ids=sorted(touched_student_ids),
                school_ids=sorted(touched_school_ids),
                import_rows=[],
                include_school_evaluations=False,
                include_offers=False,
                include_admission_events=True,
                ingest_official_facts_enabled=False,
                cycle_year=(int(args.cycle_year) if int(args.cycle_year) > 0 else datetime.now(timezone.utc).year),
                active_outcomes=["admission_probability"],
                lookback_days=max(1, int(args.lookback_days)),
                min_true_per_outcome=max(1, int(args.min_true_per_outcome)),
                build_dataset=True,
                dataset_version=f"causal-public-cc-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}",
            )

        await session.commit()
        after_counts = await _count_core_tables(session)
        eligible_after = await estimate_eligible_snapshots(session, lookback_days=540)

    payload = {
        "status": "ok",
        "run_id": run_id,
        "request": {
            "base_url": args.base_url,
            "listing_mode": args.listing_mode,
            "top_period": args.top_period,
            "max_topics": int(args.max_topics),
            "topics_page_size": int(args.topics_page_size),
            "max_posts_per_topic": int(args.max_posts_per_topic),
            "target_events": int(args.target_events),
            "create_missing_schools": bool(args.create_missing_schools),
            "effective_create_missing_schools": bool(effective_create_missing),
            "only_metrics_schools": bool(args.only_metrics_schools),
            "use_ranked_allowlist": bool(args.use_ranked_allowlist),
            "ranked_allowlist_version": (
                ranked_allowlist_context.version if ranked_allowlist_context is not None else None
            ),
            "include_backfill": bool(args.include_backfill),
            "cycle_year": int(args.cycle_year),
            "cleanup_noisy_existing": bool(args.cleanup_noisy_existing),
            "cleanup_only": bool(args.cleanup_only),
        },
        "fetch": {
            "topics_fetched": len(topics),
            "posts_fetched": len(posts),
            "posts_processed_with_decisions": processed_posts,
        },
        "ingest": {
            "students_created": created_students,
            "schools_created": created_schools,
            "candidate_decisions": candidate_decisions,
            "events_imported": imported_events,
            "alias_resolved_count": alias_resolved_count,
            "allowlist_matched_count": allowlist_matched_count,
            "skipped_outside_ranked_scope": skipped_outside_ranked_scope,
            "skipped_no_metrics_school": skipped_no_metrics_school,
            "stage_counts": stage_counts,
            "skipped_unknown_school": skipped_unknown_school,
            "skipped_invalid_school_name": skipped_invalid_school_name,
            "skipped_outside_ranked_scope_top_schools": [
                {"school_name": str(name), "count": int(count)}
                for name, count in skipped_outside_ranked_scope_names.most_common(30)
            ],
            "skipped_no_metrics_school_top_schools": [
                {"school_name": str(name), "count": int(count)}
                for name, count in skipped_no_metrics_school_names.most_common(30)
            ],
            "skipped_unknown_school_top_schools": [
                {"school_name": str(name), "count": int(count)}
                for name, count in skipped_unknown_school_names.most_common(30)
            ],
            "skipped_invalid_school_name_top_schools": [
                {"school_name": str(name), "count": int(count)}
                for name, count in skipped_invalid_school_names.most_common(30)
            ],
            "target_events_reached": imported_events >= int(args.target_events),
            "touched_students": len(touched_student_ids),
            "touched_schools": len(touched_school_ids),
            "eligible_delta_estimate": int(eligible_after) - int(eligible_before),
        },
        "counts": {
            "before": before_counts,
            "after": after_counts,
            "delta": {
                key: int(after_counts.get(key) or 0) - int(before_counts.get(key) or 0)
                for key in sorted(set(before_counts) | set(after_counts))
            },
        },
        "cleanup_result": cleanup_result,
        "eligible_snapshots_540": {
            "before": int(eligible_before),
            "after": int(eligible_after),
            "delta": int(eligible_after) - int(eligible_before),
        },
        "backfill_result": backfill_result,
    }

    report_json = output_root / "admission_truth_public_report.json"
    report_md = output_root / "admission_truth_public_report.md"
    report_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_lines = [
        f"# Admission Truth Public Report `{run_id}`",
        "",
        f"- source: `collegeconfidential`",
        f"- listing_mode: `{args.listing_mode}` (top_period=`{args.top_period}`)",
        f"- topics_fetched: `{len(topics)}`",
        f"- posts_fetched: `{len(posts)}`",
        f"- posts_processed_with_decisions: `{processed_posts}`",
        f"- events_imported: `{imported_events}`",
        f"- target_events: `{int(args.target_events)}`",
        f"- target_events_reached: `{imported_events >= int(args.target_events)}`",
        f"- use_ranked_allowlist: `{bool(args.use_ranked_allowlist)}`",
        f"- ranked_allowlist_version: `{payload['request'].get('ranked_allowlist_version')}`",
        f"- students_created: `{created_students}`",
        f"- schools_created: `{created_schools}`",
        f"- touched_schools: `{len(touched_school_ids)}`",
        f"- allowlist_matched_count: `{allowlist_matched_count}`",
        f"- skipped_outside_ranked_scope: `{skipped_outside_ranked_scope}`",
        f"- alias_resolved_count: `{alias_resolved_count}`",
        f"- skipped_no_metrics_school: `{skipped_no_metrics_school}`",
        f"- skipped_invalid_school_name: `{skipped_invalid_school_name}`",
        f"- admission_events_delta: `{payload['counts']['delta'].get('admission_events', 0)}`",
        f"- causal_outcome_events_delta: `{payload['counts']['delta'].get('causal_outcome_events', 0)}`",
        f"- snapshots_delta: `{payload['counts']['delta'].get('causal_feature_snapshots', 0)}`",
    ]
    report_md.write_text("\n".join(md_lines), encoding="utf-8")
    payload["report_json"] = str(report_json)
    payload["report_md"] = str(report_md)

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
