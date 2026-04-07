"""Ingest public admission truth events from Reddit r/collegeresults.

This script builds an evidence-backed admission truth stream without synthetic
labels. It parses public result posts, writes student/school/event rows, then
rebuilds real-only feature snapshots for the imported scope.
"""

from __future__ import annotations

import argparse
import asyncio
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
from scholarpath.scripts.admission_truth_public_shared import (
    MetricsSchoolContext,
    RankedSchoolAllowlistContext,
    estimate_eligible_snapshots,
    load_ranked_school_allowlist,
    load_metrics_school_context,
    resolve_school_name_for_ingest,
)
from scholarpath.services.causal_real_asset_service import backfill_real_admission_assets
from scholarpath.services.causal_data_service import register_admission_event

_STAGE_HEADING_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "admit": (
        re.compile(r"^(acceptances?|accepted|admitted|admits?)\b", re.I),
        re.compile(r"^likely letters?\b", re.I),
    ),
    "reject": (
        re.compile(r"^(rejections?|rejected|denied)\b", re.I),
    ),
    "waitlist": (
        re.compile(r"^(waitlists?|waitlisted)\b", re.I),
    ),
    "deferred": (
        re.compile(r"^(deferrals?|deferred)\b", re.I),
    ),
    "commit": (
        re.compile(r"^(commits?|committed|enrolling at)\b", re.I),
    ),
}

_SECTION_STOP_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^(demographics?|academics?|awards?|ecs?|extracurriculars?|essays?|letters?|interviews?)\b", re.I),
    re.compile(r"^(stats?|scores?)\b", re.I),
    re.compile(r"^(additional information|thoughts?|results?)\b", re.I),
)

_NON_SCHOOL_TOKENS = {
    "all uc",
    "all ucs",
    "all ivies",
    "all t20",
    "all t50",
    "state schools",
    "safeties",
    "targets",
    "reaches",
}

_SHORT_SCHOOL_ALIASES = {
    "mit",
    "cmu",
    "caltech",
    "ucla",
    "ucb",
    "uc berkeley",
    "berkeley",
    "ucsd",
    "uc san diego",
    "uci",
    "uc irvine",
    "ucd",
    "uc davis",
    "ucsb",
    "uc santa barbara",
    "ucsc",
    "uc santa cruz",
    "nyu",
    "usc",
    "umd",
    "umd cp",
    "umich",
    "uiuc",
    "gatech",
    "gt",
    "uw",
    "uw seattle",
    "uw madison",
    "uva",
    "pitt",
    "rutgers",
    "purdue",
    "cornell",
    "harvard",
    "yale",
    "princeton",
    "stanford",
    "duke",
    "brown",
    "dartmouth",
    "upenn",
    "columbia",
    "northwestern",
    "vanderbilt",
    "emory",
    "georgetown",
    "nd",
    "notre dame",
    "sjsu",
    "sdsu",
    "slo",
    "cal poly",
    "psu",
    "osu",
    "bu",
    "bc",
}

_SCHOOL_KEYWORD_TOKENS = {
    "university",
    "college",
    "institute",
    "polytechnic",
    "academy",
    "school",
    "campus",
}

_NARRATIVE_TOKENS = {
    "i",
    "my",
    "me",
    "we",
    "our",
    "you",
    "your",
    "he",
    "she",
    "they",
    "because",
    "after",
    "before",
    "while",
    "when",
    "think",
    "feel",
    "surprised",
    "excited",
    "disappointed",
    "essay",
    "essays",
    "grade",
    "grades",
    "cost",
    "tuition",
}

_NOISY_SCHOOL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\\"),
    re.compile(
        r"\b(reject(?:ed|ion|ions)?|waitlist(?:ed|s)?|accept(?:ed|ance|ances)?|admit(?:ted|s)?|defer(?:red|rals?)?|committed?)\b",
        re.I,
    ),
    re.compile(r"\b(gpa|sat|act|ap|ecs?|essay|interview|major|income|grant|aid)\b", re.I),
    re.compile(r"https?://|www\.", re.I),
    re.compile(r"[!?\[\]{}:$]|[%]|[#]"),
    re.compile(r"[$€£]"),
    re.compile(r"[0-9]{3,}"),
)

_RAW_NOISY_SCHOOL_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\s*[-+*]+\s*"),
    re.compile(r"&amp;", re.I),
    re.compile(r"\\"),
)

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

_QUESTION_SENTENCE_PREFIX = re.compile(
    r"^(?:do|does|did|can|could|should|would|is|are|am|what|why|how|when|where|which|who)\b",
    re.I,
)


@dataclass(slots=True)
class RedditPost:
    post_id: str
    title: str
    body: str
    permalink: str
    created_at: datetime


@dataclass(slots=True)
class ParsedProfile:
    gpa: float
    gpa_scale: str
    sat_total: int | None
    act_composite: int | None
    intended_majors: list[str]
    budget_usd: int
    need_financial_aid: bool


@dataclass(slots=True)
class ParsedDecision:
    stage: str
    school_name: str


@dataclass(slots=True)
class SchoolLookupResult:
    school: School | None
    created: bool
    skip_reason: str | None = None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest public Reddit admission truth events.")
    parser.add_argument("--subreddit", default="collegeresults", help="Target subreddit (default: collegeresults).")
    parser.add_argument(
        "--listing-mode",
        default="new",
        choices=["new", "top", "hot"],
        help="Reddit listing mode (default: new).",
    )
    parser.add_argument(
        "--top-time",
        default="year",
        choices=["hour", "day", "week", "month", "year", "all"],
        help="Time filter for listing-mode=top (default: year).",
    )
    parser.add_argument("--max-posts", type=int, default=1200, help="Maximum posts to fetch (default: 1200).")
    parser.add_argument("--page-size", type=int, default=100, help="Reddit listing page size (default: 100).")
    parser.add_argument(
        "--target-events",
        type=int,
        default=5000,
        help="Stop once parsed decisions reach this target (default: 5000).",
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
        help="Skip post-import snapshot/dataset backfill.",
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
        default=0.45,
        help="Throttle interval between Reddit listing calls (default: 0.45s).",
    )
    parser.add_argument(
        "--include-comments",
        dest="include_comments",
        action="store_true",
        default=True,
        help="Also parse Reddit comments for admission decisions (default: true).",
    )
    parser.add_argument(
        "--no-include-comments",
        dest="include_comments",
        action="store_false",
        help="Disable Reddit comments ingestion and only parse post bodies.",
    )
    parser.add_argument(
        "--max-comments-per-post",
        type=int,
        default=80,
        help="Maximum parsed comments per post when include-comments=true (default: 80).",
    )
    parser.add_argument(
        "--max-comment-records",
        type=int,
        default=5000,
        help="Global cap for parsed comment records per run (default: 5000).",
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
        help="Cleanup noisy placeholder schools/events from prior Reddit imports before ingest.",
    )
    parser.add_argument(
        "--cleanup-only",
        action="store_true",
        default=False,
        help="Run noisy cleanup only and skip new Reddit fetching/ingest.",
    )
    return parser


def _clean_markdown_line(value: str) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    text = text.replace("\\", " ")
    text = re.sub(r"[*_`>#~]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _detect_stage_heading(cleaned_line: str) -> tuple[str | None, str]:
    line = cleaned_line.strip()
    if not line:
        return None, ""
    lower = line.lower()
    for stage, patterns in _STAGE_HEADING_PATTERNS.items():
        for pattern in patterns:
            if pattern.search(lower):
                # Handle inline section, e.g. "Acceptances: MIT, Stanford".
                payload = line
                if ":" in line:
                    payload = line.split(":", 1)[1].strip()
                else:
                    payload = ""
                return stage, payload
    return None, ""


def _is_section_stop(cleaned_line: str) -> bool:
    line = cleaned_line.strip()
    if not line:
        return False
    for pattern in _SECTION_STOP_PATTERNS:
        if pattern.search(line):
            return True
    return False


def _line_can_contain_school_list(raw_line: str, cleaned_line: str) -> bool:
    line = cleaned_line.strip()
    if not line:
        return False
    lower = line.lower()
    if any(token in lower.split() for token in _NARRATIVE_TOKENS):
        return False
    if len(line) > 160 and all(sep not in line for sep in [",", ";", "/", "|"]):
        return False
    has_sentence_punctuation = bool(re.search(r"[.!?]", line))
    raw_strip = str(raw_line or "").lstrip()
    bullet_like = raw_strip.startswith(("-", "*", "•", "+"))
    has_list_separator = any(sep in line for sep in [",", ";", "/", "|"])
    if bullet_like or has_list_separator:
        return True
    if has_sentence_punctuation:
        return False
    return len(line.split()) <= 6


def _split_school_candidates(payload: str) -> list[str]:
    text = _clean_markdown_line(payload)
    if not text:
        return []
    text = re.sub(r"\b(EA|ED|REA|RD|SCEA)\b[:\s-]*", " ", text, flags=re.I)
    parts = re.split(r"\s*[;,/]\s*", text)
    if len(parts) == 1 and " and " in text.lower():
        parts = re.split(r"\s+\band\b\s+", text, flags=re.I)

    out: list[str] = []
    for part in parts:
        candidate = part.strip(" .:-")
        if not candidate:
            continue
        candidate = re.sub(r"^[+\-•]+\s*", "", candidate)
        candidate = re.split(r"\s*(?:->|=>)\s*", candidate, maxsplit=1)[0]
        candidate = re.split(r"\s*:\s*", candidate, maxsplit=1)[0]
        candidate = re.split(r"\s*[❌✅✔✖]\s*", candidate, maxsplit=1)[0]
        candidate = re.split(r"\s*\$\s*", candidate, maxsplit=1)[0]
        candidate = re.split(r"\s+[-|]\s+", candidate, maxsplit=1)[0]
        candidate = re.split(r"\s*\(", candidate, maxsplit=1)[0]
        candidate = re.sub(r"^(at|to)\s+", "", candidate, flags=re.I).strip()
        candidate = re.sub(r"\s+", " ", candidate).strip(" .:-")
        if _looks_like_school_name(candidate):
            out.append(candidate)
    return out


def _looks_like_school_name(value: str) -> bool:
    text = _clean_markdown_line(value).strip(" .:-")
    if not text:
        return False
    if len(text) < 2 or len(text) > 120:
        return False
    low = re.sub(r"\s+", " ", text.lower()).strip()
    if low in _NON_SCHOOL_TOKENS:
        return False
    if any(pattern.search(text) for pattern in _NOISY_SCHOOL_PATTERNS):
        return False
    if not re.search(r"[a-zA-Z]", text):
        return False
    words = [piece for piece in re.split(r"\s+", re.sub(r"[^a-zA-Z&'.-]+", " ", text)) if piece]
    if not words:
        return False
    if len(words) > 8:
        return False
    if any(word.lower() in _NARRATIVE_TOKENS for word in words):
        return False
    normalized = re.sub(r"[.\-]+", " ", low)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if normalized in _SHORT_SCHOOL_ALIASES:
        return True
    has_keyword = any(keyword in normalized for keyword in _SCHOOL_KEYWORD_TOKENS)
    if len(words) == 1 and not has_keyword:
        return False
    uppercase_tokens = sum(1 for word in words if word.isupper() or word[:1].isupper())
    if uppercase_tokens == 0 and not has_keyword:
        return False
    return True


def _is_noisy_autocreated_school_name(value: str) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return True
    if any(pattern.search(raw) for pattern in _RAW_NOISY_SCHOOL_PATTERNS):
        return True
    text = _clean_markdown_line(raw).strip()
    if not _looks_like_school_name(text):
        return True
    low = text.lower()
    if len(text.split()) > 10:
        return True
    if text.endswith("&"):
        return True
    if re.search(r"\b(and|but|because|after|before|while|likely|guessing)\b", low):
        return True
    return False


def parse_decisions(text: str) -> list[ParsedDecision]:
    current_stage: str | None = None
    decisions: list[ParsedDecision] = []
    seen: set[tuple[str, str]] = set()
    merged = str(text or "")
    for raw_line in merged.splitlines():
        line = _clean_markdown_line(raw_line)
        if not line:
            continue

        stage_from_heading, inline_payload = _detect_stage_heading(line)
        if stage_from_heading:
            current_stage = stage_from_heading
            if inline_payload:
                for school_name in _split_school_candidates(inline_payload):
                    key = (current_stage, school_name.lower())
                    if key not in seen:
                        decisions.append(ParsedDecision(stage=current_stage, school_name=school_name))
                        seen.add(key)
            continue

        if _is_section_stop(line):
            current_stage = None
            continue

        if not current_stage:
            continue

        if not _line_can_contain_school_list(raw_line, line):
            continue

        for school_name in _split_school_candidates(line):
            key = (current_stage, school_name.lower())
            if key not in seen:
                decisions.append(ParsedDecision(stage=current_stage, school_name=school_name))
                seen.add(key)

    # Reddit posts often use prose instead of section headers; recover high-precision cases.
    for fragment in re.split(r"[\n\.\!\?]+", merged):
        sentence = _clean_markdown_line(fragment).strip()
        if len(sentence) < 12:
            continue
        if _QUESTION_SENTENCE_PREFIX.match(sentence):
            continue
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
                    decisions.append(ParsedDecision(stage=stage, school_name=school_name))
                    seen.add(key)
    return decisions


def parse_profile(text: str) -> ParsedProfile:
    merged = _clean_markdown_line(text)
    lower = merged.lower()

    gpa = 3.7
    gpa_scale = "4.0"
    gpa_match = re.search(r"\bgpa[^0-9]{0,20}([0-9](?:\.[0-9]{1,3})?)", merged, flags=re.I)
    if gpa_match:
        try:
            gpa_raw = float(gpa_match.group(1))
            if 0.0 < gpa_raw <= 5.5:
                gpa = min(gpa_raw, 4.0) if gpa_raw <= 4.5 else gpa_raw
                gpa_scale = "4.0" if gpa_raw <= 4.5 else "5.0"
            elif 5.5 < gpa_raw <= 100.0:
                gpa = gpa_raw
                gpa_scale = "100"
        except ValueError:
            pass

    sat_total: int | None = None
    sat_match = re.search(r"\bsat[^0-9]{0,20}(1[0-6][0-9]{2}|[4-9][0-9]{2})\b", merged, flags=re.I)
    if sat_match:
        sat_total = int(sat_match.group(1))

    act_composite: int | None = None
    act_match = re.search(r"\bact[^0-9]{0,20}([1-3]?[0-9])\b", merged, flags=re.I)
    if act_match:
        value = int(act_match.group(1))
        if 1 <= value <= 36:
            act_composite = value

    intended_majors: list[str] = []
    major_match = re.search(r"intended major(?:s)?[^:]{0,20}:\s*([^*#\n\r]{1,180})", text, flags=re.I)
    if major_match:
        major_text = _clean_markdown_line(major_match.group(1))
        raw_majors = [piece.strip() for piece in re.split(r"[,/;]| and ", major_text, flags=re.I) if piece.strip()]
        intended_majors = []
        for piece in raw_majors:
            normalized = re.sub(r"\s+", " ", piece).strip()[:80]
            if not normalized:
                continue
            if re.search(r"\b(gpa|sat|act|ap|ib|honors|rank|income|school)\b", normalized, flags=re.I):
                continue
            intended_majors.append(normalized)
            if len(intended_majors) >= 4:
                break

    need_financial_aid = bool(
        re.search(r"\b(low[- ]income|need[- ]based|need aid|financial aid|first[- ]gen|fgli|questbridge)\b", lower)
    )
    budget_usd = 60000
    if re.search(r"\b(full pay|upper[- ]income|high income)\b", lower):
        budget_usd = 110000
    elif re.search(r"\b(upper middle|middle class)\b", lower):
        budget_usd = 70000
    elif re.search(r"\b(low[- ]income|working class|aid needed)\b", lower):
        budget_usd = 25000

    return ParsedProfile(
        gpa=gpa,
        gpa_scale=gpa_scale,
        sat_total=sat_total,
        act_composite=act_composite,
        intended_majors=intended_majors,
        budget_usd=budget_usd,
        need_financial_aid=need_financial_aid,
    )


def _normalise_school_key(name: str) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", str(name or "").lower())
    text = re.sub(r"\s+", " ", text).strip()
    return text


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
            "source": "reddit_collegeresults_public",
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
    post: RedditPost,
    profile: ParsedProfile,
    cycle_year: int,
) -> tuple[Student, bool]:
    email = f"reddit_{post.post_id}@public-data.local"
    existing = await session.scalar(select(Student).where(Student.email == email))
    if existing is not None:
        return existing, False
    student = Student(
        name=f"Public Applicant {post.post_id}",
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
        extracurriculars={"source": "reddit_collegeresults_public"},
        awards=None,
        intended_majors=profile.intended_majors or None,
        budget_usd=max(1, int(profile.budget_usd)),
        need_financial_aid=bool(profile.need_financial_aid),
        preferences={
            "source": "reddit_collegeresults_public",
            "post_id": post.post_id,
            "post_url": post.permalink,
        },
        ed_preference=None,
        target_year=cycle_year,
        profile_completed=True,
        profile_embedding=None,
    )
    session.add(student)
    await session.flush()
    return student, True


def _fetch_reddit_posts(
    *,
    subreddit: str,
    listing_mode: str,
    top_time: str,
    max_posts: int,
    page_size: int,
    request_interval_seconds: float,
    include_comments: bool,
    max_comments_per_post: int,
    max_comment_records: int,
) -> list[RedditPost]:
    def _fetch_post_comments(*, post: RedditPost, max_comments: int) -> list[RedditPost]:
        if max_comments <= 0:
            return []
        response = client.get(
            f"https://www.reddit.com/comments/{post.post_id}.json",
            params={"limit": 500, "sort": "new", "depth": 5},
        )
        if response.status_code != 200:
            return []
        payload = (
            response.json() if response.headers.get("content-type", "").startswith("application/json") else []
        )
        if not isinstance(payload, list) or len(payload) < 2:
            return []

        comment_tree = ((((payload[1] or {}).get("data") or {}).get("children")) or [])
        records: list[RedditPost] = []

        def _walk(nodes: list[Any]) -> None:
            for node in nodes:
                if len(records) >= max_comments:
                    return
                if not isinstance(node, dict):
                    continue
                if str(node.get("kind") or "") != "t1":
                    continue
                row = (node.get("data") or {}) if isinstance(node.get("data"), dict) else {}
                comment_id = str(row.get("id") or "").strip()
                body = str(row.get("body") or "")
                if comment_id and len(body.strip()) >= 30:
                    records.append(
                        RedditPost(
                            post_id=f"{post.post_id}-c-{comment_id}",
                            title=post.title,
                            body=body,
                            permalink=f"https://www.reddit.com{str(row.get('permalink') or '').strip()}",
                            created_at=datetime.fromtimestamp(
                                float(row.get("created_utc") or 0),
                                tz=timezone.utc,
                            ),
                        )
                    )
                replies = row.get("replies")
                if isinstance(replies, dict):
                    child_nodes = (((replies or {}).get("data") or {}).get("children") or [])
                    if isinstance(child_nodes, list) and child_nodes:
                        _walk(child_nodes)
                if len(records) >= max_comments:
                    return

        _walk(comment_tree if isinstance(comment_tree, list) else [])
        return records[:max_comments]

    client = httpx.Client(
        headers={"User-Agent": "ScholarPathDataBot/1.0 (public research; contact: support@scholarpath.local)"},
        timeout=20.0,
        follow_redirects=True,
    )
    posts: list[RedditPost] = []
    seen_post_ids: set[str] = set()
    comment_records = 0
    after: str | None = None
    safe_listing_mode = str(listing_mode or "new").strip().lower() or "new"
    if safe_listing_mode not in {"new", "top", "hot"}:
        safe_listing_mode = "new"
    while len(posts) < max_posts:
        params = {"limit": min(page_size, 100)}
        if after:
            params["after"] = after
        if safe_listing_mode == "top":
            params["t"] = str(top_time or "year").strip().lower() or "year"
        url = f"https://www.reddit.com/r/{subreddit}/{safe_listing_mode}.json"
        response = client.get(url, params=params)
        if response.status_code != 200:
            break
        payload = response.json()
        data = payload.get("data") or {}
        children = data.get("children") or []
        if not children:
            break
        for item in children:
            row = (item or {}).get("data") or {}
            post_id = str(row.get("id") or "").strip()
            if not post_id:
                continue
            if post_id in seen_post_ids:
                continue
            body = str(row.get("selftext") or "")
            if len(body.strip()) < 30:
                continue
            seen_post_ids.add(post_id)
            post = RedditPost(
                post_id=post_id,
                title=str(row.get("title") or "").strip(),
                body=body,
                permalink=f"https://www.reddit.com{str(row.get('permalink') or '').strip()}",
                created_at=datetime.fromtimestamp(float(row.get("created_utc") or 0), tz=timezone.utc),
            )
            posts.append(post)
            if include_comments and comment_records < max(0, int(max_comment_records)):
                remaining = max(0, int(max_comment_records) - int(comment_records))
                per_post_cap = min(max(1, int(max_comments_per_post)), remaining)
                for comment_post in _fetch_post_comments(post=post, max_comments=per_post_cap):
                    if comment_post.post_id in seen_post_ids:
                        continue
                    seen_post_ids.add(comment_post.post_id)
                    posts.append(comment_post)
                    comment_records += 1
                    if len(posts) >= max_posts:
                        break
                time.sleep(max(0.0, request_interval_seconds))
            if len(posts) >= max_posts:
                break
        after = data.get("after")
        if not after:
            break
        time.sleep(max(0.0, request_interval_seconds))
    client.close()
    return posts


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
    rowcount = int(result.rowcount or 0)
    return max(0, rowcount)


async def _cleanup_noisy_reddit_import(session: AsyncSession, *, run_id: str) -> dict[str, Any]:
    school_rows = list((await session.execute(select(School.id, School.name, School.metadata_))).all())
    candidates: list[tuple[Any, str]] = []
    noisy_rows: list[tuple[Any, str]] = []
    for row in school_rows:
        metadata = row.metadata_ if isinstance(row.metadata_, dict) else {}
        if str(metadata.get("source") or "").strip() != "reddit_collegeresults_public":
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
    noisy_samples = [name for _, name in noisy_rows[:40]]

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
                        AdmissionEvent.source_name == "reddit_collegeresults_public",
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
                select(Student.id).where(
                    Student.email.like("reddit_%@public-data.local"),
                )
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
        "noisy_school_samples": noisy_samples,
    }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id or f"admission-public-reddit-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
    should_cleanup = bool(args.cleanup_noisy_existing) or bool(args.cleanup_only)
    posts: list[RedditPost] = []
    if not bool(args.cleanup_only):
        posts = _fetch_reddit_posts(
            subreddit=str(args.subreddit).strip(),
            listing_mode=str(args.listing_mode).strip(),
            top_time=str(args.top_time).strip(),
            max_posts=max(1, int(args.max_posts)),
            page_size=max(1, int(args.page_size)),
            request_interval_seconds=float(args.request_interval_seconds),
            include_comments=bool(args.include_comments),
            max_comments_per_post=max(1, int(args.max_comments_per_post)),
            max_comment_records=max(0, int(args.max_comment_records)),
        )

    output_root = Path(args.output_dir).expanduser().resolve() / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    async with async_session_factory() as session:
        before_counts = await _count_core_tables(session)
        eligible_before = await estimate_eligible_snapshots(session, lookback_days=540)
        cleanup_result: dict[str, Any] | None = None
        if should_cleanup:
            cleanup_result = await _cleanup_noisy_reddit_import(session, run_id=run_id)
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
        processed_posts = 0
        stage_counts: dict[str, int] = {}
        touched_student_ids: set[str] = set()
        touched_school_ids: set[str] = set()

        if not bool(args.cleanup_only):
            for post in posts:
                decisions = parse_decisions(f"{post.title}\n{post.body}")
                if not decisions:
                    continue
                profile = parse_profile(f"{post.title}\n{post.body}")
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
                        else:
                            skipped_unknown_school += 1
                        continue
                    school = school_lookup.school
                    if bool(args.only_metrics_schools) and str(school.id) not in covered_school_ids:
                        skipped_no_metrics_school += 1
                        continue
                    created_schools += int(school_lookup.created)
                    touched_school_ids.add(str(school.id))
                    source_key = f"reddit:{post.post_id}:{decision.stage}:{_normalise_school_key(school.name)}"
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
                        source_name="reddit_collegeresults_public",
                        metadata={
                            "run_id": run_id,
                            "source_key": source_key,
                            "post_id": post.post_id,
                            "post_url": post.permalink,
                            "post_title": post.title[:300],
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
                dataset_version=f"causal-public-reddit-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}",
            )

        await session.commit()
        after_counts = await _count_core_tables(session)
        eligible_after = await estimate_eligible_snapshots(session, lookback_days=540)

    payload = {
        "status": "ok",
        "run_id": run_id,
        "request": {
            "subreddit": args.subreddit,
            "listing_mode": args.listing_mode,
            "top_time": args.top_time,
            "max_posts": int(args.max_posts),
            "page_size": int(args.page_size),
            "target_events": int(args.target_events),
            "create_missing_schools": bool(args.create_missing_schools),
            "effective_create_missing_schools": bool(effective_create_missing),
            "only_metrics_schools": bool(args.only_metrics_schools),
            "use_ranked_allowlist": bool(args.use_ranked_allowlist),
            "ranked_allowlist_version": (
                ranked_allowlist_context.version if ranked_allowlist_context is not None else None
            ),
            "include_backfill": bool(args.include_backfill),
            "include_comments": bool(args.include_comments),
            "max_comments_per_post": int(args.max_comments_per_post),
            "max_comment_records": int(args.max_comment_records),
            "cycle_year": int(args.cycle_year),
            "cleanup_noisy_existing": bool(args.cleanup_noisy_existing),
            "cleanup_only": bool(args.cleanup_only),
        },
        "fetch": {
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
        f"- subreddit: `{args.subreddit}`",
        f"- listing_mode: `{args.listing_mode}` (top_time=`{args.top_time}`)",
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
