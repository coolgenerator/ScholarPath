"""Community review collection (Reddit) and LLM-based report generation."""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models.community_review import CommunityReview, SchoolCommunityReport
from scholarpath.db.models.school import School
from scholarpath.llm.client import LLMClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# School → subreddit mapping (~40 well-known schools)
# ---------------------------------------------------------------------------

SCHOOL_SUBREDDIT_MAP: dict[str, list[str]] = {
    "Massachusetts Institute of Technology": ["MIT"],
    "Stanford University": ["stanford"],
    "Harvard University": ["Harvard"],
    "California Institute of Technology": ["Caltech"],
    "University of Chicago": ["uchicago"],
    "Princeton University": ["princeton"],
    "Yale University": ["yale"],
    "Columbia University": ["columbia"],
    "University of Pennsylvania": ["UPenn"],
    "Duke University": ["duke"],
    "Johns Hopkins University": ["jhu"],
    "Northwestern University": ["Northwestern"],
    "Cornell University": ["Cornell"],
    "Rice University": ["riceuniversity"],
    "Dartmouth College": ["dartmouth"],
    "Brown University": ["BrownU"],
    "Vanderbilt University": ["Vanderbilt"],
    "Washington University in St. Louis": ["washu"],
    "University of Notre Dame": ["notredame"],
    "Georgetown University": ["georgetown"],
    "Carnegie Mellon University": ["cmu"],
    "University of Virginia": ["UVA"],
    "University of California, Berkeley": ["berkeley"],
    "University of California, Los Angeles": ["ucla"],
    "University of Michigan, Ann Arbor": ["uofm"],
    "University of Southern California": ["USC"],
    "New York University": ["nyu"],
    "University of North Carolina at Chapel Hill": ["UNC"],
    "Boston University": ["BostonU"],
    "Boston College": ["bostoncollege"],
    "Georgia Institute of Technology": ["gatech"],
    "University of Illinois Urbana-Champaign": ["UIUC"],
    "University of Wisconsin-Madison": ["UWMadison"],
    "University of Texas at Austin": ["UTAustin"],
    "Purdue University": ["Purdue"],
    "University of Florida": ["ufl"],
    "Northeastern University": ["NEU"],
    "Emory University": ["Emory"],
    "Tufts University": ["Tufts"],
    "University of California, San Diego": ["UCSD"],
    "Williams College": ["williamscollege"],
    "Amherst College": ["amherstcollege"],
}

_REDDIT_HEADERS = {
    "User-Agent": "ScholarPath/1.0 (educational research bot)",
}

_XHS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

_REQUEST_DELAY_SECONDS = 6  # respect Reddit rate limits
_XHS_DELAY_SECONDS = 3


# ---------------------------------------------------------------------------
# Reddit fetching
# ---------------------------------------------------------------------------


async def fetch_reddit_posts_for_school(
    school_name: str,
    subreddits: list[str],
    limit: int = 20,
    *,
    http_client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    """Search Reddit public JSON API for posts about *school_name*.

    For each subreddit, searches for posts matching the school name, then
    fetches top-5 comments for every post.  Sleeps 6 s between requests to
    stay within Reddit's unauthenticated rate limits.

    Returns a list of dicts ready for persistence.
    """
    all_posts: list[dict[str, Any]] = []

    for subreddit in subreddits:
        query = urllib.parse.quote(school_name)
        search_url = (
            f"https://www.reddit.com/r/{subreddit}/search.json"
            f"?q={query}&restrict_sr=on&sort=top&t=all&limit={limit}"
        )

        try:
            resp = await http_client.get(
                search_url, headers=_REDDIT_HEADERS, follow_redirects=True, timeout=30.0,
            )
            if resp.status_code != 200:
                logger.warning(
                    "Reddit search returned %d for r/%s q=%s",
                    resp.status_code, subreddit, school_name,
                )
                await asyncio.sleep(_REQUEST_DELAY_SECONDS)
                continue

            data = resp.json()
        except Exception:
            logger.exception("Failed to fetch Reddit search for r/%s", subreddit)
            await asyncio.sleep(_REQUEST_DELAY_SECONDS)
            continue

        posts = data.get("data", {}).get("children", [])
        logger.info("r/%s: found %d posts for '%s'", subreddit, len(posts), school_name)

        await asyncio.sleep(_REQUEST_DELAY_SECONDS)

        for post_item in posts:
            post_data = post_item.get("data", {})
            post_id = post_data.get("id", "")
            permalink = post_data.get("permalink", "")

            # Fetch top comments
            top_comments = await _fetch_top_comments(
                http_client, permalink, max_comments=5,
            )
            await asyncio.sleep(_REQUEST_DELAY_SECONDS)

            created_utc = post_data.get("created_utc")
            post_created = (
                datetime.fromtimestamp(created_utc, tz=timezone.utc)
                if created_utc
                else None
            )

            all_posts.append({
                "post_id": post_id,
                "subreddit": subreddit,
                "post_title": post_data.get("title", ""),
                "post_body": post_data.get("selftext") or None,
                "post_score": post_data.get("score", 0),
                "post_url": f"https://www.reddit.com{permalink}",
                "post_created_utc": post_created,
                "top_comments": top_comments,
                "comment_count": post_data.get("num_comments", 0),
            })

    return all_posts


async def _fetch_top_comments(
    http_client: httpx.AsyncClient,
    permalink: str,
    max_comments: int = 5,
) -> list[dict[str, Any]]:
    """Fetch top-level comments for a Reddit post via its permalink."""
    url = f"https://www.reddit.com{permalink}.json?sort=top&limit={max_comments}"
    try:
        resp = await http_client.get(
            url, headers=_REDDIT_HEADERS, follow_redirects=True, timeout=30.0,
        )
        if resp.status_code != 200:
            return []
        listings = resp.json()
    except Exception:
        logger.exception("Failed to fetch comments for %s", permalink)
        return []

    if not isinstance(listings, list) or len(listings) < 2:
        return []

    comments_listing = listings[1].get("data", {}).get("children", [])
    results: list[dict[str, Any]] = []
    for child in comments_listing[:max_comments]:
        cdata = child.get("data", {})
        if child.get("kind") != "t1":
            continue
        results.append({
            "author": cdata.get("author", "[deleted]"),
            "body": cdata.get("body", ""),
            "score": cdata.get("score", 0),
        })
    return results


# ---------------------------------------------------------------------------
# Web search-based fetching (Xiaohongshu, Zhihu, 1point3acres, Niche, CC)
# ---------------------------------------------------------------------------

import re

# Source configs: (source_id, site_domain, search_terms_builder)
_WEB_SOURCES: list[dict[str, Any]] = [
    {
        "source_id": "xiaohongshu",
        "site": "xiaohongshu.com",
        "terms_fn": lambda name, cn: [cn or name, f"{name} 留学体验"],
        "limit": 8,
    },
    {
        "source_id": "zhihu",
        "site": "zhihu.com",
        "terms_fn": lambda name, cn: [f"{cn or name} 就读体验", f"{name} 怎么样"],
        "limit": 8,
    },
    {
        "source_id": "1point3acres",
        "site": "1point3acres.com",
        "terms_fn": lambda name, cn: [name, f"{cn or name} 录取"],
        "limit": 8,
    },
    {
        "source_id": "niche",
        "site": "niche.com",
        "terms_fn": lambda name, _cn: [f"{name} review", f"{name} student life"],
        "limit": 6,
    },
    {
        "source_id": "college_confidential",
        "site": "collegeconfidential.com",
        "terms_fn": lambda name, _cn: [f"{name}"],
        "limit": 6,
    },
]


async def _fetch_via_web_search(
    source_id: str,
    site_domain: str,
    search_terms: list[str],
    limit: int = 8,
    *,
    http_client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    """Generic DuckDuckGo site-search fetcher for any community platform."""
    all_posts: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for term in search_terms[:2]:
        query = urllib.parse.quote(f"site:{site_domain} {term}")
        search_url = f"https://html.duckduckgo.com/html/?q={query}"

        try:
            resp = await http_client.get(
                search_url, headers=_XHS_HEADERS, follow_redirects=True, timeout=15.0,
            )
            if resp.status_code != 200:
                logger.warning("%s search returned %d for '%s'", source_id, resp.status_code, term)
                await asyncio.sleep(_XHS_DELAY_SECONDS)
                continue

            html_text = resp.text
            # Extract results: links with title + snippet
            results = re.findall(
                r'<a[^>]+href="(https?://[^"]*)"[^>]*class="result__a"[^>]*>(.*?)</a>.*?<a[^>]+class="result__snippet"[^>]*>(.*?)</a>',
                html_text,
                re.DOTALL,
            )

            for url, raw_title, snippet in results[:limit]:
                if site_domain not in url:
                    continue
                clean_title = re.sub(r'<[^>]+>', '', raw_title).strip()
                clean_snippet = re.sub(r'<[^>]+>', '', snippet).strip()
                post_id = f"{source_id}_{hash(url) & 0xFFFFFFFF:08x}"

                if post_id not in seen_ids:
                    seen_ids.add(post_id)
                    all_posts.append({
                        "post_id": post_id,
                        "subreddit": source_id,
                        "post_title": clean_title,
                        "post_body": clean_snippet[:500] if clean_snippet else None,
                        "post_score": 0,
                        "post_url": url,
                        "post_created_utc": None,
                        "top_comments": [],
                        "comment_count": 0,
                    })

        except Exception:
            logger.exception("Failed %s search for '%s'", source_id, term)

        await asyncio.sleep(_XHS_DELAY_SECONDS)

    logger.info("%s: found %d posts", source_id, len(all_posts))
    return all_posts[:limit]


async def fetch_web_sources_for_school(
    school_name: str,
    school_name_cn: str | None = None,
    *,
    http_client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    """Fetch posts from all web-search-based sources (XHS, Zhihu, 1point3acres, Niche, CC)."""
    all_posts: list[dict[str, Any]] = []
    for src in _WEB_SOURCES:
        terms = src["terms_fn"](school_name, school_name_cn)
        posts = await _fetch_via_web_search(
            src["source_id"], src["site"], terms, limit=src["limit"],
            http_client=http_client,
        )
        all_posts.extend(posts)
    return all_posts


# ---------------------------------------------------------------------------
# Collection: fetch + deduplicate + persist
# ---------------------------------------------------------------------------


async def _persist_posts(
    session: AsyncSession,
    school_id: Any,
    posts: list[dict[str, Any]],
    existing_ids: set[str],
) -> int:
    """Deduplicate and persist a batch of posts. Commits immediately."""
    new_count = 0
    for post in posts:
        if post["post_id"] in existing_ids:
            continue
        review = CommunityReview(
            school_id=school_id,
            source="reddit" if post["subreddit"] not in ("xiaohongshu", "zhihu", "1point3acres", "niche", "college_confidential") else post["subreddit"],
            subreddit=post["subreddit"],
            post_id=post["post_id"],
            post_title=post["post_title"],
            post_body=post["post_body"],
            post_score=post["post_score"],
            post_url=post["post_url"],
            post_created_utc=post["post_created_utc"],
            top_comments=post["top_comments"],
            comment_count=post["comment_count"],
        )
        session.add(review)
        existing_ids.add(post["post_id"])
        new_count += 1
    if new_count:
        await session.commit()
        logger.info("Persisted %d new reviews (batch)", new_count)
    return new_count


async def collect_reviews_for_school(
    session: AsyncSession,
    school: School,
    *,
    http_client: httpx.AsyncClient,
    subreddits: list[str] | None = None,
    limit: int = 20,
) -> int:
    """Fetch posts from all sources, persisting after EACH source so the
    frontend can poll and see incremental results.

    Returns the total number of newly inserted reviews.
    """
    if subreddits is None:
        subreddits = SCHOOL_SUBREDDIT_MAP.get(school.name)

    # Load existing post_ids
    existing_result = await session.execute(
        select(CommunityReview.post_id).where(CommunityReview.school_id == school.id)
    )
    existing_ids: set[str] = {row[0] for row in existing_result.all()}
    total_new = 0

    # 1. School-specific subreddits
    if subreddits:
        posts = await fetch_reddit_posts_for_school(
            school.name, subreddits, limit=limit, http_client=http_client,
        )
        total_new += await _persist_posts(session, school.id, posts, existing_ids)

    # 2. General subreddits
    general_posts = await fetch_reddit_posts_for_school(
        school.name, ["ApplyingToCollege", "college"], limit=10, http_client=http_client,
    )
    total_new += await _persist_posts(session, school.id, general_posts, existing_ids)

    # 3. Web sources — one at a time, commit after each
    school_cn = getattr(school, 'name_cn', None)
    for src in _WEB_SOURCES:
        terms = src["terms_fn"](school.name, school_cn)
        posts = await _fetch_via_web_search(
            src["source_id"], src["site"], terms, limit=src["limit"],
            http_client=http_client,
        )
        total_new += await _persist_posts(session, school.id, posts, existing_ids)

    logger.info("Collection complete for %s: %d new reviews total", school.name, total_new)
    return total_new


# ---------------------------------------------------------------------------
# LLM report generation
# ---------------------------------------------------------------------------

_COMMUNITY_REPORT_SYSTEM_PROMPT = """\
You are an education research analyst. Given a collection of posts from
Reddit, 小红书, 知乎, 一亩三分地, Niche, and College Confidential about a
university, produce a structured community sentiment report.

Evaluate the school across exactly 5 dimensions:
1. academic_experience — teaching quality, coursework difficulty, academic support
2. campus_life — housing, food, clubs, social scene, campus feel
3. career_employment — internship/job placement, career services, alumni network
4. value_for_money — tuition vs. perceived return, financial aid, hidden costs
5. overall_vibe — general student satisfaction, would-they-choose-again sentiment

For EACH dimension provide:
- score: integer 1-10
- summary: 2-3 sentence summary written in Chinese (简体中文)
- key_quotes: list of 2-4 verbatim English quotes from the posts/comments

Also provide:
- overall_score: float average of the 5 dimension scores
- overall_summary: a 3-5 sentence Chinese summary of the school's community reputation

Respond with valid JSON only.
"""


async def generate_community_report(
    session: AsyncSession,
    llm: LLMClient,
    school: School,
) -> SchoolCommunityReport:
    """Load community reviews and generate a structured sentiment report.

    The report is persisted as a :class:`SchoolCommunityReport` row (upserted
    on the unique school_id constraint).
    """
    result = await session.execute(
        select(CommunityReview)
        .where(CommunityReview.school_id == school.id)
        .order_by(CommunityReview.post_score.desc())
        .limit(50)
    )
    reviews = list(result.scalars().all())

    if not reviews:
        raise ValueError(f"No community reviews found for {school.name}")

    # Build the user prompt
    review_texts: list[str] = []
    for r in reviews:
        entry = f"### [{r.subreddit}] {r.post_title} (score: {r.post_score})\n"
        if r.post_body:
            body = r.post_body[:800]
            entry += f"{body}\n"
        if r.top_comments:
            for c in r.top_comments[:3]:
                entry += f"  > {c.get('author', '?')}: {c.get('body', '')[:300]}\n"
        review_texts.append(entry)

    user_prompt = (
        f"School: {school.name}\n"
        f"Total reviews: {len(reviews)}\n\n"
        + "\n---\n".join(review_texts)
    )

    schema = {
        "academic_experience": {"score": 0, "summary": "", "key_quotes": [""]},
        "campus_life": {"score": 0, "summary": "", "key_quotes": [""]},
        "career_employment": {"score": 0, "summary": "", "key_quotes": [""]},
        "value_for_money": {"score": 0, "summary": "", "key_quotes": [""]},
        "overall_vibe": {"score": 0, "summary": "", "key_quotes": [""]},
        "overall_score": 0.0,
        "overall_summary": "",
    }

    messages = [
        {"role": "system", "content": _COMMUNITY_REPORT_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    report_data = await llm.complete_json(
        messages,
        schema=schema,
        temperature=0.3,
        max_tokens=4096,
        caller="community_report",
    )

    # Extract dimensions (the 5 dimension keys)
    dimension_keys = [
        "academic_experience",
        "campus_life",
        "career_employment",
        "value_for_money",
        "overall_vibe",
    ]
    dimensions = {k: report_data.get(k, {}) for k in dimension_keys}

    overall_score = report_data.get("overall_score")
    if overall_score is None:
        scores = [
            d.get("score", 5) for d in dimensions.values() if isinstance(d, dict)
        ]
        overall_score = sum(scores) / max(len(scores), 1)

    overall_summary = report_data.get("overall_summary", "")

    now = datetime.now(timezone.utc)

    # Upsert: delete existing report for this school, then insert
    existing = await session.execute(
        select(SchoolCommunityReport).where(
            SchoolCommunityReport.school_id == school.id,
        )
    )
    old_report = existing.scalars().first()
    if old_report:
        await session.delete(old_report)
        await session.flush()

    report = SchoolCommunityReport(
        school_id=school.id,
        review_count=len(reviews),
        dimensions=dimensions,
        overall_score=float(overall_score),
        overall_summary=overall_summary,
        generated_at=now,
        model_version="v1",
    )
    session.add(report)
    await session.flush()

    logger.info(
        "Generated community report for %s: overall=%.1f (%d reviews)",
        school.name, overall_score, len(reviews),
    )
    return report


# ---------------------------------------------------------------------------
# Real-time on-demand: collect + summarize in one call
# ---------------------------------------------------------------------------

# Track in-progress school IDs to avoid duplicate work
_generating_locks: dict[str, asyncio.Event] = {}


async def get_or_generate_report(
    session: AsyncSession,
    llm: LLMClient,
    school: School,
    *,
    max_age_hours: int = 168,  # 7 days
) -> SchoolCommunityReport | None:
    """Return existing report if fresh, otherwise collect + generate on demand.

    This is the main entry point for the real-time API. If no report exists
    (or it's stale), it triggers collection from Reddit + Xiaohongshu and
    LLM summarization, then returns the report.

    Returns None only if no reviews could be found at all.
    """
    school_key = str(school.id)

    # Check if another request is already generating for this school
    if school_key in _generating_locks:
        event = _generating_locks[school_key]
        await asyncio.wait_for(event.wait(), timeout=120)
        # Re-fetch the report that was just generated
        result = await session.execute(
            select(SchoolCommunityReport).where(
                SchoolCommunityReport.school_id == school.id,
            )
        )
        return result.scalars().first()

    # Check for existing fresh report
    result = await session.execute(
        select(SchoolCommunityReport).where(
            SchoolCommunityReport.school_id == school.id,
        )
    )
    existing = result.scalars().first()
    if existing:
        age_hours = (datetime.now(timezone.utc) - existing.generated_at).total_seconds() / 3600
        if age_hours < max_age_hours:
            return existing

    # Mark as generating
    event = asyncio.Event()
    _generating_locks[school_key] = event

    try:
        # Step 1: Collect reviews
        async with httpx.AsyncClient() as http_client:
            new_count = await collect_reviews_for_school(
                session, school, http_client=http_client,
            )
            logger.info("On-demand collection for %s: %d new reviews", school.name, new_count)

        # Step 2: Check if we have any reviews
        review_count = await session.execute(
            select(CommunityReview.id).where(
                CommunityReview.school_id == school.id,
            ).limit(1)
        )
        if not review_count.first():
            logger.info("No reviews found for %s, cannot generate report", school.name)
            return None

        # Step 3: Generate report
        report = await generate_community_report(session, llm, school)
        await session.commit()
        return report

    except Exception:
        logger.exception("Failed on-demand report for %s", school.name)
        return None
    finally:
        event.set()
        _generating_locks.pop(school_key, None)
