"""Claim extraction from community reviews and argument graph construction.

Extracts structured claims from community reviews using an LLM, builds a
NetworkX argument graph with topic/claim/evidence nodes and
supports/contradicts relationships, then propagates beliefs using the
existing causal inference engine.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import networkx as nx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.causal.belief_propagation import NoisyORPropagator
from scholarpath.causal.graph_store import graph_to_cytoscape
from scholarpath.db.models.community_review import CommunityReview
from scholarpath.db.models.school import School
from scholarpath.db.models.school_claims import SchoolClaims
from scholarpath.llm.client import LLMClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# LLM prompt for claim extraction
# ---------------------------------------------------------------------------

_CLAIMS_EXTRACTION_SYSTEM_PROMPT = """\
You are a higher-education research analyst. Given a collection of community
posts and comments about a university, extract the key claims students and
alumni make about the school.

Extract 10-20 distinct claims. For each claim:
1. Assign a unique id like "claim_1", "claim_2", etc.
2. Write the claim text in English (text) and Chinese (text_cn).
3. Classify the topic: "academic", "campus_life", "career", "financial", or "vibe".
4. Classify sentiment: "positive", "negative", or "neutral".
5. Rate strength from 1-10 (how strongly/frequently this claim is made).
6. Count how many separate reviews/comments mention this claim (source_count).
7. Provide 1-3 verbatim quote snippets as evidence, each with the source URL.
   Use format: {"quote": "text", "url": "https://..."}

Also identify pairs of contradicting claims (max 5-8 pairs). Two claims
contradict if they make opposing assertions about the same aspect.

Respond with valid JSON only, using this schema:
{
  "claims": [
    {
      "id": "claim_1",
      "text": "English claim text",
      "text_cn": "中文观点",
      "topic": "academic",
      "sentiment": "positive",
      "strength": 8,
      "source_count": 5,
      "evidence": [{"quote": "quote text", "url": "https://..."}]
    }
  ],
  "contradictions": [
    {
      "claim_a": "claim_1",
      "claim_b": "claim_3",
      "aspect": "what aspect they disagree on",
      "severity": "high",
      "analysis": "Brief explanation of the contradiction in Chinese",
      "analysis_en": "Brief explanation of the contradiction in English"
    }
  ]
}

Rules:
- Each claim must be a specific, factual assertion (not vague).
- Strength should reflect how widely/strongly the claim appears in the data.
- Contradictions should only link claims with genuinely opposing views.
- Severity: "low" = minor disagreement, "medium" = notable, "high" = major divide.
"""


# ---------------------------------------------------------------------------
# Claim extraction
# ---------------------------------------------------------------------------


async def extract_claims(
    reviews: list[CommunityReview],
    llm: LLMClient,
) -> dict[str, Any]:
    """Send reviews to LLM and extract structured claims + contradictions.

    Returns a dict with ``claims`` (list) and ``contradictions`` (list).
    """
    review_texts: list[str] = []
    # Build source index: subreddit → [urls]
    source_urls: dict[str, list[str]] = {}
    for r in reviews:
        entry = f"### [{r.subreddit}] {r.post_title} (score: {r.post_score})\nURL: {r.post_url}\n"
        if r.post_body:
            entry += f"{r.post_body[:800]}\n"
        if r.top_comments:
            for c in r.top_comments[:3]:
                entry += f"  > {c.get('author', '?')}: {c.get('body', '')[:300]}\n"
        review_texts.append(entry)
        source_urls.setdefault(r.subreddit, []).append(r.post_url)

    user_prompt = (
        f"Total reviews: {len(reviews)}\n\n"
        + "\n---\n".join(review_texts)
    )

    schema = {
        "claims": [
            {
                "id": "",
                "text": "",
                "text_cn": "",
                "topic": "",
                "sentiment": "",
                "strength": 0,
                "source_count": 0,
                "evidence": [{"quote": "", "url": ""}],
            }
        ],
        "contradictions": [
            {
                "claim_a": "",
                "claim_b": "",
                "aspect": "",
                "severity": "",
                "analysis": "",
                "analysis_en": "",
            }
        ],
    }

    messages = [
        {"role": "system", "content": _CLAIMS_EXTRACTION_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    result = await llm.complete_json(
        messages,
        schema=schema,
        temperature=0.3,
        max_tokens=6000,
        caller="claims_extraction",
    )

    claims = result.get("claims", [])
    contradictions = result.get("contradictions", [])

    logger.info(
        "Extracted %d claims and %d contradictions",
        len(claims),
        len(contradictions),
    )
    return {"claims": claims, "contradictions": contradictions, "source_urls": source_urls}


# ---------------------------------------------------------------------------
# Argument graph construction
# ---------------------------------------------------------------------------

# Node colors for cytoscape override
_CLAIM_COLORS: dict[str, str] = {
    "positive": "#22c55e",   # green
    "negative": "#ef4444",   # red
    "neutral": "#94a3b8",    # gray
}

_TOPIC_COLOR = "#3b82f6"     # blue
_EVIDENCE_COLOR = "#d1d5db"  # light gray


def build_argument_graph(
    claims: list[dict[str, Any]],
    contradictions: list[dict[str, Any]],
) -> nx.DiGraph:
    """Build a NetworkX DiGraph representing the argument structure.

    Node types:
    - ``topic``: one of the five topic categories
    - ``claim``: an extracted claim with prior_belief = strength / 10
    - ``evidence``: a supporting quote snippet

    Edge types:
    - ``belongs_to``: topic -> claim
    - ``supports``: evidence -> claim
    - ``contradicts``: claim <-> claim (bidirectional)

    After construction, runs NoisyOR belief propagation.
    """
    dag = nx.DiGraph()

    # --- Topic nodes ---
    topics = {
        "academic": "Academic Experience / 学术体验",
        "campus_life": "Campus Life / 校园生活",
        "career": "Career & Employment / 就业前景",
        "financial": "Financial & Value / 性价比",
        "vibe": "Overall Vibe / 整体氛围",
    }
    for topic_id, label in topics.items():
        dag.add_node(
            topic_id,
            label=label,
            node_type="topic",
            prior_belief=0.5,
            confidence=0.8,
            color=_TOPIC_COLOR,
        )

    # --- Claim nodes ---
    claim_ids: set[str] = set()
    for claim in claims:
        cid = claim.get("id", "")
        if not cid:
            continue
        claim_ids.add(cid)

        sentiment = claim.get("sentiment", "neutral")
        strength = max(1, min(10, claim.get("strength", 5)))

        dag.add_node(
            cid,
            label=claim.get("text", cid)[:60],
            label_full=claim.get("text", ""),
            label_cn=claim.get("text_cn", ""),
            node_type="claim",
            sentiment=sentiment,
            topic=claim.get("topic", "vibe"),
            prior_belief=strength / 10.0,
            confidence=min(1.0, claim.get("source_count", 1) / 10.0),
            source_count=claim.get("source_count", 1),
            strength_raw=strength,
            color=_CLAIM_COLORS.get(sentiment, _CLAIM_COLORS["neutral"]),
        )

        # Edge: topic -> claim
        topic = claim.get("topic", "vibe")
        if topic in topics:
            dag.add_edge(
                topic,
                cid,
                strength=0.7,
                mechanism="belongs_to",
                causal_type="direct",
                evidence_score=0.8,
            )

        # --- Evidence nodes ---
        evidence_list = claim.get("evidence", [])
        for idx, quote in enumerate(evidence_list):
            if not quote or not isinstance(quote, str):
                continue
            eid = f"{cid}_ev{idx}"
            dag.add_node(
                eid,
                label=quote[:50] + ("..." if len(quote) > 50 else ""),
                label_full=quote,
                node_type="evidence",
                prior_belief=0.7,
                confidence=0.6,
                color=_EVIDENCE_COLOR,
            )
            dag.add_edge(
                eid,
                cid,
                strength=0.6,
                mechanism="supports",
                causal_type="direct",
                evidence_score=0.6,
            )

    # --- Contradiction edges (make them one-directional to keep DAG valid) ---
    # We pick a consistent direction (alphabetical) so the graph stays acyclic.
    for cont in contradictions:
        a = cont.get("claim_a", "")
        b = cont.get("claim_b", "")
        if a not in claim_ids or b not in claim_ids:
            continue
        # Use alphabetical order so only one direction exists
        src, tgt = (a, b) if a < b else (b, a)
        dag.add_edge(
            src,
            tgt,
            strength=-0.5,  # negative = contradiction
            mechanism="contradicts",
            causal_type="contradicts",
            evidence_score=0.5,
            severity=cont.get("severity", "medium"),
        )

    # --- Belief propagation ---
    propagator = NoisyORPropagator(leak_probability=0.02)
    dag = propagator.propagate(dag)

    return dag


def _cytoscape_with_claim_colors(dag: nx.DiGraph) -> dict[str, Any]:
    """Convert DAG to Cytoscape format with claims-specific color overrides."""
    cy = graph_to_cytoscape(dag)

    # Override colors from node attributes
    for cy_node in cy["elements"]["nodes"]:
        node_id = cy_node["data"]["id"]
        if node_id in dag.nodes:
            attrs = dag.nodes[node_id]
            if "color" in attrs:
                cy_node["data"]["color"] = attrs["color"]
            if "sentiment" in attrs:
                cy_node["data"]["sentiment"] = attrs["sentiment"]
            if "topic" in attrs:
                cy_node["data"]["topic"] = attrs["topic"]
            if "label_full" in attrs:
                cy_node["data"]["label_full"] = attrs["label_full"]
            if "label_cn" in attrs:
                cy_node["data"]["label_cn"] = attrs["label_cn"]
            if "source_count" in attrs:
                cy_node["data"]["source_count"] = attrs["source_count"]
            if "strength_raw" in attrs:
                cy_node["data"]["strength_raw"] = attrs["strength_raw"]

    # Mark contradiction edges
    for cy_edge in cy["elements"]["edges"]:
        edge_id = cy_edge["data"]["id"]
        src = cy_edge["data"]["source"]
        tgt = cy_edge["data"]["target"]
        if dag.has_edge(src, tgt):
            edge_attrs = dag.edges[src, tgt]
            if edge_attrs.get("causal_type") == "contradicts":
                cy_edge["data"]["line_style"] = "dashed"
                cy_edge["data"]["color"] = "#f59e0b"  # amber for contradictions

    return cy


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

# Concurrency guard — prevent duplicate generation for same school
_generating_locks: dict[str, asyncio.Event] = {}


async def generate_claims_graph_for_school(
    session: AsyncSession,
    llm: LLMClient,
    school: School,
) -> dict[str, Any]:
    """Load reviews, extract claims, build graph, persist, and return.

    Returns a dict with ``claims``, ``graph`` (Cytoscape format),
    and ``controversies``.
    """
    # Load reviews
    result = await session.execute(
        select(CommunityReview)
        .where(CommunityReview.school_id == school.id)
        .order_by(CommunityReview.post_score.desc())
        .limit(50)
    )
    reviews = list(result.scalars().all())
    if not reviews:
        raise ValueError(f"No community reviews found for {school.name}")

    # Extract claims
    extraction = await extract_claims(reviews, llm)
    claims = extraction["claims"]
    contradictions = extraction["contradictions"]

    # Build graph
    dag = build_argument_graph(claims, contradictions)
    cy_graph = _cytoscape_with_claim_colors(dag)

    now = datetime.now(timezone.utc)

    # Upsert: delete existing, then insert
    existing = await session.execute(
        select(SchoolClaims).where(SchoolClaims.school_id == school.id)
    )
    old = existing.scalars().first()
    if old:
        await session.delete(old)
        await session.flush()

    sc = SchoolClaims(
        school_id=school.id,
        claims_json=claims,
        graph_json=cy_graph,
        controversies_json=contradictions,
        generated_at=now,
        model_version="v1",
    )
    session.add(sc)
    await session.flush()

    logger.info(
        "Generated claims graph for %s: %d claims, %d controversies",
        school.name,
        len(claims),
        len(contradictions),
    )

    return {
        "school_id": str(school.id),
        "claims": claims,
        "graph": cy_graph,
        "controversies": contradictions,
        "generated_at": now.isoformat(),
        "model_version": "v1",
    }


async def get_or_generate_claims_graph(
    session: AsyncSession,
    llm: LLMClient,
    school: School,
    *,
    max_age_hours: int = 168,  # 7 days
) -> dict[str, Any] | None:
    """Return cached claims graph if fresh, otherwise generate on demand.

    Returns None only if no reviews exist for the school.
    """
    school_key = str(school.id)

    # Wait if another request is already generating for this school
    if school_key in _generating_locks:
        event = _generating_locks[school_key]
        await asyncio.wait_for(event.wait(), timeout=120)
        result = await session.execute(
            select(SchoolClaims).where(SchoolClaims.school_id == school.id)
        )
        sc = result.scalars().first()
        if sc:
            return {
                "school_id": str(sc.school_id),
                "claims": sc.claims_json,
                "graph": sc.graph_json,
                "controversies": sc.controversies_json,
                "generated_at": sc.generated_at.isoformat() if sc.generated_at else None,
                "model_version": sc.model_version,
            }
        return None

    # Check for existing fresh result
    result = await session.execute(
        select(SchoolClaims).where(SchoolClaims.school_id == school.id)
    )
    existing = result.scalars().first()
    if existing:
        age_hours = (
            datetime.now(timezone.utc) - existing.generated_at
        ).total_seconds() / 3600
        if age_hours < max_age_hours:
            return {
                "school_id": str(existing.school_id),
                "claims": existing.claims_json,
                "graph": existing.graph_json,
                "controversies": existing.controversies_json,
                "generated_at": existing.generated_at.isoformat(),
                "model_version": existing.model_version,
            }

    # Generate
    event = asyncio.Event()
    _generating_locks[school_key] = event

    try:
        # Check if reviews exist; if not, collect them first
        review_check = await session.execute(
            select(CommunityReview.id).where(
                CommunityReview.school_id == school.id,
            ).limit(1)
        )
        if not review_check.first():
            logger.info("No reviews for %s, triggering collection before claims graph", school.name)
            from scholarpath.services.community_review_service import collect_reviews_for_school
            import httpx
            async with httpx.AsyncClient() as http_client:
                await collect_reviews_for_school(session, school, http_client=http_client)

        payload = await generate_claims_graph_for_school(session, llm, school)
        await session.commit()
        return payload
    except Exception:
        logger.exception("Failed claims graph generation for %s", school.name)
        return None
    finally:
        event.set()
        _generating_locks.pop(school_key, None)
