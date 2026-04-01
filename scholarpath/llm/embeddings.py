"""Embedding service using Google Gemini Embedding API.

Uses ``google-genai`` SDK with ``gemini-embedding-001`` model to generate
3072-dimensional vectors for semantic similarity search via pgvector.
"""

from __future__ import annotations

import logging
from typing import Sequence

from google import genai
from google.genai import types
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from scholarpath.config import settings

logger = logging.getLogger(__name__)

# Retry policy for transient API errors.
_RETRY = retry(
    retry=retry_if_exception_type((ConnectionError, TimeoutError, Exception)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)


class EmbeddingService:
    """Generate embeddings via Google Gemini Embedding API.

    Supports multiple task types for different use cases:
    - SEMANTIC_SIMILARITY: comparing student profiles with school profiles
    - RETRIEVAL_DOCUMENT: indexing school/program descriptions
    - RETRIEVAL_QUERY: searching for schools by query
    - CLASSIFICATION: intent classification, tier categorization
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
    ) -> None:
        self._api_key = api_key or settings.GOOGLE_API_KEY
        self._model = model or settings.EMBEDDING_MODEL
        self._client = genai.Client(api_key=self._api_key)
        self._dimension = settings.EMBEDDING_DIMENSION

    @property
    def dimension(self) -> int:
        """Return the embedding vector dimension."""
        return self._dimension

    @_RETRY
    async def embed_text(
        self,
        text: str,
        *,
        task_type: str = "SEMANTIC_SIMILARITY",
    ) -> list[float]:
        """Embed a single text string.

        Args:
            text: The text to embed.
            task_type: One of SEMANTIC_SIMILARITY, RETRIEVAL_DOCUMENT,
                       RETRIEVAL_QUERY, CLASSIFICATION, CLUSTERING.

        Returns:
            A list of floats (the embedding vector).
        """
        import time as _time
        t0 = _time.monotonic()
        error_msg = None
        try:
            result = self._client.models.embed_content(
                model=self._model,
                contents=[text],
                config=types.EmbedContentConfig(task_type=task_type),
            )
            vector = result.embeddings[0].values
            logger.debug(
                "Embedded text  model=%s  task=%s  dim=%d  chars=%d",
                self._model, task_type, len(vector), len(text),
            )
            return list(vector)
        except Exception as exc:
            error_msg = str(exc)
            raise
        finally:
            latency_ms = int((_time.monotonic() - t0) * 1000)
            # Estimate tokens: ~1 token per 4 chars for embedding input
            est_tokens = max(len(text) // 4, 1)
            try:
                from scholarpath.llm.usage_tracker import record_usage
                await record_usage(
                    model=self._model,
                    provider="gemini",
                    caller="embedding.embed_text",
                    method="embed",
                    prompt_tokens=est_tokens,
                    completion_tokens=0,
                    total_tokens=est_tokens,
                    error=error_msg,
                    latency_ms=latency_ms,
                )
            except Exception:
                pass  # tracking is best-effort

    @_RETRY
    async def embed_batch(
        self,
        texts: Sequence[str],
        *,
        task_type: str = "SEMANTIC_SIMILARITY",
    ) -> list[list[float]]:
        """Embed multiple texts in a single API call.

        Args:
            texts: List of texts to embed.
            task_type: Embedding task type.

        Returns:
            A list of embedding vectors, one per input text.
        """
        if not texts:
            return []

        import time as _time
        t0 = _time.monotonic()
        error_msg = None
        try:
            result = self._client.models.embed_content(
                model=self._model,
                contents=list(texts),
                config=types.EmbedContentConfig(task_type=task_type),
            )
            vectors = [list(e.values) for e in result.embeddings]
            logger.debug(
                "Batch embedded  model=%s  task=%s  count=%d",
                self._model, task_type, len(vectors),
            )
            return vectors
        except Exception as exc:
            error_msg = str(exc)
            raise
        finally:
            latency_ms = int((_time.monotonic() - t0) * 1000)
            est_tokens = sum(max(len(t) // 4, 1) for t in texts)
            try:
                from scholarpath.llm.usage_tracker import record_usage
                await record_usage(
                    model=self._model,
                    provider="gemini",
                    caller="embedding.embed_batch",
                    method="embed",
                    prompt_tokens=est_tokens,
                    completion_tokens=0,
                    total_tokens=est_tokens,
                    error=error_msg,
                    latency_ms=latency_ms,
                )
            except Exception:
                pass

    async def embed_student_profile(self, profile: dict) -> list[float]:
        """Generate an embedding for a student profile.

        Constructs a descriptive text from structured profile data,
        then embeds it for similarity matching with school profiles.
        """
        parts = []
        if profile.get("intended_majors"):
            parts.append(f"Intended majors: {', '.join(profile['intended_majors'])}")
        if profile.get("gpa"):
            parts.append(f"GPA: {profile['gpa']}/{profile.get('gpa_scale', '4.0')}")
        if profile.get("sat_total"):
            parts.append(f"SAT: {profile['sat_total']}")
        if profile.get("extracurriculars"):
            extras = profile["extracurriculars"]
            if isinstance(extras, list):
                names = [e.get("name", str(e)) if isinstance(e, dict) else str(e) for e in extras[:5]]
                parts.append(f"Activities: {', '.join(names)}")
        if profile.get("awards"):
            awards = profile["awards"]
            if isinstance(awards, list):
                names = [a.get("name", str(a)) if isinstance(a, dict) else str(a) for a in awards[:5]]
                parts.append(f"Awards: {', '.join(names)}")
        if profile.get("preferences"):
            prefs = profile["preferences"]
            if isinstance(prefs, dict):
                pref_parts = []
                if prefs.get("location"):
                    pref_parts.append(f"location: {prefs['location']}")
                if prefs.get("size"):
                    pref_parts.append(f"size: {prefs['size']}")
                if prefs.get("culture"):
                    pref_parts.append(f"culture: {prefs['culture']}")
                if pref_parts:
                    parts.append(f"Preferences: {', '.join(pref_parts)}")
        if profile.get("budget_usd"):
            parts.append(f"Budget: ${profile['budget_usd']:,}")

        text = ". ".join(parts) if parts else "Student profile"
        return await self.embed_text(text, task_type="SEMANTIC_SIMILARITY")

    async def embed_school_profile(self, school: dict) -> list[float]:
        """Generate an embedding for a school profile.

        Constructs a descriptive text from school data for matching
        against student profiles.
        """
        parts = [school.get("name", "Unknown school")]
        if school.get("name_cn"):
            parts.append(f"({school['name_cn']})")
        if school.get("city") and school.get("state"):
            parts.append(f"Location: {school['city']}, {school['state']}")
        if school.get("school_type"):
            parts.append(f"Type: {school['school_type']}")
        if school.get("us_news_rank"):
            parts.append(f"US News Rank: #{school['us_news_rank']}")
        if school.get("acceptance_rate"):
            parts.append(f"Acceptance rate: {school['acceptance_rate']:.1%}")
        if school.get("programs"):
            program_names = [p["name"] if isinstance(p, dict) else str(p) for p in school["programs"][:5]]
            parts.append(f"Programs: {', '.join(program_names)}")
        if school.get("campus_setting"):
            parts.append(f"Setting: {school['campus_setting']}")

        text = ". ".join(parts)
        return await self.embed_text(text, task_type="RETRIEVAL_DOCUMENT")

    async def embed_query(self, query: str) -> list[float]:
        """Embed a search query for retrieval against indexed documents."""
        return await self.embed_text(query, task_type="RETRIEVAL_QUERY")

    async def embed_data_point(self, data_point: dict) -> list[float]:
        """Embed a data point for semantic deduplication and retrieval."""
        parts = []
        if data_point.get("variable_name"):
            parts.append(data_point["variable_name"])
        if data_point.get("value_text"):
            parts.append(data_point["value_text"])
        if data_point.get("source_name"):
            parts.append(f"Source: {data_point['source_name']}")

        text = ". ".join(parts) if parts else "data point"
        return await self.embed_text(text, task_type="RETRIEVAL_DOCUMENT")


# ------------------------------------------------------------------
# Singleton
# ------------------------------------------------------------------

_singleton: EmbeddingService | None = None


def get_embedding_service() -> EmbeddingService:
    """Return a module-level singleton :class:`EmbeddingService`."""
    global _singleton  # noqa: PLW0603
    if _singleton is None:
        _singleton = EmbeddingService()
    return _singleton
