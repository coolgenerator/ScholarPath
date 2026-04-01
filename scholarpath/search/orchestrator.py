"""Main DeepSearch pipeline orchestrator."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from scholarpath.llm import LLMClient, get_llm_client
from scholarpath.llm.embeddings import get_embedding_service
from scholarpath.search.conflict_detector import ConflictDetector, ConflictRecord
from scholarpath.search.decomposer import QueryDecomposer, SubQuery
from scholarpath.search.entity_aligner import AlignedEntity, EntityAligner
from scholarpath.search.refiner import SearchRefiner
from scholarpath.search.sources.base import BaseSource, SearchResult
from scholarpath.search.sources.college_scorecard import CollegeScorecardSource
from scholarpath.search.sources.niche import NicheSource
from scholarpath.search.sources.ugc import UGCSource
from scholarpath.search.sources.web_search import WebSearchSource

logger = logging.getLogger(__name__)


@dataclass
class DeepSearchResult:
    """Final output of the DeepSearch pipeline."""

    schools: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[ConflictRecord] = field(default_factory=list)
    coverage_score: float = 0.0
    search_metadata: dict[str, Any] = field(default_factory=dict)


# Type alias for progress callbacks.
ProgressCallback = Callable[[str, str, float], Any]  # (stage, message, progress_pct)


class DeepSearchOrchestrator:
    """Orchestrates the full Open DeepSearch pipeline.

    Pipeline stages:
    1. Decompose the research goal into sub-queries
    2. Fan out queries to data sources in parallel
    3. Align entities across sources
    4. Detect cross-source conflicts
    5. Refine iteratively to fill coverage gaps
    """

    def __init__(
        self,
        llm: LLMClient | None = None,
        sources: dict[str, BaseSource] | None = None,
        scorecard_api_key: str = "DEMO_KEY",
        search_api_url: str = "",
        search_api_key: str = "",
        on_progress: ProgressCallback | None = None,
    ) -> None:
        self._llm = llm or get_llm_client()
        self._on_progress = on_progress

        if sources is not None:
            self._sources = sources
        else:
            self._sources: dict[str, BaseSource] = {
                "college_scorecard": CollegeScorecardSource(api_key=scorecard_api_key),
                "niche": NicheSource(),
                "ugc": UGCSource(),
            }
            if search_api_url:
                self._sources["web_search"] = WebSearchSource(
                    search_api_url=search_api_url,
                    search_api_key=search_api_key,
                )

        self._decomposer = QueryDecomposer(self._llm)
        self._aligner = EntityAligner(self._llm)
        self._detector = ConflictDetector(self._llm)
        self._refiner = SearchRefiner(self._sources)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def search(
        self,
        student_profile: dict,
        target_schools: list[str] | None = None,
    ) -> DeepSearchResult:
        """Run the full DeepSearch pipeline and return consolidated results."""
        t0 = time.monotonic()
        metadata: dict[str, Any] = {
            "sources_used": list(self._sources.keys()),
            "target_schools": target_schools,
        }

        # --- Stage 1: Decompose ---
        await self._emit("decompose", "Decomposing research goal...", 0.05)
        research_goal = self._build_research_goal(student_profile, target_schools)
        sub_queries = await self._decomposer.decompose(student_profile, research_goal)
        metadata["sub_queries_count"] = len(sub_queries)
        logger.info("Decomposed into %d sub-queries", len(sub_queries))

        # --- Stage 2: Fan out to sources ---
        await self._emit("fetch", "Querying data sources...", 0.15)
        all_results = await self._fan_out(sub_queries, target_schools)
        metadata["raw_results_count"] = len(all_results)
        logger.info("Collected %d raw results", len(all_results))

        # --- Stage 3: Align entities ---
        await self._emit("align", "Aligning entities across sources...", 0.50)
        aligned = await self._aligner.align(all_results)
        metadata["entities_count"] = len(aligned)
        logger.info("Aligned into %d entities", len(aligned))

        # --- Stage 4: Detect conflicts ---
        await self._emit("conflicts", "Detecting data conflicts...", 0.65)
        conflicts = await self._detector.detect(aligned)
        metadata["conflicts_count"] = len(conflicts)
        logger.info("Detected %d conflicts", len(conflicts))

        # --- Stage 5: Refine ---
        await self._emit("refine", "Refining results for coverage...", 0.75)
        aligned = await self._refiner.refine(aligned, student_profile)

        # --- Build output ---
        # --- Stage 6: Embed results ---
        await self._emit("embed", "Generating embeddings...", 0.90)
        await self._embed_results(aligned)

        await self._emit("finalise", "Building final results...", 0.95)
        schools = self._build_school_dicts(aligned)
        coverage = self._compute_coverage(aligned)
        metadata["coverage_score"] = coverage

        elapsed = time.monotonic() - t0
        metadata["elapsed_seconds"] = round(elapsed, 2)
        logger.info("DeepSearch completed in %.2fs  coverage=%.1f%%", elapsed, coverage * 100)

        await self._emit("done", "Search complete.", 1.0)

        return DeepSearchResult(
            schools=schools,
            conflicts=conflicts,
            coverage_score=coverage,
            search_metadata=metadata,
        )

    # ------------------------------------------------------------------
    # Pipeline helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_research_goal(
        student_profile: dict,
        target_schools: list[str] | None,
    ) -> str:
        parts = ["Research college options"]
        if target_schools:
            parts.append(f"for schools: {', '.join(target_schools)}")
        gpa = student_profile.get("gpa")
        if gpa:
            parts.append(f"(student GPA {gpa})")
        major = student_profile.get("intended_major")
        if major:
            parts.append(f"interested in {major}")
        return " ".join(parts)

    async def _fan_out(
        self,
        sub_queries: list[SubQuery],
        target_schools: list[str] | None,
    ) -> list[SearchResult]:
        """Send sub-queries to their target sources in parallel."""
        tasks: list[asyncio.Task] = []

        schools = target_schools or [""]
        for sq in sub_queries:
            for source_name in sq.target_sources:
                source = self._sources.get(source_name)
                if source is None:
                    logger.debug("Source '%s' not available; skipping", source_name)
                    continue
                for school in schools:
                    query = school if school else sq.query
                    tasks.append(
                        asyncio.create_task(
                            self._safe_source_query(source, query, sq.expected_fields),
                        )
                    )

        if not tasks:
            return []

        gathered = await asyncio.gather(*tasks)
        all_results: list[SearchResult] = []
        for result_list in gathered:
            all_results.extend(result_list)
        return all_results

    @staticmethod
    async def _safe_source_query(
        source: BaseSource,
        school_name: str,
        fields: list[str] | None,
    ) -> list[SearchResult]:
        """Query a source, catching and logging any failure."""
        try:
            results = await source.search(school_name, fields=fields)
            # Tag results with the queried school name for alignment.
            for r in results:
                if r.raw_data is None:
                    r.raw_data = {}
                r.raw_data.setdefault("queried_school", school_name)
            return results
        except Exception:
            logger.exception(
                "Source '%s' failed for school '%s'; continuing with other sources",
                source.name,
                school_name,
            )
            return []

    @staticmethod
    async def _embed_results(aligned: list[AlignedEntity]) -> None:
        """Best-effort embed data points for semantic retrieval via pgvector."""
        try:
            emb = get_embedding_service()
            texts = []
            data_points = []
            for entity in aligned:
                for dp in entity.data_points:
                    text = f"{dp.variable_name}: {dp.value_text}"
                    if dp.source_name:
                        text += f" (source: {dp.source_name})"
                    texts.append(text)
                    data_points.append(dp)

            if not texts:
                return

            vectors = await emb.embed_batch(texts, task_type="RETRIEVAL_DOCUMENT")
            for dp, vec in zip(data_points, vectors):
                if dp.raw_data is None:
                    dp.raw_data = {}
                dp.raw_data["embedding"] = vec
        except Exception:
            logger.warning("Failed to embed search results", exc_info=True)

    @staticmethod
    def _build_school_dicts(aligned: list[AlignedEntity]) -> list[dict[str, Any]]:
        """Convert aligned entities to serialisable dicts."""
        schools: list[dict[str, Any]] = []
        for entity in aligned:
            data: dict[str, Any] = {}
            for dp in entity.data_points:
                existing = data.get(dp.variable_name)
                # Keep the value from the highest-confidence source.
                if existing is None or dp.confidence > existing.get("confidence", 0):
                    data[dp.variable_name] = {
                        "value": dp.value_numeric if dp.value_numeric is not None else dp.value_text,
                        "source": dp.source_name,
                        "confidence": dp.confidence,
                    }
            schools.append({
                "name": entity.canonical_name,
                "aliases": entity.aliases,
                "data": data,
                "sources_count": len({dp.source_name for dp in entity.data_points}),
            })
        return schools

    @staticmethod
    def _compute_coverage(aligned: list[AlignedEntity]) -> float:
        """Compute average critical-field coverage across all entities."""
        critical = {
            "acceptance_rate", "sat_math_mid", "sat_reading_mid",
            "tuition_out_of_state", "median_earnings_10yr",
            "graduation_rate", "overall_grade",
        }
        if not aligned:
            return 0.0
        coverages: list[float] = []
        for entity in aligned:
            present = {dp.variable_name for dp in entity.data_points}
            covered = len(present & critical)
            coverages.append(covered / len(critical))
        return sum(coverages) / len(coverages)

    # ------------------------------------------------------------------
    # Progress
    # ------------------------------------------------------------------

    async def _emit(self, stage: str, message: str, progress: float) -> None:
        """Emit a progress event if a callback is registered."""
        if self._on_progress is not None:
            try:
                result = self._on_progress(stage, message, progress)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.debug("Progress callback failed for stage '%s'", stage)
