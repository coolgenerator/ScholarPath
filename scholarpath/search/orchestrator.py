"""Main DeepSearch pipeline orchestrator (V2)."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Callable, Iterable

from sqlalchemy import func, select

from scholarpath.llm import LLMClient, get_llm_client
from scholarpath.llm.embeddings import get_embedding_service
from scholarpath.search.canonical_merge import (
    PRD_EXPANDED_CRITICAL_FIELDS,
    CanonicalMergeService,
    coerce_numeric,
    fingerprint_value,
    normalise_numeric,
    normalise_variable_name,
)
from scholarpath.search.conflict_detector import ConflictDetector, ConflictRecord
from scholarpath.search.db_coverage import DBCoverageLoader
from scholarpath.search.entity_aligner import AlignedEntity, EntityAligner
from scholarpath.search.field_planner import FieldCoveragePlanner, SourcePlan
from scholarpath.search.source_value import SourceValueInput, SourceValueScorer
from scholarpath.search.sources.base import BaseSource, SearchResult
from scholarpath.search.sources.college_scorecard import CollegeScorecardSource
from scholarpath.search.sources.cds_parser import CommonDataSetSource
from scholarpath.search.sources.ipeds_college_navigator import IPEDSCollegeNavigatorSource
from scholarpath.search.sources.internal_web_search import InternalWebSearchSource
from scholarpath.search.sources.niche import NicheSource
from scholarpath.search.sources.school_official_profile import SchoolOfficialProfileSource
from scholarpath.search.sources.ugc import UGCSource
from scholarpath.search.sources.web_search import WebSearchSource

logger = logging.getLogger(__name__)

# Type alias for progress callbacks.
ProgressCallback = Callable[[str, str, float], Any]  # (stage, message, progress_pct)

_TOKEN_ESTIMATE_SELF_SOURCE = 2500
_TOKEN_ESTIMATE_INTERNAL_WEB = 10700
_TOKEN_ESTIMATE_ALIGN = 270
_TOKEN_ESTIMATE_CONFLICT = 300

_SOURCE_POLICY_MIN_CALLS = 3
_SOURCE_POLICY_FUSE_SCORE = 0.20
_SOURCE_POLICY_FUSE_FAILURE_RATE = 0.60
_SOURCE_POLICY_DOWNWEIGHT_SCORE = 0.35
_SOURCE_POLICY_RECOVER_SCORE = 0.55
_SOURCE_POLICY_RECOVER_FAILURE_RATE = 0.25
_SOURCE_POLICY_FUSE_SECONDS = 30 * 60

_SOURCE_POLICY_CACHE: dict[str, dict[str, Any]] = {}
_SOURCE_POLICY_LOCK = Lock()


@dataclass
class DeepSearchResult:
    """Final output of the DeepSearch pipeline."""

    schools: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[ConflictRecord] = field(default_factory=list)
    coverage_score: float = 0.0
    search_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class _SourceRuntimeStats:
    calls: int = 0
    failures: int = 0
    latency_ms_total: float = 0.0


class DeepSearchOrchestrator:
    """Orchestrates DB-first DeepSearch with cheap-source fan-out and web fallback."""

    def __init__(
        self,
        llm: LLMClient | None = None,
        sources: dict[str, BaseSource] | None = None,
        scorecard_api_key: str | None = None,
        search_api_url: str = "",
        search_api_key: str = "",
        school_profile_search_api_url: str = "",
        school_profile_search_api_key: str = "",
        on_progress: ProgressCallback | None = None,
        school_concurrency: int = 4,
        source_http_concurrency: int = 8,
        self_extract_concurrency: int = 4,
        internal_websearch_concurrency: int = 2,
    ) -> None:
        self._llm = llm or get_llm_client()
        self._on_progress = on_progress

        if sources is not None:
            self._sources = sources
        else:
            scorecard_key = (scorecard_api_key or "").strip()
            if not scorecard_key:
                raise ValueError(
                    "SCORECARD_API_KEY is required when DeepSearch sources are auto-configured",
                )
            self._sources: dict[str, BaseSource] = {
                "college_scorecard": CollegeScorecardSource(api_key=scorecard_key),
                "niche": NicheSource(),
                "ugc": UGCSource(),
            }
            ipeds_url = ""
            ipeds_path = ""
            try:
                from scholarpath.config import settings

                ipeds_url = (settings.IPEDS_DATASET_URL or "").strip()
                ipeds_path = (settings.IPEDS_DATASET_PATH or "").strip()
            except Exception:
                ipeds_url = ""
                ipeds_path = ""
            if ipeds_url or ipeds_path:
                self._sources["ipeds_college_navigator"] = IPEDSCollegeNavigatorSource(
                    dataset_url=ipeds_url,
                    dataset_path=ipeds_path,
                )
            profile_url = school_profile_search_api_url or search_api_url
            profile_key = school_profile_search_api_key or search_api_key
            if profile_url:
                self._sources["school_official_profile"] = SchoolOfficialProfileSource(
                    search_api_url=profile_url,
                    search_api_key=profile_key,
                )
                self._sources["cds_parser"] = CommonDataSetSource(
                    search_api_url=profile_url,
                    search_api_key=profile_key,
                )
            if search_api_url:
                self._sources["web_search"] = WebSearchSource(
                    search_api_url=search_api_url,
                    search_api_key=search_api_key,
                )

        self._internal_web_source = InternalWebSearchSource(self._llm)
        self._aligner = EntityAligner(self._llm)
        self._detector = ConflictDetector(self._llm)

        self._school_concurrency = max(1, school_concurrency)
        self._source_http_concurrency = max(1, source_http_concurrency)
        self._self_extract_concurrency = max(1, self_extract_concurrency)
        self._internal_websearch_concurrency = max(1, internal_websearch_concurrency)

        self._source_http_semaphore = asyncio.Semaphore(self._source_http_concurrency)
        self._self_extract_semaphore = asyncio.Semaphore(self._self_extract_concurrency)
        self._internal_web_runtime_semaphore = asyncio.Semaphore(
            self._internal_websearch_concurrency,
        )

        self._merger = CanonicalMergeService()
        self._db_loader = DBCoverageLoader(self._merger)
        self._planner = FieldCoveragePlanner()
        self._inflight_queries: dict[tuple[str, str, tuple[str, ...], str], asyncio.Task] = {}
        self._source_runtime_stats: dict[str, _SourceRuntimeStats] = {}
        self._source_runtime_lock = asyncio.Lock()
        self._source_value_scorer = SourceValueScorer()
        self._last_source_scores: dict[str, float] = {}
        self._source_policy_state: dict[str, dict[str, Any]] = {}

    async def search(
        self,
        student_profile: dict,
        target_schools: list[str] | None = None,
        required_fields: list[str] | None = None,
        freshness_days: int = 90,
        max_internal_websearch_calls_per_school: int = 1,
        budget_mode: str = "balanced",
        eval_run_id: str | None = None,
    ) -> DeepSearchResult:
        """Run DeepSearch V2 and return consolidated results."""
        t0 = time.monotonic()
        schools = [school.strip() for school in (target_schools or []) if school and school.strip()]
        canonical_required = self._resolve_required_fields(required_fields)
        freshness_days = max(freshness_days, 0)
        max_internal_calls = max(max_internal_websearch_calls_per_school, 0)

        metadata: dict[str, Any] = {
            "sources_used": sorted(list(self._sources.keys()) + ["internal_web_search"]),
            "target_schools": schools,
            "required_fields": canonical_required,
            "freshness_days": freshness_days,
            "budget_mode": budget_mode,
            "eval_run_id": eval_run_id,
            "concurrency": {
                "school_concurrency": self._school_concurrency,
                "source_http_concurrency": self._source_http_concurrency,
                "self_extract_concurrency": self._self_extract_concurrency,
                "internal_websearch_concurrency": self._internal_websearch_concurrency,
            },
        }

        if not schools:
            metadata["coverage_score"] = 0.0
            metadata["elapsed_seconds"] = round(time.monotonic() - t0, 2)
            return DeepSearchResult(
                schools=[],
                conflicts=[],
                coverage_score=0.0,
                search_metadata=metadata,
            )

        self._inflight_queries = {}
        self._source_runtime_stats = {}
        self._source_policy_state = self._load_source_policy_state()
        effective_sources = {
            name: source
            for name, source in self._sources.items()
            if self._is_source_enabled(name)
        }
        metadata["source_policy_applied"] = self._policy_snapshot()
        metadata["effective_sources"] = sorted(list(effective_sources.keys()) + ["internal_web_search"])

        await self._emit("db_lookup", "Loading recent facts from database...", 0.05)
        db_snapshots = await self._db_loader.load(
            target_schools=schools,
            required_fields=canonical_required,
            freshness_days=freshness_days,
        )
        db_results = [
            result
            for snapshot in db_snapshots.values()
            for result in snapshot.existing_results
        ]
        db_coverage = self._merger.coverage_by_school(db_results, canonical_required)
        db_stats = self._merger.coverage_stats(
            school_names=schools,
            coverage_by_school=db_coverage,
            required_fields=canonical_required,
        )
        metadata["db_hit_ratio"] = round(db_stats.ratio, 4)
        metadata["db_covered_slots"] = db_stats.covered_slots
        metadata["db_required_slots"] = db_stats.required_slots
        metadata["db_results_count"] = len(db_results)

        await self._emit("wave_b_plan", "Planning low-cost source queries...", 0.12)
        wave_b_plans = self._planner.plan_wave_b(
            coverage=db_snapshots,
            required_fields=canonical_required,
            available_sources=set(effective_sources.keys()),
            source_priority=self._compose_source_priority(effective_sources.keys()),
        )
        wave_b_results, self_source_calls = await self._execute_wave(
            wave_b_plans,
            freshness_days=freshness_days,
            internal_call_cap=None,
        )
        metadata["self_source_calls"] = self_source_calls

        merged_results = self._merger.merge(db_results + wave_b_results)
        missing_after_wave_b = self._missing_by_school(
            school_names=schools,
            results=merged_results,
            required_fields=canonical_required,
        )
        critical_fields = {
            normalise_variable_name(field) for field in PRD_EXPANDED_CRITICAL_FIELDS
        }
        missing_critical_by_school: dict[str, list[str]] = {}
        for school in schools:
            critical_missing = [
                field
                for field in missing_after_wave_b.get(school, [])
                if normalise_variable_name(field) in critical_fields
            ]
            if critical_missing:
                missing_critical_by_school[school] = sorted(set(critical_missing))

        await self._emit("wave_c_fallback", "Applying internal web fallback...", 0.30)
        wave_c_results: list[SearchResult] = []
        internal_websearch_calls = 0
        fallback_candidates = [
            school for school in schools if school in missing_critical_by_school
        ]
        fallback_trigger_rate = (
            len(fallback_candidates) / len(schools) if schools else 0.0
        )

        metadata["fallback_trigger_rate"] = round(fallback_trigger_rate, 4)
        metadata["fallback_critical_missing_slots"] = sum(
            len(fields) for fields in missing_critical_by_school.values()
        )
        metadata["internal_concurrency_degraded"] = False
        metadata["internal_websearch_enabled"] = self._is_source_enabled("internal_web_search")
        if (
            fallback_candidates
            and max_internal_calls > 0
            and self._is_source_enabled("internal_web_search")
        ):
            effective_internal_concurrency = self._internal_websearch_concurrency
            if fallback_trigger_rate > 0.4:
                effective_internal_concurrency = 1
                metadata["internal_concurrency_degraded"] = True

            self._internal_web_runtime_semaphore = asyncio.Semaphore(
                max(1, effective_internal_concurrency),
            )
            internal_plans: dict[str, list[SourcePlan]] = {}
            for school in fallback_candidates:
                internal_plans[school] = [
                    SourcePlan(
                        school_name=school,
                        source_name="internal_web_search",
                        fields=missing_critical_by_school[school],
                    ),
                ]

            wave_c_results, internal_websearch_calls = await self._execute_wave(
                internal_plans,
                freshness_days=freshness_days,
                internal_call_cap=max_internal_calls,
            )
        elif fallback_candidates and not self._is_source_enabled("internal_web_search"):
            metadata["internal_websearch_skipped_reason"] = "source_policy_fused"

        metadata["internal_websearch_calls"] = internal_websearch_calls

        raw_fact_count = len(db_results) + len(wave_b_results) + len(wave_c_results)
        all_results = self._merger.merge(db_results + wave_b_results + wave_c_results)
        unique_fact_count = len(all_results)
        metadata["raw_fact_count_before_merge"] = raw_fact_count
        metadata["unique_fact_count_after_merge"] = unique_fact_count
        metadata["dedupe_drop_count"] = max(0, raw_fact_count - unique_fact_count)
        metadata["raw_results_count"] = unique_fact_count
        metadata["multi_source_agreement_count"] = self._count_multi_source_agreements(all_results)
        metadata["critical_coverage_by_school"] = self._critical_coverage_by_school(
            school_names=schools,
            results=all_results,
            required_fields=canonical_required,
        )

        await self._emit("align", "Aligning school entities...", 0.45)
        aligned = await self._aligner.align(all_results)
        metadata["entities_count"] = len(aligned)

        await self._emit("conflicts", "Detecting cross-source conflicts...", 0.60)
        conflicts = await self._detector.detect(aligned)
        metadata["conflicts_count"] = len(conflicts)
        metadata["multi_source_conflict_count"] = len(conflicts)
        source_metrics, source_scores = await self._compute_source_value_metrics(
            raw_results=wave_b_results + wave_c_results,
            merged_results=all_results,
            conflicts=conflicts,
            required_fields=canonical_required,
        )
        metadata["source_runtime_metrics"] = source_metrics
        metadata["source_value_scores"] = source_scores
        metadata["source_priority_next_run"] = SourceValueScorer.rank(source_scores)
        metadata["source_value_formula"] = (
            "0.34*coverage + 0.22*keep_ratio + 0.18*consistency + "
            "0.12*success_rate + 0.08*token_eff + 0.06*latency_eff"
        )
        self._last_source_scores = source_scores
        metadata["source_policy_update"] = self._update_source_policy(
            source_scores=source_scores,
            source_metrics=source_metrics,
        )

        if budget_mode != "low_cost":
            await self._emit("embed", "Generating embeddings...", 0.73)
            await self._embed_results(aligned)

        await self._emit("persist", "Persisting deduplicated facts...", 0.85)
        persistence_stats = await self._persist_results(
            aligned=aligned,
            conflicts=conflicts,
            freshness_days=freshness_days,
        )
        metadata.update(persistence_stats)

        coverage = self._compute_coverage(
            aligned=aligned,
            required_fields=canonical_required,
        )
        metadata["coverage_score"] = round(coverage, 4)
        metadata["tokens_by_stage"] = self._estimate_tokens(
            self_source_calls=self_source_calls,
            internal_websearch_calls=internal_websearch_calls,
            conflict_count=len(conflicts),
        )

        schools_payload = self._build_school_dicts(aligned)
        elapsed = time.monotonic() - t0
        metadata["elapsed_seconds"] = round(elapsed, 2)
        logger.info(
            "DeepSearch V2 completed in %.2fs coverage=%.1f%% schools=%d",
            elapsed,
            coverage * 100,
            len(schools_payload),
        )

        await self._emit("done", "Search complete.", 1.0)
        return DeepSearchResult(
            schools=schools_payload,
            conflicts=conflicts,
            coverage_score=coverage,
            search_metadata=metadata,
        )

    async def _execute_wave(
        self,
        plans_by_school: dict[str, list[SourcePlan]],
        *,
        freshness_days: int,
        internal_call_cap: int | None,
    ) -> tuple[list[SearchResult], int]:
        school_semaphore = asyncio.Semaphore(self._school_concurrency)
        counter_lock = asyncio.Lock()
        all_results: list[SearchResult] = []
        calls_made = 0

        async def run_school(school: str, plans: list[SourcePlan]) -> None:
            nonlocal calls_made
            async with school_semaphore:
                if not plans:
                    return
                local_tasks: list[asyncio.Task] = []
                internal_calls = 0
                for plan in plans:
                    if (
                        internal_call_cap is not None
                        and plan.source_name == "internal_web_search"
                    ):
                        if internal_calls >= internal_call_cap:
                            continue
                        internal_calls += 1
                    local_tasks.append(
                        asyncio.create_task(
                            self._execute_plan(plan, freshness_days=freshness_days),
                        )
                    )
                if not local_tasks:
                    return
                gathered = await asyncio.gather(*local_tasks)
                school_results: list[SearchResult] = []
                school_calls = 0
                for rows, did_call in gathered:
                    school_results.extend(rows)
                    if did_call:
                        school_calls += 1
                async with counter_lock:
                    all_results.extend(school_results)
                    calls_made += school_calls

        await asyncio.gather(
            *[
                asyncio.create_task(run_school(school, plans))
                for school, plans in plans_by_school.items()
            ]
        )
        return all_results, calls_made

    async def _execute_plan(
        self,
        plan: SourcePlan,
        *,
        freshness_days: int,
    ) -> tuple[list[SearchResult], bool]:
        field_bucket = tuple(sorted({normalise_variable_name(field) for field in plan.fields}))
        ttl_bucket = f"{max(freshness_days, 0)}d"
        inflight_key = (
            plan.school_name.lower(),
            plan.source_name,
            field_bucket,
            ttl_bucket,
        )
        existing_task = self._inflight_queries.get(inflight_key)
        if existing_task is not None:
            return await existing_task, False

        task = asyncio.create_task(self._query_source(plan))
        self._inflight_queries[inflight_key] = task
        rows = await task
        return rows, True

    async def _query_source(self, plan: SourcePlan) -> list[SearchResult]:
        source = self._source_for_plan(plan.source_name)
        if source is None:
            return []
        started_at = time.monotonic()
        failed = False
        try:
            async with self._source_http_semaphore:
                if plan.source_name == "web_search":
                    async with self._self_extract_semaphore:
                        results = await source.search(plan.school_name, fields=plan.fields)
                elif plan.source_name == "internal_web_search":
                    async with self._internal_web_runtime_semaphore:
                        results = await source.search(plan.school_name, fields=plan.fields)
                else:
                    results = await source.search(plan.school_name, fields=plan.fields)
        except Exception:
            failed = True
            logger.exception(
                "Source query failed source=%s school=%s",
                plan.source_name,
                plan.school_name,
            )
            return []
        finally:
            latency_ms = max(0.0, (time.monotonic() - started_at) * 1000)
            await self._record_source_runtime(
                source_name=plan.source_name,
                failed=failed,
                latency_ms=latency_ms,
            )

        for row in results:
            if row.raw_data is None:
                row.raw_data = {}
            row.raw_data.setdefault("queried_school", plan.school_name)
        return results

    def _source_for_plan(self, source_name: str) -> BaseSource | None:
        if not self._is_source_enabled(source_name):
            return None
        if source_name == "internal_web_search":
            return self._internal_web_source
        return self._sources.get(source_name)

    def _load_source_policy_state(self) -> dict[str, dict[str, Any]]:
        now = time.time()
        with _SOURCE_POLICY_LOCK:
            snapshot = {
                source: dict(payload)
                for source, payload in _SOURCE_POLICY_CACHE.items()
            }

        for source in list(snapshot.keys()):
            disabled_until = snapshot[source].get("disabled_until")
            if isinstance(disabled_until, (int, float)) and disabled_until <= now:
                snapshot[source]["disabled_until"] = None
                if snapshot[source].get("status") == "fused":
                    snapshot[source]["status"] = "normal"
                    snapshot[source]["reason"] = "fuse_expired"
        return snapshot

    def _policy_snapshot(self) -> dict[str, dict[str, Any]]:
        now = time.time()
        relevant_sources = set(self._sources.keys()) | {"internal_web_search"}
        snapshot: dict[str, dict[str, Any]] = {}
        for source in sorted(relevant_sources):
            payload = dict(self._source_policy_state.get(source, {}))
            disabled_until = payload.get("disabled_until")
            remaining = 0
            if isinstance(disabled_until, (int, float)):
                remaining = max(0, int(disabled_until - now))
            snapshot[source] = {
                "status": payload.get("status", "normal"),
                "reason": payload.get("reason"),
                "priority_multiplier": float(payload.get("priority_multiplier", 1.0)),
                "last_score": payload.get("last_score"),
                "fuse_remaining_seconds": remaining,
            }
        return snapshot

    def _is_source_enabled(self, source_name: str) -> bool:
        payload = self._source_policy_state.get(source_name, {})
        disabled_until = payload.get("disabled_until")
        if not isinstance(disabled_until, (int, float)):
            return True
        return disabled_until <= time.time()

    def _compose_source_priority(self, source_names: Iterable[str]) -> dict[str, float]:
        priority: dict[str, float] = {}
        for source in source_names:
            policy = self._source_policy_state.get(source, {})
            last_score = policy.get("last_score")
            if isinstance(last_score, (int, float)):
                base = float(last_score)
            else:
                base = self._last_source_scores.get(source, 0.5)
            multiplier = float(policy.get("priority_multiplier", 1.0))
            priority[source] = round(max(0.0, base * multiplier), 4)
        return priority

    async def _record_source_runtime(
        self,
        *,
        source_name: str,
        failed: bool,
        latency_ms: float,
    ) -> None:
        async with self._source_runtime_lock:
            stats = self._source_runtime_stats.get(source_name)
            if stats is None:
                stats = _SourceRuntimeStats()
                self._source_runtime_stats[source_name] = stats
            stats.calls += 1
            if failed:
                stats.failures += 1
            stats.latency_ms_total += max(0.0, latency_ms)

    async def _compute_source_value_metrics(
        self,
        *,
        raw_results: list[SearchResult],
        merged_results: list[SearchResult],
        conflicts: list[ConflictRecord],
        required_fields: list[str],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, float]]:
        async with self._source_runtime_lock:
            runtime_snapshot = {
                source: _SourceRuntimeStats(
                    calls=stats.calls,
                    failures=stats.failures,
                    latency_ms_total=stats.latency_ms_total,
                )
                for source, stats in self._source_runtime_stats.items()
            }

        raw_by_source: dict[str, int] = defaultdict(int)
        kept_by_source: dict[str, int] = defaultdict(int)
        fields_by_source: dict[str, set[str]] = defaultdict(set)
        conflict_by_source: dict[str, int] = defaultdict(int)

        for item in raw_results:
            if item.raw_data and item.raw_data.get("from_db"):
                continue
            raw_by_source[item.source_name] += 1

        for item in merged_results:
            if item.raw_data and item.raw_data.get("from_db"):
                continue
            kept_by_source[item.source_name] += 1
            fields_by_source[item.source_name].add(
                normalise_variable_name(item.variable_name),
            )

        for conflict in conflicts:
            for source in set(conflict.sources):
                conflict_by_source[source] += 1

        sources = set(runtime_snapshot.keys())
        sources.update(raw_by_source.keys())
        sources.update(kept_by_source.keys())
        if not sources:
            return {}, {}

        metrics: dict[str, dict[str, Any]] = {}
        scores: dict[str, float] = {}
        required_field_count = len({normalise_variable_name(field) for field in required_fields})

        for source in sorted(sources):
            runtime = runtime_snapshot.get(source, _SourceRuntimeStats())
            calls = runtime.calls
            failures = runtime.failures
            raw_facts = raw_by_source.get(source, 0)
            kept_facts = kept_by_source.get(source, 0)
            unique_fields = len(fields_by_source.get(source, set()))
            conflicting = conflict_by_source.get(source, 0)
            estimated_tokens = self._estimate_tokens_for_source(source, calls)
            avg_latency_ms = (
                runtime.latency_ms_total / calls if calls > 0 else 0.0
            )
            payload = SourceValueInput(
                calls=calls,
                failures=failures,
                raw_facts=raw_facts,
                kept_facts=kept_facts,
                unique_fields=unique_fields,
                conflicting_facts=conflicting,
                estimated_tokens=estimated_tokens,
                avg_latency_ms=avg_latency_ms,
            )
            score = self._source_value_scorer.score(
                payload=payload,
                required_field_count=max(required_field_count, 1),
            )
            scores[source] = score

            success_rate = (calls - failures) / calls if calls > 0 else 1.0
            metrics[source] = {
                "calls": calls,
                "failures": failures,
                "success_rate": round(max(0.0, min(1.0, success_rate)), 4),
                "raw_facts": raw_facts,
                "kept_facts": kept_facts,
                "dedup_dropped": max(0, raw_facts - kept_facts),
                "unique_fields": unique_fields,
                "conflicting_facts": conflicting,
                "estimated_tokens": estimated_tokens,
                "avg_latency_ms": round(avg_latency_ms, 2),
            }
        return metrics, scores

    def _update_source_policy(
        self,
        *,
        source_scores: dict[str, float],
        source_metrics: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        now = time.time()
        policy = {
            source: dict(payload)
            for source, payload in self._source_policy_state.items()
        }

        changes: dict[str, dict[str, Any]] = {}
        all_sources = set(policy.keys()) | set(source_scores.keys()) | set(source_metrics.keys())

        for source in all_sources:
            current = dict(policy.get(source, {}))
            old_status = str(current.get("status", "normal"))
            metrics = source_metrics.get(source, {})
            calls = int(metrics.get("calls", 0) or 0)
            failures = int(metrics.get("failures", 0) or 0)
            failure_rate = failures / calls if calls > 0 else 0.0
            score = float(source_scores.get(source, current.get("last_score", 0.5) or 0.5))

            status = old_status
            reason = current.get("reason")
            disabled_until = current.get("disabled_until")
            multiplier = float(current.get("priority_multiplier", 1.0))

            if calls >= _SOURCE_POLICY_MIN_CALLS:
                if score <= _SOURCE_POLICY_FUSE_SCORE or failure_rate >= _SOURCE_POLICY_FUSE_FAILURE_RATE:
                    status = "fused"
                    reason = "high_failure_or_low_score"
                    disabled_until = now + _SOURCE_POLICY_FUSE_SECONDS
                    multiplier = 0.25
                elif score <= _SOURCE_POLICY_DOWNWEIGHT_SCORE:
                    status = "downweighted"
                    reason = "low_source_value"
                    disabled_until = None
                    multiplier = 0.60
                elif score >= _SOURCE_POLICY_RECOVER_SCORE and failure_rate <= _SOURCE_POLICY_RECOVER_FAILURE_RATE:
                    status = "normal"
                    reason = "recovered"
                    disabled_until = None
                    multiplier = 1.0
                elif isinstance(disabled_until, (int, float)) and disabled_until <= now:
                    status = "normal"
                    reason = "fuse_expired"
                    disabled_until = None
                    multiplier = max(multiplier, 0.8)

            current.update(
                {
                    "status": status,
                    "reason": reason,
                    "disabled_until": disabled_until,
                    "priority_multiplier": round(max(0.1, min(1.5, multiplier)), 4),
                    "last_score": round(max(0.0, min(1.0, score)), 4),
                    "last_updated": now,
                }
            )
            policy[source] = current

            if old_status != status:
                changes[source] = {
                    "from": old_status,
                    "to": status,
                    "reason": reason,
                    "score": round(score, 4),
                    "failure_rate": round(failure_rate, 4),
                }

        # Safety guard: avoid disabling all non-internal sources.
        core_sources = set(self._sources.keys())
        enabled_core = [
            source
            for source in core_sources
            if not isinstance(policy.get(source, {}).get("disabled_until"), (int, float))
            or policy[source]["disabled_until"] <= now
        ]
        if core_sources and not enabled_core:
            best_source = max(
                core_sources,
                key=lambda source: source_scores.get(
                    source,
                    float(policy.get(source, {}).get("last_score", 0.0) or 0.0),
                ),
            )
            item = dict(policy.get(best_source, {}))
            item.update(
                {
                    "status": "downweighted",
                    "reason": "safety_guard_keep_one_source",
                    "disabled_until": None,
                    "priority_multiplier": max(0.6, float(item.get("priority_multiplier", 0.6))),
                    "last_updated": now,
                }
            )
            policy[best_source] = item
            changes[best_source] = {
                "from": "fused",
                "to": "downweighted",
                "reason": "safety_guard_keep_one_source",
                "score": item.get("last_score"),
                "failure_rate": source_metrics.get(best_source, {}).get("failure_rate", 0.0),
            }

        with _SOURCE_POLICY_LOCK:
            _SOURCE_POLICY_CACHE.clear()
            _SOURCE_POLICY_CACHE.update(policy)
        self._source_policy_state = policy

        return {
            "changed_sources": changes,
            "policy_snapshot": self._policy_snapshot(),
            "fuse_seconds": _SOURCE_POLICY_FUSE_SECONDS,
            "min_calls_for_action": _SOURCE_POLICY_MIN_CALLS,
        }

    def _missing_by_school(
        self,
        *,
        school_names: list[str],
        results: list[SearchResult],
        required_fields: list[str],
    ) -> dict[str, list[str]]:
        coverage = self._merger.coverage_by_school(results, required_fields)
        required = {normalise_variable_name(field) for field in required_fields}
        missing: dict[str, list[str]] = {}
        for school in school_names:
            present = coverage.get(school, set())
            missing[school] = sorted(required - present)
        return missing

    async def _persist_results(
        self,
        *,
        aligned: list[AlignedEntity],
        conflicts: list[ConflictRecord],
        freshness_days: int,
    ) -> dict[str, Any]:
        from scholarpath.db.models import DataPoint
        from scholarpath.db.session import async_session_factory

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=max(freshness_days, 0))
        school_names = [entity.canonical_name for entity in aligned]
        if not school_names:
            return {
                "persisted_data_points": 0,
                "persisted_conflicts": 0,
                "persisted_schools": 0,
            }

        async with async_session_factory() as session:
            school_map = await self._resolve_school_map(session, school_names)
            school_ids = [sid for sid in school_map.values() if sid is not None]
            if not school_ids:
                return {
                    "persisted_data_points": 0,
                    "persisted_conflicts": 0,
                    "persisted_schools": 0,
                }

            existing_stmt = (
                select(DataPoint)
                .where(DataPoint.school_id.in_(school_ids))
                .where(DataPoint.crawled_at >= cutoff)
            )
            existing_rows = list((await session.execute(existing_stmt)).scalars().all())

            existing_fingerprints = {
                self._datapoint_fingerprint(
                    school_id=str(row.school_id),
                    variable_name=row.variable_name,
                    source_name=row.source_name,
                    value_text=row.value_text,
                    value_numeric=row.value_numeric,
                    freshness_days=freshness_days,
                )
                for row in existing_rows
            }
            pending_fingerprints: set[tuple[str, str, str, str, str]] = set()

            inserted_rows: list[DataPoint] = []
            for entity in aligned:
                school_id = school_map.get(entity.canonical_name.lower())
                if school_id is None:
                    continue
                for item in entity.data_points:
                    if item.raw_data and item.raw_data.get("from_db"):
                        continue
                    variable = normalise_variable_name(item.variable_name)
                    numeric = item.value_numeric
                    if numeric is None:
                        numeric = coerce_numeric(
                            item.value_text,
                            variable_name=variable,
                        )
                    else:
                        numeric = normalise_numeric(
                            numeric,
                            variable_name=variable,
                            value_text=item.value_text,
                        )
                    fp = self._datapoint_fingerprint(
                        school_id=str(school_id),
                        variable_name=variable,
                        source_name=item.source_name,
                        value_text=item.value_text,
                        value_numeric=numeric,
                        freshness_days=freshness_days,
                    )
                    if fp in existing_fingerprints or fp in pending_fingerprints:
                        continue
                    pending_fingerprints.add(fp)

                    embedding = None
                    if item.raw_data and isinstance(item.raw_data.get("embedding"), list):
                        embedding = item.raw_data["embedding"]

                    inserted_rows.append(
                        DataPoint(
                            school_id=school_id,
                            program_id=None,
                            source_type=self._safe_source_type(item.source_type),
                            source_name=item.source_name[:100],
                            source_url=item.source_url or None,
                            variable_name=variable[:150],
                            value_text=item.value_text,
                            value_numeric=numeric,
                            confidence=self._bounded_confidence(item.confidence),
                            sample_size=item.sample_size,
                            temporal_range=item.temporal_range,
                            crawled_at=now,
                            derivation_method="deepsearch_v2",
                            embedding=embedding,
                        )
                    )

            if inserted_rows:
                session.add_all(inserted_rows)
                await session.flush()

            all_rows = existing_rows + inserted_rows
            persisted_conflicts = await self._persist_conflicts(
                session=session,
                conflicts=conflicts,
                school_map=school_map,
                datapoints=all_rows,
            )

            await session.commit()
            persisted_school_count = len({row.school_id for row in inserted_rows if row.school_id})
            return {
                "persisted_data_points": len(inserted_rows),
                "persisted_conflicts": persisted_conflicts,
                "persisted_schools": persisted_school_count,
            }

    async def _persist_conflicts(
        self,
        *,
        session: Any,
        conflicts: list[ConflictRecord],
        school_map: dict[str, Any],
        datapoints: list[Any],
    ) -> int:
        from scholarpath.db.models import Conflict, ResolutionStatus

        if not conflicts:
            return 0

        school_ids = [sid for sid in school_map.values() if sid is not None]
        existing_conflicts_stmt = select(Conflict).where(Conflict.school_id.in_(school_ids))
        existing_conflicts = list((await session.execute(existing_conflicts_stmt)).scalars().all())
        existing_keys = {
            (
                str(row.school_id),
                normalise_variable_name(row.variable_name),
                *sorted(
                    [
                        fingerprint_value(value_text=row.value_a, value_numeric=None),
                        fingerprint_value(value_text=row.value_b, value_numeric=None),
                    ]
                ),
            )
            for row in existing_conflicts
        }

        grouped: dict[tuple[str, str], list[Any]] = defaultdict(list)
        for row in datapoints:
            if row.school_id is None:
                continue
            key = (str(row.school_id), normalise_variable_name(row.variable_name))
            grouped[key].append(row)

        created = 0
        for record in conflicts:
            school_id = school_map.get(record.school.lower())
            if school_id is None:
                continue
            variable = normalise_variable_name(record.variable)
            candidates = grouped.get((str(school_id), variable), [])
            if len(candidates) < 2:
                continue

            pair = self._pick_conflict_pair(candidates, record)
            if pair is None:
                continue
            left, right = pair
            conflict_key = (
                str(school_id),
                variable,
                *sorted(
                    [
                        fingerprint_value(value_text=left.value_text, value_numeric=left.value_numeric),
                        fingerprint_value(value_text=right.value_text, value_numeric=right.value_numeric),
                    ]
                ),
            )
            if conflict_key in existing_keys:
                continue
            existing_keys.add(conflict_key)

            severity = self._safe_severity(record.severity)
            session.add(
                Conflict(
                    school_id=school_id,
                    variable_name=variable[:150],
                    datapoint_a_id=left.id,
                    datapoint_b_id=right.id,
                    severity=severity,
                    value_a=left.value_text,
                    value_b=right.value_text,
                    resolution_status=ResolutionStatus.UNRESOLVED.value,
                    causal_analysis={
                        "recommended_resolution": record.recommended_resolution,
                        "sources": record.sources,
                    },
                )
            )
            created += 1
        return created

    @staticmethod
    def _pick_conflict_pair(candidates: list[Any], record: ConflictRecord) -> tuple[Any, Any] | None:
        matched: list[Any] = []
        for source, value in zip(record.sources, record.values):
            for row in candidates:
                if row in matched:
                    continue
                if row.source_name != source:
                    continue
                if normalise_variable_name(row.variable_name) != normalise_variable_name(record.variable):
                    continue
                if (
                    fingerprint_value(value_text=row.value_text, value_numeric=row.value_numeric)
                    == fingerprint_value(value_text=value, value_numeric=None)
                ):
                    matched.append(row)
                    break
            if len(matched) >= 2:
                return matched[0], matched[1]

        best_by_value: dict[str, Any] = {}
        for row in sorted(candidates, key=lambda item: item.confidence, reverse=True):
            value_key = fingerprint_value(value_text=row.value_text, value_numeric=row.value_numeric)
            best_by_value.setdefault(value_key, row)
        if len(best_by_value) < 2:
            return None
        rows = list(best_by_value.values())[:2]
        return rows[0], rows[1]

    async def _resolve_school_map(self, session: Any, school_names: list[str]) -> dict[str, Any]:
        from scholarpath.db.models import School

        school_map: dict[str, Any] = {}
        lower_names = [name.lower() for name in school_names]
        stmt = select(func.lower(School.name), School.id).where(
            func.lower(School.name).in_(lower_names),
        )
        for lower_name, school_id in (await session.execute(stmt)).all():
            school_map[lower_name] = school_id

        for name in school_names:
            key = name.lower()
            if key in school_map:
                continue
            fuzzy_stmt = (
                select(School.id)
                .where(School.name.ilike(f"%{name}%"))
                .order_by(School.us_news_rank.asc().nullslast())
                .limit(1)
            )
            school_id = (await session.execute(fuzzy_stmt)).scalars().first()
            school_map[key] = school_id
        return school_map

    @staticmethod
    def _datapoint_fingerprint(
        *,
        school_id: str,
        variable_name: str,
        source_name: str,
        value_text: str,
        value_numeric: float | None,
        freshness_days: int,
    ) -> tuple[str, str, str, str, str]:
        canonical_var = normalise_variable_name(variable_name)
        source = source_name.strip().lower()
        numeric = normalise_numeric(
            value_numeric,
            variable_name=canonical_var,
            value_text=value_text,
        )
        if numeric is None:
            numeric = coerce_numeric(value_text, variable_name=canonical_var)
        return (
            school_id,
            canonical_var,
            source,
            fingerprint_value(value_text=value_text, value_numeric=numeric),
            f"{max(freshness_days, 0)}d",
        )

    @staticmethod
    def _bounded_confidence(confidence: float) -> float:
        return max(0.0, min(1.0, float(confidence)))

    @staticmethod
    def _safe_source_type(source_type: str) -> str:
        if source_type in {"official", "proxy", "ugc"}:
            return source_type
        return "proxy"

    @staticmethod
    def _safe_severity(severity: str) -> str:
        if severity in {"low", "medium", "high"}:
            return severity
        return "low"

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
        schools: list[dict[str, Any]] = []
        for entity in aligned:
            data: dict[str, Any] = {}
            for dp in entity.data_points:
                variable = normalise_variable_name(dp.variable_name)
                existing = data.get(variable)
                if existing is None or dp.confidence > existing.get("confidence", 0):
                    data[variable] = {
                        "value": dp.value_numeric if dp.value_numeric is not None else dp.value_text,
                        "source": dp.source_name,
                        "confidence": dp.confidence,
                    }
            schools.append(
                {
                    "name": entity.canonical_name,
                    "aliases": entity.aliases,
                    "data": data,
                    "sources_count": len({dp.source_name for dp in entity.data_points}),
                }
            )
        return schools

    def _critical_coverage_by_school(
        self,
        *,
        school_names: list[str],
        results: list[SearchResult],
        required_fields: list[str],
    ) -> dict[str, dict[str, Any]]:
        critical = {
            normalise_variable_name(field)
            for field in PRD_EXPANDED_CRITICAL_FIELDS
        }
        required_critical = {
            normalise_variable_name(field)
            for field in required_fields
            if normalise_variable_name(field) in critical
        }
        coverage = self._merger.coverage_by_school(
            results,
            sorted(required_critical),
        )
        total_slots = len(required_critical)
        payload: dict[str, dict[str, Any]] = {}
        for school in school_names:
            covered = len(coverage.get(school, set()) & required_critical)
            recall = covered / total_slots if total_slots > 0 else 1.0
            payload[school] = {
                "covered_critical_slots": covered,
                "critical_slots": total_slots,
                "critical_recall": round(recall, 4),
            }
        return payload

    @staticmethod
    def _count_multi_source_agreements(results: list[SearchResult]) -> int:
        agreements = 0
        for item in results:
            if not item.raw_data:
                continue
            deduped = item.raw_data.get("deduped_sources")
            if not isinstance(deduped, list):
                continue
            distinct = {str(source).strip().lower() for source in deduped if source}
            if len(distinct) >= 2:
                agreements += 1
        return agreements

    @staticmethod
    def _compute_coverage(
        *,
        aligned: list[AlignedEntity],
        required_fields: list[str],
    ) -> float:
        required = {normalise_variable_name(field) for field in required_fields}
        if not aligned:
            return 0.0
        if not required:
            return 1.0
        scores: list[float] = []
        for entity in aligned:
            present = {normalise_variable_name(dp.variable_name) for dp in entity.data_points}
            scores.append(len(required & present) / len(required))
        return sum(scores) / len(scores)

    @staticmethod
    def _estimate_tokens_for_source(source_name: str, calls: int) -> int:
        count = max(calls, 0)
        if source_name == "web_search":
            return count * _TOKEN_ESTIMATE_SELF_SOURCE
        if source_name == "internal_web_search":
            return count * _TOKEN_ESTIMATE_INTERNAL_WEB
        return 0

    @staticmethod
    def _estimate_tokens(
        *,
        self_source_calls: int,
        internal_websearch_calls: int,
        conflict_count: int,
    ) -> dict[str, int]:
        self_tokens = self_source_calls * _TOKEN_ESTIMATE_SELF_SOURCE
        internal_tokens = internal_websearch_calls * _TOKEN_ESTIMATE_INTERNAL_WEB
        align_tokens = _TOKEN_ESTIMATE_ALIGN
        conflict_tokens = conflict_count * _TOKEN_ESTIMATE_CONFLICT
        total = self_tokens + internal_tokens + align_tokens + conflict_tokens
        return {
            "self_source_estimated": self_tokens,
            "internal_websearch_estimated": internal_tokens,
            "entity_alignment_estimated": align_tokens,
            "conflict_estimated": conflict_tokens,
            "total_estimated": total,
        }

    @staticmethod
    def _resolve_required_fields(required_fields: list[str] | None) -> list[str]:
        base = required_fields or PRD_EXPANDED_CRITICAL_FIELDS
        return sorted({normalise_variable_name(field) for field in base})

    async def _emit(self, stage: str, message: str, progress: float) -> None:
        """Emit a progress event if a callback is registered."""
        if self._on_progress is None:
            return
        try:
            result = self._on_progress(stage, message, progress)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.debug("Progress callback failed for stage '%s'", stage)
