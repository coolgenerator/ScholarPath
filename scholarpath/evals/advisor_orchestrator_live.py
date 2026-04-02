"""Advisor orchestrator gold evaluation runner (orchestrator + re-edit merged)."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from scholarpath.advisor.contracts import (
    AdvisorAction,
    AdvisorCapability,
    InfoCardArtifact,
    AdvisorRequest,
    AdvisorResponse,
    AdvisorRouteMeta,
    MemoryIngestEvent,
)
from scholarpath.advisor.memory_context import ContextMetrics, persist_turn_message
from scholarpath.advisor.orchestration import (
    CapabilityContext,
    CapabilityDefinition,
    CapabilityRegistry,
    CapabilityResult,
    OrchestratorRuntime,
)
from scholarpath.advisor.orchestrator import AdvisorOrchestrator
from scholarpath.chat.memory import ChatMemory
from scholarpath.config import settings
from scholarpath.db.models.advisor_memory import (
    AdvisorMemoryItem,
    AdvisorMessage,
    AdvisorMessageChunk,
)
from scholarpath.db.models.base import Base
from scholarpath.evals.advisor_orchestrator_judge import (
    AdvisorOrchestratorJudge,
    build_eval_llm_client,
)
from scholarpath.evals.advisor_orchestrator_io import (
    append_history,
    json_default,
    write_cases_jsonl,
    write_json,
    write_markdown_summary,
)
from scholarpath.evals.advisor_orchestrator_selection import (
    select_eval_cases,
    select_reedit_cases,
    select_stratified_cases,
)
from scholarpath.llm import LLMClient

DEFAULT_DATASET_PATH = (
    Path(__file__).resolve().parent
    / "datasets"
    / "advisor_orchestrator_gold_v1.json"
)
DEFAULT_REEDIT_DATASET_PATH = (
    Path(__file__).resolve().parent
    / "datasets"
    / "advisor_reedit_gold_v1.json"
)
DEFAULT_OUTPUT_DIR = Path(".benchmarks/advisor_orchestrator")

_VALID_CATEGORIES = {
    "cat_single_intent": 12,
    "cat_multi_over_limit": 10,
    "cat_conflict_clarify": 6,
    "cat_low_confidence": 4,
    "cat_explicit_recovery": 4,
    "cat_input_error": 2,
    "cat_memory_degraded": 2,
}
_VALID_LANG_TAGS = {"lang_zh", "lang_en"}
_ORCHESTRATOR_MINI_CATEGORY_QUOTAS: dict[str, int] = {
    "cat_single_intent": 3,
    "cat_multi_over_limit": 2,
    "cat_conflict_clarify": 1,
    "cat_low_confidence": 1,
    "cat_explicit_recovery": 1,
    "cat_input_error": 1,
    "cat_memory_degraded": 1,
}
_REEDIT_MINI_CATEGORY_QUOTAS: dict[str, int] = {
    "cat_reedit_middle": 1,
    "cat_reedit_edge": 1,
    "cat_reedit_tail": 1,
    "cat_reedit_invalid": 2,
    "cat_reedit_history": 1,
}
_REEDIT_REQUIRED_EXPECT_KEYS = {
    "overwrite_success",
    "truncation_correct",
    "history_consistent",
    "contract_ok",
}
_NON_CAUSAL_CAPABILITIES: set[str] = {
    "common.general",
    "common.emotional_support",
    "common.clarify",
    "undergrad.school.query",
    "undergrad.strategy.plan",
}
_DEFAULT_REAL_CAPABILITIES: tuple[str, ...] = (
    "undergrad.school.recommend",
    "undergrad.school.query",
    "offer.compare",
    "offer.what_if",
)
_EVAL_STUDENT_UUID = "9b46c72c-d57b-48e5-8409-4c9770db0f2c"
_PENDING_KEY = "advisor_pending_queue"
_FAILED_KEY = "advisor_failed_steps"


@dataclass
class AdvisorOrchestratorEvalCase:
    case_id: str
    request: dict[str, Any]
    seed: dict[str, Any]
    context_profile: str
    expect: dict[str, Any]
    tags: list[str] = field(default_factory=list)


@dataclass
class AdvisorOrchestratorEvalDataset:
    dataset_id: str
    version: str
    thresholds: dict[str, Any]
    cases: list[AdvisorOrchestratorEvalCase]


@dataclass
class AdvisorReeditEvalCase:
    case_id: str
    request: dict[str, Any]
    seed: dict[str, Any]
    expect: dict[str, Any]
    tags: list[str] = field(default_factory=list)


@dataclass
class AdvisorReeditEvalDataset:
    dataset_id: str
    version: str
    thresholds: dict[str, Any]
    cases: list[AdvisorReeditEvalCase]


@dataclass
class AdvisorOrchestratorCaseReport:
    case_id: str
    tags: list[str]
    request: dict[str, Any]
    response: dict[str, Any]
    deterministic_checks: dict[str, Any]
    deterministic_score: float
    judge_score: float = 0.0
    final_score: float = 0.0
    errors: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AdvisorReeditCaseReport:
    case_id: str
    tags: list[str]
    request: dict[str, Any]
    response: dict[str, Any]
    deterministic_checks: dict[str, Any]
    deterministic_score: float
    judge_score: float = 0.0
    final_score: float = 0.0
    errors: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AdvisorOrchestratorEvalReport:
    run_id: str
    generated_at: str
    config: dict[str, Any]
    orchestrator_metrics: dict[str, Any]
    reedit_metrics: dict[str, Any]
    merged_metrics: dict[str, Any]
    metrics: dict[str, Any]
    status: str
    recommendations: list[str]
    stub_metrics: dict[str, Any] = field(default_factory=dict)
    real_metrics: dict[str, Any] = field(default_factory=dict)
    gate_by_lane: dict[str, Any] = field(default_factory=dict)
    warning_counts_by_stage: dict[str, int] = field(default_factory=dict)
    judge_summary: dict[str, Any] = field(default_factory=dict)
    cases: list[AdvisorOrchestratorCaseReport] = field(default_factory=list)
    reedit_cases: list[AdvisorReeditCaseReport] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class _EvalRedis:
    """Minimal in-memory async redis subset used by ChatMemory."""

    def __init__(self) -> None:
        self._lists: dict[str, list[str]] = {}
        self._hashes: dict[str, dict[str, str]] = {}

    async def rpush(self, key: str, value: str) -> None:
        self._lists.setdefault(key, []).append(value)

    async def ltrim(self, key: str, start: int, end: int) -> None:
        values = self._lists.get(key, [])
        self._lists[key] = _slice(values, start, end)

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        values = self._lists.get(key, [])
        return _slice(values, start, end)

    async def hset(self, key: str, field: str, value: str) -> None:
        self._hashes.setdefault(key, {})[field] = value

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hashes.get(key, {}))

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self._lists.pop(key, None)
            self._hashes.pop(key, None)


class _NoopScalarResult:
    def first(self) -> None:
        return None

    def all(self) -> list[Any]:
        return []


class _NoopExecuteResult:
    def scalars(self) -> _NoopScalarResult:
        return _NoopScalarResult()

    def all(self) -> list[Any]:
        return []


class _NoopAsyncSession:
    """Minimal async session adapter for eval lanes that should avoid DB writes."""

    async def execute(self, *args, **kwargs) -> _NoopExecuteResult:  # type: ignore[no-untyped-def]
        return _NoopExecuteResult()

    async def flush(self) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None

    async def refresh(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    async def get(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    async def delete(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None

    def add(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        return None


class EvalOrchestratorRuntime(OrchestratorRuntime):
    """Runtime adapter for eval to suppress DB ingest noise and use seeded context."""

    def __init__(
        self,
        *,
        session: AsyncSession,
        redis,
        registry: CapabilityRegistry,
        context_profile: str,
        seed: dict[str, Any],
    ) -> None:
        super().__init__(session=session, redis=redis, registry=registry)
        self._context_profile = context_profile
        self._seed = seed

    async def record_turn_event(self, *, event: MemoryIngestEvent) -> None:
        return None

    async def assemble_context(
        self,
        *,
        session_id: str,
        stage: str,
        message: str,
        student_id,
        domain: str | None,
    ) -> tuple[dict[str, Any], ContextMetrics]:
        seeded_common = dict(self._seed.get("common_context", {}))
        pending = self._seed.get("pending", [])
        failed = self._seed.get("failed", [])
        if isinstance(pending, list) and _PENDING_KEY not in seeded_common:
            seeded_common[_PENDING_KEY] = pending
        if isinstance(failed, list) and _FAILED_KEY not in seeded_common:
            seeded_common[_FAILED_KEY] = failed

        recent_msgs = self._seed.get("recent_messages", [])
        if isinstance(recent_msgs, list):
            recent_text = "\n".join(str(item) for item in recent_msgs if str(item).strip())
        else:
            recent_text = str(recent_msgs or "")

        route_context = (
            f"stage={stage}\n"
            f"session={session_id}\n"
            f"domain={domain}\n"
            f"recent={recent_text}\n"
            f"user_message={message}"
        )
        context = {
            "recent_messages": recent_text,
            "route_prompt_context": route_context,
            "undergrad": dict(self._seed.get("undergrad_context", {})),
            "offer": dict(self._seed.get("offer_context", {})),
            "common": seeded_common,
            "memory_items": list(self._seed.get("memory_items", [])),
            "memory_conflicts": list(self._seed.get("memory_conflicts", [])),
            "retrieved_chunks": list(self._seed.get("retrieved_chunks", [])),
        }
        memory_degraded = self._context_profile == "memory_degraded"
        metrics = ContextMetrics(
            context_tokens=max(len(route_context) // 4, 1),
            memory_hits=len(context["memory_items"]),
            rag_hits=len(context["retrieved_chunks"]),
            rag_latency_ms=8 if not memory_degraded else 120,
            memory_degraded=memory_degraded,
            memory_conflicts=len(context["memory_conflicts"]),
        )
        return context, metrics


class RealLaneRuntime(OrchestratorRuntime):
    """Runtime adapter for real lane to avoid side-effectful async ingestion tasks."""

    async def record_turn_event(self, *, event: MemoryIngestEvent) -> None:
        return None


class EvalAdvisorOrchestrator(AdvisorOrchestrator):
    """Isolated orchestrator for eval: no DB memory ingest, seeded context only."""

    def __init__(
        self,
        *,
        llm: LLMClient,
        registry: CapabilityRegistry,
        context_profile: str,
        seed: dict[str, Any],
    ) -> None:
        eval_session = _NoopAsyncSession()
        eval_redis = _EvalRedis()
        super().__init__(
            llm=llm,
            session=eval_session,  # type: ignore[arg-type]
            redis=eval_redis,  # type: ignore[arg-type]
            registry=registry,
            runtime=EvalOrchestratorRuntime(
                session=eval_session,  # type: ignore[arg-type]
                redis=eval_redis,
                registry=registry,
                context_profile=context_profile,
                seed=seed,
            ),
        )


def _parse_expected_counts(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        key_text = str(key).strip()
        if not key_text:
            continue
        try:
            count = int(value)
        except (TypeError, ValueError):
            continue
        if count < 0:
            continue
        out[key_text] = count
    return out


def _parse_expected_int(raw: Any, *, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value <= 0:
        return default
    return value


def _normalize_execution_lane(execution_lane: str) -> str:
    lane = str(execution_lane or "both").strip().lower()
    if lane not in {"stub", "real", "both"}:
        raise ValueError("execution_lane must be one of: stub, real, both")
    return lane


def _normalize_real_capabilities(real_capabilities: list[str] | None) -> tuple[str, ...]:
    if real_capabilities is None:
        return _DEFAULT_REAL_CAPABILITIES
    cleaned = [
        str(capability).strip()
        for capability in real_capabilities
        if str(capability).strip()
    ]
    return tuple(dict.fromkeys(cleaned))


def _empty_usage_stats(*, disabled: bool = False) -> dict[str, Any]:
    payload = {
        "calls": 0,
        "errors": 0,
        "tokens": 0,
        "p95_latency_ms": 0.0,
        "rate_limit_errors": 0,
        "latency_total_ms": 0,
    }
    if disabled:
        payload["disabled"] = True
    return payload


def _lane_gate(metrics: dict[str, Any], *, include_reedit: bool) -> dict[str, Any]:
    failed_checks: list[str] = []
    if float(metrics.get("contract_valid_rate", 0.0) or 0.0) < 1.0:
        failed_checks.append("contract_valid_rate")
    if int(metrics.get("execution_limit_violations", 0) or 0) > 0:
        failed_checks.append("execution_limit_violations")
    if include_reedit:
        for key in (
            "reedit_overwrite_success_rate",
            "reedit_truncation_correct_rate",
            "reedit_history_consistency_rate",
        ):
            if float(metrics.get(key, 0.0) or 0.0) < 0.95:
                failed_checks.append(key)
    return {
        "passed": len(failed_checks) == 0,
        "failed_checks": failed_checks,
    }


def _warning_counts_by_stage(errors: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in errors:
        stage = str(item.get("stage", "unknown")).strip() or "unknown"
        counts[stage] = counts.get(stage, 0) + 1
    return dict(sorted(counts.items()))


def _build_real_registry(*, capabilities: tuple[str, ...]) -> CapabilityRegistry:
    from scholarpath.advisor.adapters import build_default_registry

    source = build_default_registry()
    if not capabilities:
        return source

    selected = set(capabilities)
    registry = CapabilityRegistry()
    for capability_id in source.list_capability_ids():
        if capability_id not in selected:
            continue
        definition = source.get(capability_id)
        if definition is None:
            continue
        registry.register(definition)
    return registry


def load_advisor_orchestrator_dataset(
    path: str | Path | None = None,
) -> AdvisorOrchestratorEvalDataset:
    dataset_path = Path(path) if path is not None else DEFAULT_DATASET_PATH
    payload = json.loads(dataset_path.read_text(encoding="utf-8"))
    raw_cases = payload.get("cases", [])
    if not isinstance(raw_cases, list):
        raise ValueError("advisor_orchestrator dataset cases must be a list")

    schema = payload.get("schema", {})
    if not isinstance(schema, dict):
        schema = {}
    expected_case_count = _parse_expected_int(
        schema.get("case_count"),
        default=40,
    )
    expected_category_counts = _parse_expected_counts(
        schema.get("category_counts"),
    ) or dict(_VALID_CATEGORIES)
    expected_language_counts = _parse_expected_counts(
        schema.get("language_counts"),
    ) or {"lang_zh": 28, "lang_en": 12}
    min_code_switch = _parse_expected_int(
        schema.get("min_code_switch"),
        default=6,
    )

    cases: list[AdvisorOrchestratorEvalCase] = []
    category_counter = {key: 0 for key in expected_category_counts}
    lang_counter = {"lang_zh": 0, "lang_en": 0}
    code_switch_count = 0
    seen_ids: set[str] = set()

    for idx, row in enumerate(raw_cases):
        if not isinstance(row, dict):
            raise ValueError(f"case[{idx}] must be an object")
        case_id = str(row.get("case_id", "")).strip()
        if not case_id:
            raise ValueError(f"case[{idx}] missing case_id")
        if case_id in seen_ids:
            raise ValueError(f"Duplicate case_id: {case_id}")
        seen_ids.add(case_id)

        request = row.get("request")
        if not isinstance(request, dict):
            raise ValueError(f"case[{idx}] request must be object")
        message = str(request.get("message", "")).strip()
        if not message:
            raise ValueError(f"case[{idx}] request.message must be non-empty")

        seed = row.get("seed", {})
        if not isinstance(seed, dict):
            raise ValueError(f"case[{idx}] seed must be object")

        context_profile = str(row.get("context_profile", "normal")).strip().lower()
        if context_profile not in {"normal", "memory_degraded"}:
            raise ValueError(f"case[{idx}] invalid context_profile={context_profile}")

        expect = row.get("expect")
        if not isinstance(expect, dict):
            raise ValueError(f"case[{idx}] expect must be object")
        if "primary_any" in expect and not isinstance(expect.get("primary_any"), list):
            raise ValueError(f"case[{idx}] expect.primary_any must be list")
        if "error_code_any" in expect and not isinstance(expect.get("error_code_any"), list):
            raise ValueError(f"case[{idx}] expect.error_code_any must be list")

        tags = row.get("tags", [])
        if not isinstance(tags, list):
            raise ValueError(f"case[{idx}] tags must be list")
        tags = [str(item).strip() for item in tags if str(item).strip()]
        if not tags:
            raise ValueError(f"case[{idx}] tags must not be empty")

        categories = [tag for tag in tags if tag in expected_category_counts]
        if len(categories) != 1:
            raise ValueError(f"case[{idx}] must include exactly one category tag")
        category_counter[categories[0]] += 1

        langs = [tag for tag in tags if tag in _VALID_LANG_TAGS]
        if len(langs) != 1:
            raise ValueError(f"case[{idx}] must include exactly one language tag")
        lang_counter[langs[0]] += 1

        if "code_switch" in tags:
            code_switch_count += 1

        cases.append(
            AdvisorOrchestratorEvalCase(
                case_id=case_id,
                request=request,
                seed=seed,
                context_profile=context_profile,
                expect=expect,
                tags=tags,
            )
        )

    if len(cases) != expected_case_count:
        raise ValueError(
            "advisor_orchestrator dataset must contain "
            f"{expected_case_count} cases, got {len(cases)}"
        )

    for category, expected in expected_category_counts.items():
        actual = category_counter.get(category, 0)
        if actual != expected:
            raise ValueError(f"{category} requires {expected} cases, got {actual}")

    if (
        lang_counter["lang_zh"] != expected_language_counts.get("lang_zh", 0)
        or lang_counter["lang_en"] != expected_language_counts.get("lang_en", 0)
    ):
        raise ValueError(
            "Language ratio must match dataset schema "
            f"(got zh={lang_counter['lang_zh']}, en={lang_counter['lang_en']})"
        )
    if code_switch_count < min_code_switch:
        raise ValueError(
            f"code_switch coverage must be >={min_code_switch}, got {code_switch_count}"
        )

    thresholds = payload.get("strict_thresholds", {})
    if not isinstance(thresholds, dict):
        thresholds = {}

    return AdvisorOrchestratorEvalDataset(
        dataset_id=str(payload.get("dataset_id", dataset_path.stem)),
        version=str(payload.get("version", "1.0.0")),
        thresholds=thresholds,
        cases=cases,
    )


def load_advisor_reedit_dataset(
    path: str | Path | None = None,
) -> AdvisorReeditEvalDataset:
    dataset_path = Path(path) if path is not None else DEFAULT_REEDIT_DATASET_PATH
    payload = json.loads(dataset_path.read_text(encoding="utf-8"))
    raw_cases = payload.get("cases", [])
    if not isinstance(raw_cases, list):
        raise ValueError("advisor_reedit dataset cases must be a list")

    schema = payload.get("schema", {})
    if not isinstance(schema, dict):
        schema = {}
    expected_case_count = _parse_expected_int(
        schema.get("case_count"),
        default=12,
    )
    expected_category_counts = _parse_expected_counts(
        schema.get("category_counts"),
    )

    cases: list[AdvisorReeditEvalCase] = []
    category_counter: dict[str, int] = {
        key: 0 for key in expected_category_counts
    }
    seen_ids: set[str] = set()
    for idx, row in enumerate(raw_cases):
        if not isinstance(row, dict):
            raise ValueError(f"case[{idx}] must be an object")

        case_id = str(row.get("case_id", "")).strip()
        if not case_id:
            raise ValueError(f"case[{idx}] missing case_id")
        if case_id in seen_ids:
            raise ValueError(f"Duplicate case_id: {case_id}")
        seen_ids.add(case_id)

        request = row.get("request")
        if not isinstance(request, dict):
            raise ValueError(f"case[{idx}] request must be object")
        message = str(request.get("message", "")).strip()
        if not message:
            raise ValueError(f"case[{idx}] request.message must be non-empty")
        edit = request.get("edit")
        if not isinstance(edit, dict):
            raise ValueError(f"case[{idx}] request.edit must be object")
        target_turn = str(edit.get("target_turn_id", "")).strip()
        if not target_turn:
            raise ValueError(f"case[{idx}] request.edit.target_turn_id must be non-empty")
        if str(edit.get("mode", "overwrite")).strip() != "overwrite":
            raise ValueError(f"case[{idx}] request.edit.mode must be overwrite")

        seed = row.get("seed", {})
        if not isinstance(seed, dict):
            raise ValueError(f"case[{idx}] seed must be object")
        timeline = seed.get("timeline")
        if not isinstance(timeline, list) or not timeline:
            raise ValueError(f"case[{idx}] seed.timeline must be non-empty list")
        for t_idx, item in enumerate(timeline):
            if not isinstance(item, dict):
                raise ValueError(f"case[{idx}] seed.timeline[{t_idx}] must be object")
            turn_id = str(item.get("turn_id", "")).strip()
            role = str(item.get("role", "")).strip()
            content = str(item.get("content", "")).strip()
            if not turn_id or role not in {"user", "assistant"} or not content:
                raise ValueError(
                    f"case[{idx}] seed.timeline[{t_idx}] requires turn_id/role/content"
                )

        expect = row.get("expect")
        if not isinstance(expect, dict):
            raise ValueError(f"case[{idx}] expect must be object")
        missing = [key for key in _REEDIT_REQUIRED_EXPECT_KEYS if key not in expect]
        if missing:
            raise ValueError(f"case[{idx}] missing expect keys: {', '.join(sorted(missing))}")
        for key in _REEDIT_REQUIRED_EXPECT_KEYS:
            if not isinstance(expect.get(key), bool):
                raise ValueError(f"case[{idx}] expect.{key} must be bool")
        if "capability_any" in expect and not isinstance(expect.get("capability_any"), list):
            raise ValueError(f"case[{idx}] expect.capability_any must be list")

        tags = row.get("tags", [])
        if not isinstance(tags, list):
            raise ValueError(f"case[{idx}] tags must be list")
        tags = [str(item).strip() for item in tags if str(item).strip()]
        if not tags:
            raise ValueError(f"case[{idx}] tags must not be empty")
        if expected_category_counts:
            categories = [tag for tag in tags if tag in expected_category_counts]
            if len(categories) != 1:
                raise ValueError(f"case[{idx}] must include exactly one reedit category tag")
            category_counter[categories[0]] += 1

        cases.append(
            AdvisorReeditEvalCase(
                case_id=case_id,
                request=request,
                seed=seed,
                expect=expect,
                tags=tags,
            )
        )

    if len(cases) != expected_case_count:
        raise ValueError(
            f"advisor_reedit dataset must contain {expected_case_count} cases, got {len(cases)}"
        )
    if expected_category_counts:
        for category, expected in expected_category_counts.items():
            actual = category_counter.get(category, 0)
            if actual != expected:
                raise ValueError(f"{category} requires {expected} cases, got {actual}")

    thresholds = payload.get("strict_thresholds", {})
    if not isinstance(thresholds, dict):
        thresholds = {}

    return AdvisorReeditEvalDataset(
        dataset_id=str(payload.get("dataset_id", dataset_path.stem)),
        version=str(payload.get("version", "1.0.0")),
        thresholds=thresholds,
        cases=cases,
    )


def build_eval_registry() -> CapabilityRegistry:
    registry = CapabilityRegistry()
    for capability_id, domain, requires_student, description in (
        ("undergrad.profile.intake", "undergrad", True, "undergrad profile intake"),
        ("undergrad.school.recommend", "undergrad", True, "undergrad recommendation"),
        ("undergrad.school.query", "undergrad", True, "undergrad school query"),
        ("undergrad.strategy.plan", "undergrad", True, "undergrad strategy"),
        ("offer.compare", "offer", True, "offer comparison"),
        ("offer.decision", "offer", True, "offer decision"),
        ("offer.what_if", "offer", True, "offer what-if"),
        ("common.general", "common", False, "general"),
        ("common.emotional_support", "common", False, "emotional support"),
        ("common.clarify", "common", False, "clarify"),
    ):
        registry.register(
            CapabilityDefinition(
                capability_id=capability_id,  # type: ignore[arg-type]
                domain=domain,  # type: ignore[arg-type]
                description=description,
                requires_student=requires_student,
                handler=_make_stub_handler(capability_id),  # type: ignore[arg-type]
            )
        )
    return registry


def _make_stub_handler(capability_id: AdvisorCapability):
    async def _handler(ctx: CapabilityContext) -> CapabilityResult:
        raw_fail = ctx.client_context.get("eval_force_fail", [])
        fail_caps = {
            str(item).strip()
            for item in (raw_fail if isinstance(raw_fail, list) else [])
            if str(item).strip()
        }
        if capability_id in fail_caps:
            raise RuntimeError(f"forced eval failure: {capability_id}")

        if capability_id == "common.clarify":
            return CapabilityResult(
                assistant_text="请先确认你的主任务优先级，我会按优先级继续执行。",
                actions=[
                    AdvisorAction(
                        action_id="route.clarify",
                        label="先澄清优先级",
                        payload={"client_context": {"trigger": "route.clarify"}},
                    )
                ],
                step_summary={"message": "Clarification requested."},
            )

        if capability_id == "undergrad.school.query":
            raw_obs = ctx.client_context.get("eval_deepsearch")
            deepsearch_obs = raw_obs if isinstance(raw_obs, dict) else {}
            profile = str(ctx.client_context.get("eval_deepsearch_profile", "")).strip().lower()
            if not deepsearch_obs and profile in {"cold", "warm"}:
                deepsearch_obs = {
                    "triggered": profile == "cold",
                    "reuse_hit": profile == "warm",
                    "db_hit_ratio": 0.12 if profile == "cold" else 0.91,
                    "external_calls": 3 if profile == "cold" else 0,
                    "missing_fields_before": ["tuition_out_of_state", "sat_50"] if profile == "cold" else [],
                }
            triggered = bool(deepsearch_obs.get("triggered", False))
            reuse_hit = bool(deepsearch_obs.get("reuse_hit", False))
            try:
                db_hit_ratio = float(deepsearch_obs.get("db_hit_ratio", 0.0) or 0.0)
            except (TypeError, ValueError):
                db_hit_ratio = 0.0
            external_calls_raw = deepsearch_obs.get("external_calls", 0)
            try:
                external_calls = int(external_calls_raw or 0)
            except (TypeError, ValueError):
                external_calls = 0
            missing_fields_raw = deepsearch_obs.get("missing_fields_before", [])
            missing_fields_before = (
                [str(item) for item in missing_fields_raw if str(item).strip()]
                if isinstance(missing_fields_raw, list)
                else []
            )
            deepsearch_payload = {
                "triggered": triggered,
                "reuse_hit": reuse_hit,
                "db_hit_ratio": round(max(0.0, min(db_hit_ratio, 1.0)), 4),
                "external_calls": max(0, external_calls),
                "missing_fields_before": missing_fields_before,
            }
            text = f"{capability_id} executed by eval stub."
            return CapabilityResult(
                assistant_text=text,
                artifacts=[
                    InfoCardArtifact(
                        title="School Query",
                        summary="Eval stub school query response.",
                        data={
                            "query": ctx.message,
                            "internal_deepsearch": deepsearch_payload,
                        },
                    )
                ],
                actions=[
                    AdvisorAction(
                        action_id="eval.inspect",
                        label=f"Inspect {capability_id}",
                        payload={"capability_hint": capability_id},
                    )
                ],
                step_summary={"message": f"{capability_id} completed."},
                metadata={"llm_calls": 0},
            )

        text = f"{capability_id} executed by eval stub."
        return CapabilityResult(
            assistant_text=text,
            actions=[
                AdvisorAction(
                    action_id="eval.inspect",
                    label=f"Inspect {capability_id}",
                    payload={"capability_hint": capability_id},
                )
            ],
            step_summary={"message": f"{capability_id} completed."},
        )

    return _handler


async def run_advisor_orchestrator_eval(
    *,
    dataset_path: str | Path | None = None,
    reedit_dataset_path: str | Path | None = None,
    include_reedit: bool = True,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    judge_enabled: bool = True,
    judge_concurrency: int = 2,
    judge_temperature: float = 0.1,
    judge_max_tokens: int = 1200,
    max_rpm_total: int = 180,
    sample_size: int = 40,
    case_ids: list[str] | None = None,
    reedit_sample_size: int | None = None,
    reedit_case_ids: list[str] | None = None,
    execution_lane: str = "both",
    real_capabilities: list[str] | None = None,
    warning_gate: bool = True,
    usage_enabled: bool = True,
    llm: LLMClient | None = None,
) -> AdvisorOrchestratorEvalReport:
    if max_rpm_total > 200:
        raise ValueError("max_rpm_total must be <= 200")
    if max_rpm_total <= 0:
        raise ValueError("max_rpm_total must be > 0")
    lane = _normalize_execution_lane(execution_lane)
    selected_real_capabilities = _normalize_real_capabilities(real_capabilities)

    dataset = load_advisor_orchestrator_dataset(dataset_path)
    reedit_dataset = load_advisor_reedit_dataset(reedit_dataset_path)

    selected_cases = _select_eval_cases(dataset.cases, sample_size=sample_size, case_ids=case_ids)
    selected_reedit_cases = (
        _select_reedit_cases(
            reedit_dataset.cases,
            sample_size=reedit_sample_size,
            case_ids=reedit_case_ids,
        )
        if include_reedit
        else []
    )

    run_id = (
        "advisor-orchestrator-"
        + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    now = datetime.now(timezone.utc).isoformat()
    out_root = Path(output_dir)
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    router_llm = llm or build_eval_llm_client(max_rpm_total=max_rpm_total)
    judge_llm = router_llm
    stub_registry = build_eval_registry()

    stub_case_reports: list[AdvisorOrchestratorCaseReport] = []
    stub_judge_payloads: list[dict[str, Any]] = []
    stub_execution_errors: list[dict[str, Any]] = []
    if lane in {"stub", "both"}:
        (
            stub_case_reports,
            stub_judge_payloads,
            stub_execution_errors,
        ) = await _run_orchestrator_cases(
            run_id=run_id,
            selected_cases=selected_cases,
            llm=router_llm,
            registry=stub_registry,
            usage_enabled=usage_enabled,
        )

    real_case_reports: list[AdvisorOrchestratorCaseReport] = []
    real_judge_payloads: list[dict[str, Any]] = []
    real_execution_errors: list[dict[str, Any]] = []
    if lane in {"real", "both"}:
        (
            real_case_reports,
            real_judge_payloads,
            real_execution_errors,
        ) = await _run_orchestrator_cases_real(
            run_id=run_id,
            selected_cases=selected_cases,
            llm=router_llm,
            capabilities=selected_real_capabilities,
            usage_enabled=usage_enabled,
        )

    case_reports = stub_case_reports if stub_case_reports else real_case_reports
    judge_case_payloads = stub_judge_payloads if stub_judge_payloads else real_judge_payloads
    execution_errors = stub_execution_errors if stub_execution_errors else real_execution_errors

    reedit_case_reports: list[AdvisorReeditCaseReport] = []
    reedit_judge_payloads: list[dict[str, Any]] = []
    reedit_errors: list[dict[str, Any]] = []
    if include_reedit:
        (
            reedit_case_reports,
            reedit_judge_payloads,
            reedit_errors,
        ) = await _run_reedit_cases(
            run_id=run_id,
            selected_cases=selected_reedit_cases,
            llm=router_llm,
            registry=stub_registry,
        )

    stub_metrics = _aggregate_deterministic_metrics(stub_case_reports)
    stub_metrics["execution_errors"] = stub_execution_errors
    stub_metrics["deterministic_overall_score"] = round(
        sum(item.deterministic_score for item in stub_case_reports) / len(stub_case_reports)
        if stub_case_reports
        else 0.0,
        4,
    )

    real_metrics = _aggregate_deterministic_metrics(real_case_reports)
    real_metrics["execution_errors"] = real_execution_errors
    real_metrics["deterministic_overall_score"] = round(
        sum(item.deterministic_score for item in real_case_reports) / len(real_case_reports)
        if real_case_reports
        else 0.0,
        4,
    )

    orchestrator_metrics = _aggregate_deterministic_metrics(case_reports)
    orchestrator_metrics["execution_errors"] = execution_errors
    orchestrator_metrics["deterministic_overall_score"] = round(
        sum(item.deterministic_score for item in case_reports) / len(case_reports)
        if case_reports
        else 0.0,
        4,
    )

    reedit_metrics = _aggregate_reedit_metrics(reedit_case_reports)
    reedit_metrics["execution_errors"] = reedit_errors
    reedit_metrics["deterministic_overall_score"] = round(
        sum(item.deterministic_score for item in reedit_case_reports) / len(reedit_case_reports)
        if reedit_case_reports
        else 0.0,
        4,
    )

    all_case_reports: list[AdvisorOrchestratorCaseReport | AdvisorReeditCaseReport] = [
        *case_reports,
        *reedit_case_reports,
    ]
    judge_case_payloads_all = [*judge_case_payloads, *reedit_judge_payloads]

    judge_summary: dict[str, Any] = {}
    judge_report: dict[str, Any] = {}
    judge_report_reedit: dict[str, Any] = {"case_results": []}
    judge_tokens = {
        "calls": 0,
        "errors": 0,
        "tokens": 0,
        "p95_latency_ms": 0.0,
        "rate_limit_errors": 0,
    }
    polish_tokens = {
        "calls": 0,
        "errors": 0,
        "tokens": 0,
        "p95_latency_ms": 0.0,
        "rate_limit_errors": 0,
    }

    if judge_enabled:
        judge = AdvisorOrchestratorJudge(
            llm=judge_llm,
            max_rpm_total=max_rpm_total,
            concurrency=judge_concurrency,
            temperature=judge_temperature,
            max_tokens=judge_max_tokens,
        )
        judge_eval_id = f"{run_id}-cases-judge"
        run_eval_id = f"{run_id}-run-judge"
        pass_result = await judge.evaluate_cases(
            pass_name="advisor_orchestrator_merged",
            eval_run_id=judge_eval_id,
            case_payloads=judge_case_payloads_all,
            run_metadata={
                "orchestrator_dataset_id": dataset.dataset_id,
                "orchestrator_version": dataset.version,
                "reedit_dataset_id": reedit_dataset.dataset_id,
                "reedit_version": reedit_dataset.version,
                "include_reedit": include_reedit,
            },
        )
        judge_report = pass_result.to_dict()
        reedit_case_ids = {item.case_id for item in reedit_case_reports}
        judge_report_reedit = {
            "case_results": [
                item
                for item in judge_report.get("case_results", [])
                if str(item.get("case_id", "")) in reedit_case_ids
            ]
        }
        judge_case_index = {item.case_id: item for item in pass_result.case_results}

        for report in case_reports:
            judged = judge_case_index.get(report.case_id)
            if judged is None:
                continue
            report.judge_score = round(float(judged.case_score), 4)
            report.final_score = round(
                0.6 * float(report.deterministic_score) + 0.4 * float(report.judge_score),
                4,
            )

        for report in reedit_case_reports:
            judged = judge_case_index.get(report.case_id)
            if judged is None:
                continue
            report.judge_score = round(float(judged.case_score), 4)
            report.final_score = round(
                0.6 * float(report.deterministic_score) + 0.4 * float(report.judge_score),
                4,
            )

        merged_metrics = _build_merged_metrics(
            orchestrator_metrics=orchestrator_metrics,
            reedit_metrics=reedit_metrics,
            include_reedit=include_reedit,
            all_case_reports=all_case_reports,
        )

        run_summary = await judge.evaluate_run(
            run_id=run_id,
            eval_run_id=run_eval_id,
            pass_summary={
                "avg_case_score": pass_result.avg_case_score,
                "case_count": pass_result.case_count,
                "orchestrator_case_count": len(case_reports),
                "reedit_case_count": len(reedit_case_reports),
            },
            aggregate_metrics={
                "orchestrator_metrics": orchestrator_metrics,
                "reedit_metrics": reedit_metrics,
                "merged_metrics": merged_metrics,
            },
        )
        judge_summary = run_summary.to_dict()
        judge_tokens = await _collect_token_usage(
            eval_run_id=run_id,
            caller_prefixes=("eval.advisor.orchestrator.judge.",),
            enabled=usage_enabled,
        )
    else:
        for report in case_reports:
            report.judge_score = 0.0
            report.final_score = report.deterministic_score
        for report in reedit_case_reports:
            report.judge_score = 0.0
            report.final_score = report.deterministic_score

    merged_metrics = _build_merged_metrics(
        orchestrator_metrics=orchestrator_metrics,
        reedit_metrics=reedit_metrics,
        include_reedit=include_reedit,
        all_case_reports=all_case_reports,
    )
    polish_tokens = await _collect_token_usage(
        eval_run_id=run_id,
        caller_prefixes=("advisor.style.",),
        enabled=usage_enabled,
    )
    render_metrics = _collect_complex_output_render_metrics(case_reports)
    merged_metrics["judge_enabled"] = judge_enabled
    merged_metrics["judge_overall_score"] = float(judge_summary.get("overall_score", 0.0) or 0.0)
    merged_metrics["judge_status"] = str(judge_summary.get("status", "n/a"))
    merged_metrics["tokens_actual_judge"] = int(judge_tokens.get("tokens", 0) or 0)
    merged_metrics["judge_calls"] = int(judge_tokens.get("calls", 0) or 0)
    merged_metrics["judge_errors"] = int(judge_tokens.get("errors", 0) or 0)
    merged_metrics["judge_rate_limit_errors"] = int(judge_tokens.get("rate_limit_errors", 0) or 0)
    merged_metrics["judge_p95_latency_ms"] = float(judge_tokens.get("p95_latency_ms", 0.0) or 0.0)
    merged_metrics["judge_latency_total_ms"] = int(judge_tokens.get("latency_total_ms", 0) or 0)
    judge_task_count = (len(case_reports) + 1) if judge_enabled else 0
    merged_metrics["judge_task_count"] = judge_task_count
    merged_metrics["judge_tokens_per_task"] = round(
        (int(judge_tokens.get("tokens", 0) or 0) / judge_task_count),
        2,
    ) if judge_task_count > 0 else 0.0
    merged_metrics["judge_latency_avg_ms_per_task"] = round(
        (int(judge_tokens.get("latency_total_ms", 0) or 0) / judge_task_count),
        2,
    ) if judge_task_count > 0 else 0.0
    merged_metrics["complex_output_polish_calls"] = int(polish_tokens.get("calls", 0) or 0)
    merged_metrics["complex_output_polish_errors"] = int(polish_tokens.get("errors", 0) or 0)
    merged_metrics["complex_output_polish_tokens"] = int(polish_tokens.get("tokens", 0) or 0)
    merged_metrics["complex_output_render_pass_rate"] = float(
        render_metrics.get("complex_output_render_pass_rate", 0.0) or 0.0
    )
    merged_metrics["complex_output_render_total"] = int(
        render_metrics.get("complex_output_render_total", 0) or 0
    )
    merged_metrics["complex_output_render_pass_count"] = int(
        render_metrics.get("complex_output_render_pass_count", 0) or 0
    )
    merged_metrics["execution_lane"] = lane
    merged_metrics["stub_case_count"] = len(stub_case_reports)
    merged_metrics["real_case_count"] = len(real_case_reports)
    warning_counts = _warning_counts_by_stage(
        [*stub_execution_errors, *real_execution_errors, *reedit_errors]
    )
    merged_metrics["warning_counts_by_stage"] = warning_counts
    merged_metrics["warning_count_total"] = sum(warning_counts.values())

    gate_by_lane: dict[str, Any] = {}
    if lane in {"stub", "both"}:
        stub_gate_metrics = _build_merged_metrics(
            orchestrator_metrics=stub_metrics,
            reedit_metrics=reedit_metrics,
            include_reedit=include_reedit,
            all_case_reports=[*stub_case_reports, *reedit_case_reports],
        )
        gate_by_lane["stub"] = _lane_gate(stub_gate_metrics, include_reedit=include_reedit)
    if lane in {"real", "both"}:
        real_gate_metrics = _build_merged_metrics(
            orchestrator_metrics=real_metrics,
            reedit_metrics=reedit_metrics,
            include_reedit=include_reedit,
            all_case_reports=[*real_case_reports, *reedit_case_reports],
        )
        gate_by_lane["real"] = _lane_gate(real_gate_metrics, include_reedit=include_reedit)

    strict_thresholds = _merged_thresholds(
        orchestrator_thresholds=dataset.thresholds,
        reedit_thresholds=reedit_dataset.thresholds,
        include_reedit=include_reedit,
    )
    status = _grade_status(
        metrics=merged_metrics,
        strict_thresholds=strict_thresholds,
        include_reedit=include_reedit,
    )
    if warning_gate:
        lane_failed = any(not bool(item.get("passed", False)) for item in gate_by_lane.values())
        if lane_failed or int(merged_metrics.get("warning_count_total", 0) or 0) > 0:
            status = "bad"
    recommendations = _build_recommendations(
        metrics=merged_metrics,
        status=status,
        strict_thresholds=strict_thresholds,
        judge_enabled=judge_enabled,
        include_reedit=include_reedit,
    )
    if warning_gate and any(not bool(item.get("passed", False)) for item in gate_by_lane.values()):
        recommendations.append("Lane gate failed: inspect `gate_by_lane` and `warning_counts_by_stage`.")

    report = AdvisorOrchestratorEvalReport(
        run_id=run_id,
        generated_at=now,
        config={
            "dataset_id": dataset.dataset_id,
            "dataset_version": dataset.version,
            "dataset_path": str(Path(dataset_path) if dataset_path is not None else DEFAULT_DATASET_PATH),
            "reedit_dataset_id": reedit_dataset.dataset_id,
            "reedit_dataset_version": reedit_dataset.version,
            "reedit_dataset_path": str(
                Path(reedit_dataset_path)
                if reedit_dataset_path is not None
                else DEFAULT_REEDIT_DATASET_PATH
            ),
            "output_dir": str(run_dir),
            "judge_enabled": judge_enabled,
            "judge_concurrency": judge_concurrency,
            "judge_temperature": judge_temperature,
            "judge_max_tokens": judge_max_tokens,
            "max_rpm_total": max_rpm_total,
            "execution_lane": lane,
            "real_capabilities": list(selected_real_capabilities),
            "warning_gate": warning_gate,
            "usage_enabled": usage_enabled,
            "sample_size": sample_size,
            "case_ids": case_ids or [],
            "selected_case_ids": [item.case_id for item in selected_cases],
            "reedit_sample_size": reedit_sample_size,
            "reedit_case_ids": reedit_case_ids or [],
            "include_reedit": include_reedit,
            "selected_reedit_case_ids": [item.case_id for item in selected_reedit_cases],
            "strict_thresholds": strict_thresholds,
            "router_model": settings.ZAI_MODEL,
        },
        orchestrator_metrics=orchestrator_metrics,
        reedit_metrics=reedit_metrics,
        merged_metrics=merged_metrics,
        metrics=merged_metrics,
        status=status,
        recommendations=recommendations,
        stub_metrics=stub_metrics,
        real_metrics=real_metrics,
        gate_by_lane=gate_by_lane,
        warning_counts_by_stage=warning_counts,
        judge_summary=judge_summary,
        cases=case_reports,
        reedit_cases=reedit_case_reports,
    )

    write_json(run_dir / "report.json", report.to_dict())
    write_json(run_dir / "judge_cases.json", judge_report)
    write_json(run_dir / "judge_cases_reedit.json", judge_report_reedit)
    write_json(run_dir / "judge_summary.json", judge_summary)
    write_json(
        run_dir / "merged_summary.json",
        {
            "run_id": run_id,
            "status": status,
            "gate_by_lane": gate_by_lane,
            "warning_counts_by_stage": warning_counts,
            "orchestrator_metrics": orchestrator_metrics,
            "stub_metrics": stub_metrics,
            "real_metrics": real_metrics,
            "reedit_metrics": reedit_metrics,
            "merged_metrics": merged_metrics,
            "recommendations": recommendations,
        },
    )
    write_cases_jsonl(run_dir / "cases.jsonl", case_reports)
    write_cases_jsonl(run_dir / "reedit_cases.jsonl", reedit_case_reports)
    write_markdown_summary(run_dir / "summary.md", report)
    append_history(out_root / "history.csv", report)

    return report


async def _run_orchestrator_cases(
    *,
    run_id: str,
    selected_cases: list[AdvisorOrchestratorEvalCase],
    llm: LLMClient,
    registry: CapabilityRegistry,
    usage_enabled: bool,
) -> tuple[list[AdvisorOrchestratorCaseReport], list[dict[str, Any]], list[dict[str, Any]]]:
    case_reports: list[AdvisorOrchestratorCaseReport] = []
    judge_case_payloads: list[dict[str, Any]] = []
    execution_errors: list[dict[str, Any]] = []

    for case in selected_cases:
        session_id = f"{run_id}:{case.case_id}"
        request_payload = dict(case.request)
        request_payload["session_id"] = session_id
        request_payload.setdefault("student_id", _EVAL_STUDENT_UUID)
        request_payload.setdefault("turn_id", f"{case.case_id}-turn")
        request_payload.setdefault("locale", "zh-CN")

        try:
            request = AdvisorRequest.model_validate(request_payload)
        except Exception as exc:
            report = AdvisorOrchestratorCaseReport(
                case_id=case.case_id,
                tags=case.tags,
                request=request_payload,
                response={},
                deterministic_checks={"request_valid": False},
                deterministic_score=0.0,
                final_score=0.0,
                errors=[{"stage": "request_validate", "error": str(exc)}],
            )
            case_reports.append(report)
            execution_errors.append({"case_id": case.case_id, "stage": "request_validate", "error": str(exc)})
            continue

        orchestrator = EvalAdvisorOrchestrator(
            llm=llm,
            registry=registry,
            context_profile=case.context_profile,
            seed=case.seed,
        )
        response: AdvisorResponse | None = None
        case_errors: list[dict[str, Any]] = []
        suffix_token = None
        if hasattr(llm, "set_caller_suffix"):
            try:
                suffix_token = llm.set_caller_suffix(f"{run_id}:{case.case_id}")
            except Exception:
                suffix_token = None
        try:
            response = await orchestrator.process(request)
        except Exception as exc:
            case_errors.append({"stage": "process", "error": str(exc)})
            execution_errors.append({"case_id": case.case_id, "stage": "process", "error": str(exc)})
            response = _runtime_error_response(
                request=request,
                message=f"Runtime failure in eval case: {exc}",
            )
        finally:
            if suffix_token is not None and hasattr(llm, "reset_caller_suffix"):
                try:
                    llm.reset_caller_suffix(suffix_token)
                except Exception as exc:
                    logger.warning(
                        "Failed to reset eval caller suffix: case=%s",
                        case.case_id,
                        exc_info=exc,
                    )

        checks, score = _evaluate_deterministic(case=case, response=response)
        usage_stats = await _collect_token_usage(
            eval_run_id=f"{run_id}:{case.case_id}",
            enabled=usage_enabled,
        )
        _attach_task_usage_metrics(checks=checks, response=response, usage_stats=usage_stats)
        case_report = AdvisorOrchestratorCaseReport(
            case_id=case.case_id,
            tags=case.tags,
            request=request.model_dump(mode="json"),
            response=response.model_dump(mode="json"),
            deterministic_checks=checks,
            deterministic_score=score,
            final_score=score,
            errors=case_errors,
        )
        case_reports.append(case_report)
        judge_case_payloads.append(
            {
                "case_id": case.case_id,
                "case_type": "orchestrator",
                "tags": case.tags,
                "expect": case.expect,
                "deterministic_checks": checks,
                "response": {
                    "domain": response.domain,
                    "capability": response.capability,
                    "done": [item.model_dump(mode="json") for item in response.done],
                    "pending": [item.model_dump(mode="json") for item in response.pending],
                    "next_actions": [item.model_dump(mode="json") for item in response.next_actions],
                    "error": response.error.model_dump(mode="json") if response.error else None,
                    "route_meta": response.route_meta.model_dump(mode="json"),
                },
            }
        )

    return case_reports, judge_case_payloads, execution_errors


async def _run_orchestrator_cases_real(
    *,
    run_id: str,
    selected_cases: list[AdvisorOrchestratorEvalCase],
    llm: LLMClient,
    capabilities: tuple[str, ...],
    usage_enabled: bool,
) -> tuple[list[AdvisorOrchestratorCaseReport], list[dict[str, Any]], list[dict[str, Any]]]:
    case_reports: list[AdvisorOrchestratorCaseReport] = []
    judge_case_payloads: list[dict[str, Any]] = []
    execution_errors: list[dict[str, Any]] = []

    try:
        from scholarpath.db.session import async_session_factory
    except Exception as exc:
        execution_errors.append(
            {"stage": "real_lane_precondition", "error": f"db_session_unavailable: {exc}"}
        )
        return case_reports, judge_case_payloads, execution_errors

    try:
        registry = _build_real_registry(capabilities=capabilities)
    except Exception as exc:
        execution_errors.append(
            {"stage": "real_lane_precondition", "error": f"real_registry_build_failed: {exc}"}
        )
        return case_reports, judge_case_payloads, execution_errors

    if not registry.list_capability_ids():
        execution_errors.append(
            {
                "stage": "real_lane_precondition",
                "error": "real_registry_empty_after_capability_filter",
            }
        )
        return case_reports, judge_case_payloads, execution_errors

    for case in selected_cases:
        session_id = f"{run_id}:real:{case.case_id}"
        request_payload = dict(case.request)
        request_payload["session_id"] = session_id
        request_payload.setdefault("student_id", _EVAL_STUDENT_UUID)
        request_payload.setdefault("turn_id", f"{case.case_id}-turn")
        request_payload.setdefault("locale", "zh-CN")

        try:
            request = AdvisorRequest.model_validate(request_payload)
        except Exception as exc:
            report = AdvisorOrchestratorCaseReport(
                case_id=case.case_id,
                tags=case.tags,
                request=request_payload,
                response={},
                deterministic_checks={"request_valid": False},
                deterministic_score=0.0,
                final_score=0.0,
                errors=[{"stage": "request_validate", "error": str(exc)}],
            )
            case_reports.append(report)
            execution_errors.append({"case_id": case.case_id, "stage": "request_validate", "error": str(exc)})
            continue

        response: AdvisorResponse | None = None
        case_errors: list[dict[str, Any]] = []
        suffix_token = None
        if hasattr(llm, "set_caller_suffix"):
            try:
                suffix_token = llm.set_caller_suffix(f"{run_id}:real:{case.case_id}")
            except Exception as exc:
                logger.warning("Failed to set real-lane caller suffix: case=%s", case.case_id, exc_info=exc)
                suffix_token = None

        try:
            async with async_session_factory() as session:
                redis = _EvalRedis()
                runtime = RealLaneRuntime(
                    session=session,
                    redis=redis,  # type: ignore[arg-type]
                    registry=registry,
                )
                orchestrator = AdvisorOrchestrator(
                    llm=llm,
                    session=session,
                    redis=redis,  # type: ignore[arg-type]
                    registry=registry,
                    runtime=runtime,
                )
                response = await orchestrator.process(request)
                await session.rollback()
        except Exception as exc:
            case_errors.append({"stage": "process", "error": str(exc)})
            execution_errors.append({"case_id": case.case_id, "stage": "process", "error": str(exc)})
            response = _runtime_error_response(
                request=request,
                message=f"Real-lane runtime failure in eval case: {exc}",
            )
        finally:
            if suffix_token is not None and hasattr(llm, "reset_caller_suffix"):
                try:
                    llm.reset_caller_suffix(suffix_token)
                except Exception as exc:
                    logger.warning(
                        "Failed to reset real-lane caller suffix: case=%s",
                        case.case_id,
                        exc_info=exc,
                    )

        checks, score = _evaluate_deterministic(case=case, response=response)
        usage_stats = await _collect_token_usage(
            eval_run_id=f"{run_id}:real:{case.case_id}",
            enabled=usage_enabled,
        )
        _attach_task_usage_metrics(checks=checks, response=response, usage_stats=usage_stats)
        case_report = AdvisorOrchestratorCaseReport(
            case_id=case.case_id,
            tags=case.tags,
            request=request.model_dump(mode="json"),
            response=response.model_dump(mode="json"),
            deterministic_checks=checks,
            deterministic_score=score,
            final_score=score,
            errors=case_errors,
        )
        case_reports.append(case_report)
        judge_case_payloads.append(
            {
                "case_id": case.case_id,
                "case_type": "orchestrator_real",
                "tags": case.tags,
                "expect": case.expect,
                "deterministic_checks": checks,
                "response": {
                    "domain": response.domain,
                    "capability": response.capability,
                    "done": [item.model_dump(mode="json") for item in response.done],
                    "pending": [item.model_dump(mode="json") for item in response.pending],
                    "next_actions": [item.model_dump(mode="json") for item in response.next_actions],
                    "error": response.error.model_dump(mode="json") if response.error else None,
                    "route_meta": response.route_meta.model_dump(mode="json"),
                },
            }
        )

    return case_reports, judge_case_payloads, execution_errors


async def _run_reedit_cases(
    *,
    run_id: str,
    selected_cases: list[AdvisorReeditEvalCase],
    llm: LLMClient,
    registry: CapabilityRegistry,
) -> tuple[list[AdvisorReeditCaseReport], list[dict[str, Any]], list[dict[str, Any]]]:
    from scholarpath.api.routes.advisor import _apply_edit_overwrite, _load_db_history_entries

    case_reports: list[AdvisorReeditCaseReport] = []
    judge_case_payloads: list[dict[str, Any]] = []
    execution_errors: list[dict[str, Any]] = []

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        for case in selected_cases:
            session_id = f"{run_id}:reedit:{case.case_id}"
            request_payload = dict(case.request)
            request_payload["session_id"] = session_id
            request_payload.setdefault("student_id", _EVAL_STUDENT_UUID)
            request_payload.setdefault("turn_id", f"{case.case_id}-run")
            request_payload.setdefault("locale", "zh-CN")

            case_errors: list[dict[str, Any]] = []
            async with factory() as session:
                redis = _EvalRedis()
                memory = ChatMemory(redis)
                try:
                    await _seed_reedit_timeline(
                        session=session,
                        memory=memory,
                        session_id=session_id,
                        seed=case.seed,
                    )
                    before_rows = await _load_session_timeline(session=session, session_id=session_id)

                    try:
                        request = AdvisorRequest.model_validate(request_payload)
                    except Exception as exc:
                        response = _invalid_input_response(
                            request_payload=request_payload,
                            message=f"Invalid re-edit request payload: {exc}",
                        )
                        checks, score = _evaluate_reedit_deterministic(
                            case=case,
                            request_payload=request_payload,
                            response=response,
                            rewrite_error=str(exc),
                            before_rows=before_rows,
                            after_rows=before_rows,
                            history_entries=[],
                        )
                        report = AdvisorReeditCaseReport(
                            case_id=case.case_id,
                            tags=case.tags,
                            request=request_payload,
                            response=response.model_dump(mode="json"),
                            deterministic_checks=checks,
                            deterministic_score=score,
                            final_score=score,
                            errors=[{"stage": "request_validate", "error": str(exc)}],
                        )
                        case_reports.append(report)
                        execution_errors.append({"case_id": case.case_id, "stage": "request_validate", "error": str(exc)})
                        await session.rollback()
                        continue

                    rewritten, rewrite_error = await _apply_edit_overwrite(
                        session=session,
                        redis=redis,
                        request=request,
                    )

                    response: AdvisorResponse
                    if rewrite_error is not None:
                        response = _invalid_input_response(
                            request_payload=request.model_dump(mode="json"),
                            message=str(rewrite_error),
                        )
                    else:
                        exec_seed = case.seed.get("orchestrator_seed")
                        if not isinstance(exec_seed, dict):
                            exec_seed = {}
                        orchestrator = EvalAdvisorOrchestrator(
                            llm=llm,
                            registry=registry,
                            context_profile="normal",
                            seed=exec_seed,
                        )
                        response = await orchestrator.process(rewritten)
                        await persist_turn_message(
                            session=session,
                            event=MemoryIngestEvent(
                                turn_id=response.turn_id,
                                session_id=rewritten.session_id,
                                student_id=rewritten.student_id,
                                domain=response.domain,
                                capability=response.capability,
                                role="assistant",
                                content=response.assistant_text,
                                artifacts=[item.model_dump(mode="json") for item in response.artifacts],
                                done=response.done,
                                pending=response.pending,
                                next_actions=response.next_actions,
                            ),
                        )

                    await session.flush()
                    after_rows = await _load_session_timeline(session=session, session_id=session_id)
                    history_entries = await _load_db_history_entries(session=session, session_id=session_id)
                    await session.commit()

                    checks, score = _evaluate_reedit_deterministic(
                        case=case,
                        request_payload=request_payload,
                        response=response,
                        rewrite_error=rewrite_error,
                        before_rows=before_rows,
                        after_rows=after_rows,
                        history_entries=history_entries,
                    )

                    report = AdvisorReeditCaseReport(
                        case_id=case.case_id,
                        tags=case.tags,
                        request=request_payload,
                        response=response.model_dump(mode="json"),
                        deterministic_checks=checks,
                        deterministic_score=score,
                        final_score=score,
                        errors=case_errors,
                    )
                    case_reports.append(report)
                    judge_case_payloads.append(
                        {
                            "case_id": case.case_id,
                            "case_type": "reedit",
                            "tags": case.tags,
                            "expect": case.expect,
                            "deterministic_checks": checks,
                            "response": {
                                "domain": response.domain,
                                "capability": response.capability,
                                "done": [item.model_dump(mode="json") for item in response.done],
                                "pending": [item.model_dump(mode="json") for item in response.pending],
                                "next_actions": [item.model_dump(mode="json") for item in response.next_actions],
                                "error": response.error.model_dump(mode="json") if response.error else None,
                                "timeline_before": before_rows,
                                "timeline_after": after_rows,
                            },
                        }
                    )
                except Exception as exc:
                    await session.rollback()
                    case_errors.append({"stage": "reedit_process", "error": str(exc)})
                    execution_errors.append({"case_id": case.case_id, "stage": "reedit_process", "error": str(exc)})
                    response = _runtime_error_response(
                        request=AdvisorRequest.model_validate(
                            {
                                "session_id": session_id,
                                "message": request_payload.get("message") or "reedit",
                                "turn_id": request_payload.get("turn_id") or f"{case.case_id}-error",
                            }
                        ),
                        message=f"Re-edit runtime failure in eval case: {exc}",
                    )
                    report = AdvisorReeditCaseReport(
                        case_id=case.case_id,
                        tags=case.tags,
                        request=request_payload,
                        response=response.model_dump(mode="json"),
                        deterministic_checks={"runtime_error": True},
                        deterministic_score=0.0,
                        final_score=0.0,
                        errors=case_errors,
                    )
                    case_reports.append(report)
    finally:
        await engine.dispose()

    return case_reports, judge_case_payloads, execution_errors


async def _seed_reedit_timeline(
    *,
    session: AsyncSession,
    memory: ChatMemory,
    session_id: str,
    seed: dict[str, Any],
) -> None:
    timeline = seed.get("timeline", [])
    if not isinstance(timeline, list):
        raise ValueError("reedit seed.timeline must be list")
    base = datetime.now(tz=UTC)

    rows: list[AdvisorMessage] = []
    for idx, item in enumerate(timeline):
        if not isinstance(item, dict):
            continue
        turn_id = str(item.get("turn_id", "")).strip()
        role = str(item.get("role", "")).strip().lower()
        content = str(item.get("content", "")).strip()
        if not turn_id or role not in {"user", "assistant"} or not content:
            continue
        row = AdvisorMessage(
            turn_id=turn_id,
            session_id=session_id,
            student_id=None,
            role=role,
            domain=str(item.get("domain") or "undergrad"),
            capability=str(item.get("capability") or "undergrad.school.recommend"),
            content=content,
            artifacts_json=[],
            done_json=[],
            pending_json=[],
            next_actions_json=[],
            ingestion_status="ready",
            created_at=base + timedelta(seconds=idx + 1),
            updated_at=base + timedelta(seconds=idx + 1),
        )
        session.add(row)
        rows.append(row)

    await session.flush()

    for idx, row in enumerate(rows):
        session.add(
            AdvisorMessageChunk(
                message_id=row.id,
                turn_id=row.turn_id,
                session_id=row.session_id,
                student_id=None,
                domain=row.domain,
                chunk_index=0,
                content=f"chunk:{row.role}:{row.content}",
                token_count=16,
                score_meta={"source": "eval_seed", "idx": idx},
                embedding=None,
            )
        )
        session.add(
            AdvisorMemoryItem(
                session_id=session_id,
                student_id=None,
                domain=row.domain,
                scope="session",
                item_type="decision",
                item_key=f"seed:{idx}:{row.turn_id}:{row.role}",
                item_value={"content": row.content},
                confidence=0.9,
                status="active",
                source_turn_id=row.turn_id,
            )
        )
        await memory.save_message(session_id, row.role, row.content)

    await session.flush()


async def _load_session_timeline(
    *,
    session: AsyncSession,
    session_id: str,
) -> list[dict[str, Any]]:
    rows = (
        await session.execute(
            select(AdvisorMessage)
            .where(AdvisorMessage.session_id == session_id)
            .order_by(AdvisorMessage.created_at.asc(), AdvisorMessage.id.asc())
        )
    ).scalars().all()
    return [
        {
            "turn_id": row.turn_id,
            "role": row.role,
            "content": row.content,
        }
        for row in rows
    ]


def _invalid_input_response(*, request_payload: dict[str, Any], message: str) -> AdvisorResponse:
    turn_id = str(request_payload.get("turn_id") or uuid.uuid4())
    return AdvisorResponse(
        turn_id=turn_id,
        domain="common",
        capability="common.general",
        assistant_text=message,
        artifacts=[],
        actions=[],
        done=[],
        pending=[],
        next_actions=[
            AdvisorAction(
                action_id="route.clarify",
                label="先澄清目标",
                payload={"client_context": {"trigger": "route.clarify"}},
            )
        ],
        route_meta=AdvisorRouteMeta(
            domain_confidence=0.0,
            capability_confidence=0.0,
            router_model=settings.ZAI_MODEL,
            latency_ms=0,
            fallback_used=True,
            guard_result="invalid_input",
            guard_reason="invalid_input",
            context_tokens=0,
            memory_hits=0,
            rag_hits=0,
            rag_latency_ms=0,
            memory_degraded=True,
        ),
        error={
            "code": "INVALID_INPUT",
            "message": message,
            "retriable": False,
        },
    )


def _runtime_error_response(*, request: AdvisorRequest, message: str) -> AdvisorResponse:
    return AdvisorResponse(
        turn_id=request.turn_id or str(uuid.uuid4()),
        domain="common",
        capability="common.clarify",
        assistant_text=message,
        artifacts=[],
        actions=[],
        done=[],
        pending=[],
        next_actions=[],
        route_meta=AdvisorRouteMeta(
            domain_confidence=0.0,
            capability_confidence=0.0,
            router_model=settings.ZAI_MODEL,
            latency_ms=0,
            fallback_used=True,
            context_tokens=0,
            memory_hits=0,
            rag_hits=0,
            rag_latency_ms=0,
            memory_degraded=True,
        ),
        error={
            "code": "CAPABILITY_FAILED",
            "message": message,
            "retriable": True,
        },
    )


def _evaluate_deterministic(
    *,
    case: AdvisorOrchestratorEvalCase,
    response: AdvisorResponse,
) -> tuple[dict[str, Any], float]:
    expect = case.expect
    checks: dict[str, Any] = {}
    applicable = 0
    passed = 0

    def register(name: str, value: bool | None) -> None:
        nonlocal applicable, passed
        checks[name] = value
        if value is None:
            return
        applicable += 1
        if value:
            passed += 1

    contract_ok = (
        isinstance(response.done, list)
        and isinstance(response.pending, list)
        and isinstance(response.next_actions, list)
        and response.assistant_text is not None
    )
    register("contract_ok", contract_ok)

    max_done = int(expect.get("max_done", 2))
    max_execution_ok = len(response.done) <= max_done
    register("max_execution_ok", max_execution_ok)
    checks["done_count"] = len(response.done)
    checks["pending_count"] = len(response.pending)

    primary_any = expect.get("primary_any")
    if isinstance(primary_any, list) and primary_any:
        primary_ok = response.capability in {str(item) for item in primary_any}
        register("primary_hit", primary_ok)
    else:
        register("primary_hit", None)

    if "must_clarify" in expect:
        must_clarify = bool(expect.get("must_clarify"))
        clarify_ok = (response.capability == "common.clarify") == must_clarify
        register("clarify_correct", clarify_ok)
        register("must_clarify_alignment", clarify_ok)
        checks["must_clarify_expected"] = must_clarify
        checks["clarify_predicted"] = response.capability == "common.clarify"
    else:
        register("clarify_correct", None)
        register("must_clarify_alignment", None)
        checks["must_clarify_expected"] = None
        checks["clarify_predicted"] = response.capability == "common.clarify"

    pending_contains = expect.get("pending_contains")
    if isinstance(pending_contains, list) and pending_contains:
        required_hits = 0
        for rule in pending_contains:
            if not isinstance(rule, dict):
                continue
            capability = str(rule.get("capability", "")).strip()
            if not capability:
                continue
            reason_any = {
                str(item).strip()
                for item in rule.get("reason_any", [])
                if str(item).strip()
            }
            matched = next((row for row in response.pending if row.capability == capability), None)
            if matched is None:
                continue
            if reason_any and matched.reason not in reason_any:
                continue
            required_hits += 1
        pending_ok = required_hits == len(
            [
                item
                for item in pending_contains
                if isinstance(item, dict) and str(item.get("capability", "")).strip()
            ]
        )
        register("pending_reason_ok", pending_ok)
    else:
        pending_reason_any = expect.get("pending_reason_any")
        if isinstance(pending_reason_any, list) and pending_reason_any:
            allowed = {str(item).strip() for item in pending_reason_any if str(item).strip()}
            pending_ok = any(step.reason in allowed for step in response.pending)
            register("pending_reason_ok", pending_ok)
        else:
            register("pending_reason_ok", None)

    if "error_code_any" in expect:
        allowed = {
            str(item).strip()
            for item in expect.get("error_code_any", [])
            if str(item).strip()
        }
        error_ok = response.error is not None and response.error.code in allowed
        register("error_expectation_ok", error_ok)
    else:
        register("error_expectation_ok", None)

    if response.error is not None:
        error_contract_ok = bool(str(response.error.code).strip() and str(response.error.message).strip())
        register("error_contract_ok", error_contract_ok)
    else:
        register("error_contract_ok", None)

    recoverable_actions_any = expect.get("recoverable_actions_any")
    if isinstance(recoverable_actions_any, list) and recoverable_actions_any:
        allowed_actions = {
            str(item).strip()
            for item in recoverable_actions_any
            if str(item).strip()
        }
        response_actions = {row.action_id for row in response.next_actions}
        recoverable_ok = bool(response_actions.intersection(allowed_actions))
        register("recoverable_ok", recoverable_ok)
    else:
        register("recoverable_ok", None)

    if "memory_degraded" in expect:
        expected_degraded = bool(expect.get("memory_degraded"))
        degraded_ok = response.route_meta.memory_degraded is expected_degraded
        register("memory_degraded_ok", degraded_ok)
    else:
        register("memory_degraded_ok", None)

    expected_deepsearch = expect.get("deepsearch")
    if isinstance(expected_deepsearch, dict):
        checks["deepsearch_expected_enabled"] = True
        observation = _extract_internal_deepsearch_observation(response)
        checks["deepsearch_observation"] = observation or {}
        checks["deepsearch_pair_id"] = str(expected_deepsearch.get("pair_id", "")).strip() or None
        checks["deepsearch_phase"] = str(expected_deepsearch.get("phase", "")).strip().lower() or None

        register("deepsearch_present_ok", observation is not None)

        expected_triggered = expected_deepsearch.get("triggered")
        if isinstance(expected_triggered, bool):
            triggered_ok = bool(observation) and bool(observation.get("triggered", False)) is expected_triggered
            register("deepsearch_trigger_ok", triggered_ok)
        else:
            register("deepsearch_trigger_ok", None)

        expected_reuse = expected_deepsearch.get("reuse_hit")
        if isinstance(expected_reuse, bool):
            reuse_ok = bool(observation) and bool(observation.get("reuse_hit", False)) is expected_reuse
            register("deepsearch_reuse_ok", reuse_ok)
        else:
            register("deepsearch_reuse_ok", None)

        db_min = _safe_float(expected_deepsearch.get("db_hit_ratio_min"))
        db_max = _safe_float(expected_deepsearch.get("db_hit_ratio_max"))
        if db_min is not None or db_max is not None:
            db_val = _safe_float((observation or {}).get("db_hit_ratio"))
            in_range = False
            if db_val is not None:
                low = db_min if db_min is not None else 0.0
                high = db_max if db_max is not None else 1.0
                in_range = low <= db_val <= high
            register("deepsearch_db_hit_range_ok", in_range)
        else:
            register("deepsearch_db_hit_range_ok", None)

        external_calls_max = _safe_int(expected_deepsearch.get("external_calls_max"))
        if external_calls_max is not None:
            ext_calls = _safe_int((observation or {}).get("external_calls"))
            register(
                "deepsearch_external_calls_ok",
                (ext_calls is not None) and (ext_calls <= external_calls_max),
            )
        else:
            register("deepsearch_external_calls_ok", None)

        checks["deepsearch_triggered"] = bool((observation or {}).get("triggered", False))
        checks["deepsearch_reuse_hit"] = bool((observation or {}).get("reuse_hit", False))
        checks["deepsearch_db_hit_ratio"] = _safe_float((observation or {}).get("db_hit_ratio")) or 0.0
        checks["deepsearch_external_calls"] = _safe_int((observation or {}).get("external_calls")) or 0
    else:
        checks["deepsearch_expected_enabled"] = False
        checks["deepsearch_pair_id"] = None
        checks["deepsearch_phase"] = None
        checks["deepsearch_triggered"] = False
        checks["deepsearch_reuse_hit"] = False
        checks["deepsearch_db_hit_ratio"] = 0.0
        checks["deepsearch_external_calls"] = 0

    score = round((passed / applicable) * 100.0, 4) if applicable > 0 else 0.0
    checks["applicable_checks"] = applicable
    checks["passed_checks"] = passed
    return checks, score


def _extract_internal_deepsearch_observation(response: AdvisorResponse) -> dict[str, Any] | None:
    for artifact in response.artifacts:
        payload: dict[str, Any] | None = None
        if hasattr(artifact, "model_dump"):
            try:
                payload = artifact.model_dump(mode="json")  # type: ignore[assignment]
            except Exception:
                payload = None
        elif isinstance(artifact, dict):
            payload = artifact
        if not isinstance(payload, dict):
            continue
        if str(payload.get("type", "")).strip() != "info_card":
            continue
        data = payload.get("data")
        if not isinstance(data, dict):
            continue
        deepsearch = data.get("internal_deepsearch")
        if isinstance(deepsearch, dict):
            return deepsearch
    return None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _attach_task_usage_metrics(
    *,
    checks: dict[str, Any],
    response: AdvisorResponse,
    usage_stats: dict[str, Any],
) -> None:
    task_count = max(len(response.done), 1)
    request_latency_ms = int(response.route_meta.latency_ms or 0)
    request_latency_per_task_ms = (
        round(request_latency_ms / task_count, 2) if request_latency_ms > 0 else 0.0
    )
    llm_tokens_total = int(usage_stats.get("tokens", 0) or 0)
    llm_calls_total = int(usage_stats.get("calls", 0) or 0)
    llm_latency_total_ms = int(usage_stats.get("latency_total_ms", 0) or 0)
    llm_tokens_per_task = round(llm_tokens_total / task_count, 2) if task_count > 0 else 0.0
    llm_latency_per_task_ms = (
        round(llm_latency_total_ms / task_count, 2) if task_count > 0 else 0.0
    )

    checks["task_count"] = task_count
    checks["request_latency_ms"] = request_latency_ms
    checks["request_latency_per_task_ms"] = request_latency_per_task_ms
    checks["llm_tokens_total"] = llm_tokens_total
    checks["llm_calls_total"] = llm_calls_total
    checks["llm_latency_total_ms"] = llm_latency_total_ms
    checks["llm_tokens_per_task"] = llm_tokens_per_task
    checks["llm_latency_per_task_ms"] = llm_latency_per_task_ms


def _evaluate_reedit_deterministic(
    *,
    case: AdvisorReeditEvalCase,
    request_payload: dict[str, Any],
    response: AdvisorResponse,
    rewrite_error: str | None,
    before_rows: list[dict[str, Any]],
    after_rows: list[dict[str, Any]],
    history_entries: list[dict[str, Any]],
) -> tuple[dict[str, Any], float]:
    expect = case.expect
    checks: dict[str, Any] = {}
    applicable = 0
    passed = 0

    def register(name: str, value: bool | None) -> None:
        nonlocal applicable, passed
        checks[name] = value
        if value is None:
            return
        applicable += 1
        if value:
            passed += 1

    edit_payload = request_payload.get("edit")
    target_turn = ""
    if isinstance(edit_payload, dict):
        target_turn = str(edit_payload.get("target_turn_id", "")).strip()

    actual_overwrite_success = rewrite_error is None

    before_index = None
    if target_turn:
        for idx, item in enumerate(before_rows):
            if item.get("turn_id") == target_turn and item.get("role") == "user":
                before_index = idx
                break

    if actual_overwrite_success and before_index is not None:
        edited_user = next(
            (
                item
                for item in after_rows
                if item.get("turn_id") == target_turn and item.get("role") == "user"
            ),
            None,
        )
        before_turns_after = {
            str(item.get("turn_id", "")).strip()
            for item in before_rows[before_index + 1 :]
            if str(item.get("turn_id", "")).strip()
            and str(item.get("turn_id", "")).strip() != target_turn
        }
        stale_turn_exists = any(
            str(item.get("turn_id", "")).strip() in before_turns_after
            for item in after_rows
        )
        actual_truncation = (
            edited_user is not None
            and str(edited_user.get("content", "")) == str(request_payload.get("message", ""))
            and not stale_turn_exists
        )
    else:
        actual_truncation = before_rows == after_rows

    if history_entries:
        history_role_content = [
            (str(row.get("role", "")), str(row.get("content", "")))
            for row in history_entries
        ]
        timeline_role_content = [
            (str(row.get("role", "")), str(row.get("content", "")))
            for row in after_rows
        ]
        editable_flags_ok = all(
            (entry.get("editable") is True) if entry.get("role") == "user" else (entry.get("editable") is False)
            for entry in history_entries
        )
        actual_history_consistent = history_role_content == timeline_role_content and editable_flags_ok
    else:
        actual_history_consistent = False

    contract_ok_actual = (
        isinstance(response.done, list)
        and isinstance(response.pending, list)
        and isinstance(response.next_actions, list)
        and response.assistant_text is not None
    )

    register(
        "overwrite_success",
        actual_overwrite_success is bool(expect.get("overwrite_success")),
    )
    register(
        "truncation_correct",
        actual_truncation is bool(expect.get("truncation_correct")),
    )
    register(
        "history_consistent",
        actual_history_consistent is bool(expect.get("history_consistent")),
    )
    register(
        "contract_ok",
        contract_ok_actual is bool(expect.get("contract_ok")),
    )

    capability_any = expect.get("capability_any")
    if isinstance(capability_any, list) and capability_any:
        register("capability_match", response.capability in {str(item) for item in capability_any})
    else:
        register("capability_match", None)

    checks["rewrite_error"] = rewrite_error
    checks["actual_overwrite_success"] = actual_overwrite_success
    checks["actual_truncation_correct"] = actual_truncation
    checks["actual_history_consistent"] = actual_history_consistent
    checks["actual_contract_ok"] = contract_ok_actual
    checks["before_timeline_size"] = len(before_rows)
    checks["after_timeline_size"] = len(after_rows)
    checks["history_size"] = len(history_entries)
    checks["applicable_checks"] = applicable
    checks["passed_checks"] = passed

    score = round((passed / applicable) * 100.0, 4) if applicable > 0 else 0.0
    return checks, score


def _aggregate_deterministic_metrics(
    case_reports: list[AdvisorOrchestratorCaseReport],
) -> dict[str, Any]:
    primary_total = primary_hits = 0
    clarify_total = clarify_hits = 0
    pending_total = pending_hits = 0
    recover_total = recover_hits = 0
    contract_total = contract_hits = 0
    error_total = error_hits = 0
    memory_total = memory_hits = 0
    execution_limit_violations = 0
    clarify_tp = clarify_fp = clarify_fn = 0
    non_causal_latencies_ms: list[int] = []
    non_causal_task_latencies_ms: list[float] = []
    task_latencies_ms: list[float] = []
    task_count_total = 0
    llm_tokens_total = 0
    llm_calls_total = 0
    llm_latency_total_ms = 0
    deepsearch_expect_total = deepsearch_expect_hits = 0
    deepsearch_trigger_total = deepsearch_trigger_hits = 0
    deepsearch_reuse_total = deepsearch_reuse_hits = 0
    deepsearch_db_range_total = deepsearch_db_range_hits = 0
    deepsearch_external_total = deepsearch_external_hits = 0
    deepsearch_case_db_hits: list[float] = []
    deepsearch_case_external_calls: list[int] = []
    deepsearch_triggered_count = 0
    deepsearch_pair_state: dict[str, dict[str, dict[str, float | int | bool]]] = {}

    for report in case_reports:
        checks = report.deterministic_checks
        if isinstance(checks.get("primary_hit"), bool):
            primary_total += 1
            primary_hits += int(bool(checks["primary_hit"]))
        if isinstance(checks.get("clarify_correct"), bool):
            clarify_total += 1
            clarify_hits += int(bool(checks["clarify_correct"]))
        expected_clarify = checks.get("must_clarify_expected")
        predicted_clarify = checks.get("clarify_predicted")
        if isinstance(expected_clarify, bool) and isinstance(predicted_clarify, bool):
            if predicted_clarify and expected_clarify:
                clarify_tp += 1
            elif predicted_clarify and not expected_clarify:
                clarify_fp += 1
            elif (not predicted_clarify) and expected_clarify:
                clarify_fn += 1
        if isinstance(checks.get("pending_reason_ok"), bool):
            pending_total += 1
            pending_hits += int(bool(checks["pending_reason_ok"]))
        if isinstance(checks.get("recoverable_ok"), bool):
            recover_total += 1
            recover_hits += int(bool(checks["recoverable_ok"]))
        if isinstance(checks.get("contract_ok"), bool):
            contract_total += 1
            contract_hits += int(bool(checks["contract_ok"]))
        if isinstance(checks.get("error_contract_ok"), bool):
            error_total += 1
            error_hits += int(bool(checks["error_contract_ok"]))
        if isinstance(checks.get("memory_degraded_ok"), bool):
            memory_total += 1
            memory_hits += int(bool(checks["memory_degraded_ok"]))
        if int(checks.get("done_count", 0) or 0) > 2:
            execution_limit_violations += 1
        task_count = int(checks.get("task_count", 0) or 0)
        if task_count > 0:
            task_count_total += task_count
        task_latency = float(checks.get("request_latency_per_task_ms", 0.0) or 0.0)
        if task_latency > 0:
            task_latencies_ms.append(task_latency)
        llm_tokens_total += int(checks.get("llm_tokens_total", 0) or 0)
        llm_calls_total += int(checks.get("llm_calls_total", 0) or 0)
        llm_latency_total_ms += int(checks.get("llm_latency_total_ms", 0) or 0)
        response = report.response if isinstance(report.response, dict) else {}
        done = response.get("done")
        done_caps: set[str] = set()
        if isinstance(done, list):
            for step in done:
                if not isinstance(step, dict):
                    continue
                cap = str(step.get("capability", "")).strip()
                if cap:
                    done_caps.add(cap)
        primary_cap = str(response.get("capability", "")).strip()
        is_non_causal = primary_cap in _NON_CAUSAL_CAPABILITIES or bool(
            done_caps & _NON_CAUSAL_CAPABILITIES
        )
        route_meta = response.get("route_meta") if isinstance(response.get("route_meta"), dict) else {}
        latency_raw = route_meta.get("latency_ms")
        latency_ms = int(latency_raw) if isinstance(latency_raw, (int, float)) else 0
        if is_non_causal and latency_ms > 0:
            non_causal_latencies_ms.append(latency_ms)
            if task_count > 0:
                non_causal_task_latencies_ms.append(round(latency_ms / task_count, 2))

        if bool(checks.get("deepsearch_expected_enabled", False)):
            deepsearch_case_db_hits.append(float(checks.get("deepsearch_db_hit_ratio", 0.0) or 0.0))
            deepsearch_case_external_calls.append(int(checks.get("deepsearch_external_calls", 0) or 0))
            deepsearch_triggered_count += int(bool(checks.get("deepsearch_triggered", False)))

            case_checks = [
                checks.get("deepsearch_present_ok"),
                checks.get("deepsearch_trigger_ok"),
                checks.get("deepsearch_reuse_ok"),
                checks.get("deepsearch_db_hit_range_ok"),
                checks.get("deepsearch_external_calls_ok"),
            ]
            applicable_case_checks = [item for item in case_checks if isinstance(item, bool)]
            if applicable_case_checks:
                deepsearch_expect_total += 1
                deepsearch_expect_hits += int(all(applicable_case_checks))

            if isinstance(checks.get("deepsearch_trigger_ok"), bool):
                deepsearch_trigger_total += 1
                deepsearch_trigger_hits += int(bool(checks["deepsearch_trigger_ok"]))
            if isinstance(checks.get("deepsearch_reuse_ok"), bool):
                deepsearch_reuse_total += 1
                deepsearch_reuse_hits += int(bool(checks["deepsearch_reuse_ok"]))
            if isinstance(checks.get("deepsearch_db_hit_range_ok"), bool):
                deepsearch_db_range_total += 1
                deepsearch_db_range_hits += int(bool(checks["deepsearch_db_hit_range_ok"]))
            if isinstance(checks.get("deepsearch_external_calls_ok"), bool):
                deepsearch_external_total += 1
                deepsearch_external_hits += int(bool(checks["deepsearch_external_calls_ok"]))

            pair_id = str(checks.get("deepsearch_pair_id", "") or "").strip()
            phase = str(checks.get("deepsearch_phase", "") or "").strip().lower()
            if pair_id and phase in {"cold", "warm"}:
                pair_bucket = deepsearch_pair_state.setdefault(pair_id, {})
                pair_bucket[phase] = {
                    "db_hit_ratio": float(checks.get("deepsearch_db_hit_ratio", 0.0) or 0.0),
                    "external_calls": int(checks.get("deepsearch_external_calls", 0) or 0),
                }

    deepsearch_pair_total = 0
    deepsearch_pair_uplift_hits = 0
    deepsearch_pair_external_reduction_hits = 0
    deepsearch_uplifts: list[float] = []
    for pair in deepsearch_pair_state.values():
        cold = pair.get("cold")
        warm = pair.get("warm")
        if not isinstance(cold, dict) or not isinstance(warm, dict):
            continue
        deepsearch_pair_total += 1
        cold_db = float(cold.get("db_hit_ratio", 0.0) or 0.0)
        warm_db = float(warm.get("db_hit_ratio", 0.0) or 0.0)
        cold_calls = int(cold.get("external_calls", 0) or 0)
        warm_calls = int(warm.get("external_calls", 0) or 0)
        uplift = round(warm_db - cold_db, 6)
        deepsearch_uplifts.append(uplift)
        if warm_db > cold_db:
            deepsearch_pair_uplift_hits += 1
        if warm_calls <= cold_calls:
            deepsearch_pair_external_reduction_hits += 1

    return {
        "case_count": len(case_reports),
        "primary_hit_rate": _ratio(primary_hits, primary_total),
        "clarify_correct_rate": _ratio(clarify_hits, clarify_total),
        "clarify_precision": _ratio(clarify_tp, clarify_tp + clarify_fp),
        "clarify_recall": _ratio(clarify_tp, clarify_tp + clarify_fn),
        "pending_reason_accuracy": _ratio(pending_hits, pending_total),
        "recoverability_rate": _ratio(recover_hits, recover_total),
        "contract_valid_rate": _ratio(contract_hits, contract_total),
        "error_contract_rate": _ratio(error_hits, error_total),
        "memory_degraded_signal_rate": _ratio(memory_hits, memory_total),
        "execution_limit_violations": execution_limit_violations,
        "task_count_total": task_count_total,
        "task_latency_p90_ms": round(_percentile(task_latencies_ms, ratio=0.90), 2)
        if task_latencies_ms
        else 0.0,
        "task_latency_p95_ms": round(_percentile(task_latencies_ms, ratio=0.95), 2)
        if task_latencies_ms
        else 0.0,
        "llm_tokens_total": llm_tokens_total,
        "llm_calls_total": llm_calls_total,
        "llm_latency_total_ms": llm_latency_total_ms,
        "llm_tokens_per_task": round(llm_tokens_total / task_count_total, 2)
        if task_count_total > 0
        else 0.0,
        "llm_calls_per_task": round(llm_calls_total / task_count_total, 4)
        if task_count_total > 0
        else 0.0,
        "llm_latency_avg_ms_per_task": round(llm_latency_total_ms / task_count_total, 2)
        if task_count_total > 0
        else 0.0,
        "non_causal_sample_count": len(non_causal_latencies_ms),
        "non_causal_p90_ms": int(_percentile(non_causal_latencies_ms, ratio=0.90))
        if non_causal_latencies_ms
        else 0,
        "non_causal_p95_ms": int(_percentile(non_causal_latencies_ms, ratio=0.95))
        if non_causal_latencies_ms
        else 0,
        "non_causal_task_p90_ms": round(_percentile(non_causal_task_latencies_ms, ratio=0.90), 2)
        if non_causal_task_latencies_ms
        else 0.0,
        "non_causal_task_p95_ms": round(_percentile(non_causal_task_latencies_ms, ratio=0.95), 2)
        if non_causal_task_latencies_ms
        else 0.0,
        "primary_total": primary_total,
        "clarify_total": clarify_total,
        "clarify_tp": clarify_tp,
        "clarify_fp": clarify_fp,
        "clarify_fn": clarify_fn,
        "pending_total": pending_total,
        "recover_total": recover_total,
        "contract_total": contract_total,
        "error_total": error_total,
        "memory_total": memory_total,
        "deepsearch_expect_total": deepsearch_expect_total,
        "deepsearch_expectation_rate": _ratio(deepsearch_expect_hits, deepsearch_expect_total),
        "deepsearch_trigger_expect_total": deepsearch_trigger_total,
        "deepsearch_trigger_expectation_rate": _ratio(
            deepsearch_trigger_hits,
            deepsearch_trigger_total,
        ),
        "deepsearch_reuse_expect_total": deepsearch_reuse_total,
        "deepsearch_reuse_expectation_rate": _ratio(deepsearch_reuse_hits, deepsearch_reuse_total),
        "deepsearch_db_range_expect_total": deepsearch_db_range_total,
        "deepsearch_db_range_expectation_rate": _ratio(
            deepsearch_db_range_hits,
            deepsearch_db_range_total,
        ),
        "deepsearch_external_expect_total": deepsearch_external_total,
        "deepsearch_external_expectation_rate": _ratio(
            deepsearch_external_hits,
            deepsearch_external_total,
        ),
        "deepsearch_trigger_rate": _ratio(
            deepsearch_triggered_count,
            len(deepsearch_case_db_hits),
        ),
        "deepsearch_db_hit_ratio_avg": round(
            sum(deepsearch_case_db_hits) / len(deepsearch_case_db_hits),
            6,
        )
        if deepsearch_case_db_hits
        else 0.0,
        "deepsearch_external_calls_avg": round(
            sum(deepsearch_case_external_calls) / len(deepsearch_case_external_calls),
            6,
        )
        if deepsearch_case_external_calls
        else 0.0,
        "deepsearch_pair_total": deepsearch_pair_total,
        "deepsearch_pair_uplift_pass_rate": _ratio(
            deepsearch_pair_uplift_hits,
            deepsearch_pair_total,
        ),
        "deepsearch_pair_external_reduction_rate": _ratio(
            deepsearch_pair_external_reduction_hits,
            deepsearch_pair_total,
        ),
        "deepsearch_db_hit_uplift_avg": round(
            sum(deepsearch_uplifts) / len(deepsearch_uplifts),
            6,
        )
        if deepsearch_uplifts
        else 0.0,
    }


def _aggregate_reedit_metrics(
    case_reports: list[AdvisorReeditCaseReport],
) -> dict[str, Any]:
    overwrite_total = overwrite_hits = 0
    trunc_total = trunc_hits = 0
    history_total = history_hits = 0
    contract_total = contract_hits = 0

    for report in case_reports:
        checks = report.deterministic_checks
        if isinstance(checks.get("overwrite_success"), bool):
            overwrite_total += 1
            overwrite_hits += int(bool(checks["overwrite_success"]))
        if isinstance(checks.get("truncation_correct"), bool):
            trunc_total += 1
            trunc_hits += int(bool(checks["truncation_correct"]))
        if isinstance(checks.get("history_consistent"), bool):
            history_total += 1
            history_hits += int(bool(checks["history_consistent"]))
        if isinstance(checks.get("contract_ok"), bool):
            contract_total += 1
            contract_hits += int(bool(checks["contract_ok"]))

    return {
        "case_count": len(case_reports),
        "reedit_overwrite_success_rate": _ratio(overwrite_hits, overwrite_total),
        "reedit_truncation_correct_rate": _ratio(trunc_hits, trunc_total),
        "reedit_history_consistency_rate": _ratio(history_hits, history_total),
        "reedit_contract_valid_rate": _ratio(contract_hits, contract_total),
        "overwrite_total": overwrite_total,
        "truncation_total": trunc_total,
        "history_total": history_total,
        "contract_total": contract_total,
    }


def _collect_complex_output_render_metrics(
    case_reports: list[AdvisorOrchestratorCaseReport],
) -> dict[str, Any]:
    total = 0
    passed = 0

    for report in case_reports:
        response = report.response if isinstance(report.response, dict) else {}
        artifacts = response.get("artifacts")
        if not isinstance(artifacts, list):
            continue

        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_type = str(artifact.get("type", "")).strip()

            if artifact_type == "school_recommendation":
                total += 1
                data = artifact.get("data", {})
                if not isinstance(data, dict):
                    continue
                narrative = str(data.get("narrative", "") or "").strip()
                schools = data.get("schools")
                if narrative and isinstance(schools, list):
                    passed += 1
                continue

            if artifact_type == "offer_comparison":
                total += 1
                recommendation = str(artifact.get("recommendation", "") or "").strip()
                offers = artifact.get("offers")
                if recommendation and isinstance(offers, list):
                    passed += 1
                continue

            if artifact_type == "what_if_result":
                total += 1
                explanation = str(artifact.get("explanation", "") or "").strip()
                deltas = artifact.get("deltas")
                if explanation and isinstance(deltas, dict):
                    passed += 1

    return {
        "complex_output_render_total": total,
        "complex_output_render_pass_count": passed,
        "complex_output_render_pass_rate": _ratio(passed, total),
    }


def _build_merged_metrics(
    *,
    orchestrator_metrics: dict[str, Any],
    reedit_metrics: dict[str, Any],
    include_reedit: bool,
    all_case_reports: list[AdvisorOrchestratorCaseReport | AdvisorReeditCaseReport],
) -> dict[str, Any]:
    merged = dict(orchestrator_metrics)
    merged["orchestrator_case_count"] = int(orchestrator_metrics.get("case_count", 0) or 0)
    merged["reedit_case_count"] = int(reedit_metrics.get("case_count", 0) or 0)
    merged["include_reedit"] = bool(include_reedit)

    for key in (
        "reedit_overwrite_success_rate",
        "reedit_truncation_correct_rate",
        "reedit_history_consistency_rate",
        "reedit_contract_valid_rate",
    ):
        merged[key] = float(reedit_metrics.get(key, 0.0) or 0.0)

    merged_case_count = merged["orchestrator_case_count"] + merged["reedit_case_count"]
    merged["case_count"] = merged_case_count

    orchestrator_weight = merged["orchestrator_case_count"]
    reedit_weight = merged["reedit_case_count"]

    o_det = float(orchestrator_metrics.get("deterministic_overall_score", 0.0) or 0.0)
    r_det = float(reedit_metrics.get("deterministic_overall_score", 0.0) or 0.0)
    if merged_case_count > 0:
        merged_det = ((o_det * orchestrator_weight) + (r_det * reedit_weight)) / merged_case_count
    else:
        merged_det = 0.0
    merged["deterministic_overall_score"] = round(merged_det, 4)

    merged["final_overall_score"] = round(
        sum(item.final_score for item in all_case_reports) / merged_case_count
        if merged_case_count
        else 0.0,
        4,
    )
    merged["merged_case_score"] = merged["final_overall_score"]
    return merged


def _merged_thresholds(
    *,
    orchestrator_thresholds: dict[str, Any],
    reedit_thresholds: dict[str, Any],
    include_reedit: bool,
) -> dict[str, float]:
    defaults = {
        "primary_hit_rate": 0.90,
        "clarify_correct_rate": 0.95,
        "execution_limit_violations": 0.0,
        "contract_valid_rate": 1.0,
        "non_causal_p90_ms": 8000.0,
        "deepsearch_expectation_rate": 0.90,
        "deepsearch_pair_uplift_pass_rate": 0.90,
        "deepsearch_pair_external_reduction_rate": 0.90,
        "reedit_overwrite_success_rate": 0.95,
        "reedit_truncation_correct_rate": 0.95,
        "reedit_history_consistency_rate": 0.95,
        "judge_overall_score": 85.0,
    }
    merged = dict(defaults)

    source = dict(orchestrator_thresholds)
    if include_reedit:
        source.update(reedit_thresholds)

    for key in defaults:
        if key not in source:
            continue
        try:
            merged[key] = float(source[key])
        except (TypeError, ValueError):
            continue
    return merged


def _grade_status(
    *,
    metrics: dict[str, Any],
    strict_thresholds: dict[str, float],
    include_reedit: bool,
) -> str:
    primary_ok = float(metrics.get("primary_hit_rate", 0.0) or 0.0) >= strict_thresholds["primary_hit_rate"]
    clarify_ok = float(metrics.get("clarify_correct_rate", 0.0) or 0.0) >= strict_thresholds["clarify_correct_rate"]
    limit_ok = float(metrics.get("execution_limit_violations", 1) or 0.0) <= strict_thresholds["execution_limit_violations"]
    contract_ok = float(metrics.get("contract_valid_rate", 0.0) or 0.0) >= strict_thresholds["contract_valid_rate"]
    latency_ok = float(metrics.get("non_causal_p90_ms", 0.0) or 0.0) <= strict_thresholds["non_causal_p90_ms"]
    deepsearch_ok = True
    if int(metrics.get("deepsearch_expect_total", 0) or 0) > 0:
        deepsearch_ok = (
            float(metrics.get("deepsearch_expectation_rate", 0.0) or 0.0)
            >= strict_thresholds["deepsearch_expectation_rate"]
        )
        if int(metrics.get("deepsearch_pair_total", 0) or 0) > 0:
            deepsearch_ok = deepsearch_ok and (
                float(metrics.get("deepsearch_pair_uplift_pass_rate", 0.0) or 0.0)
                >= strict_thresholds["deepsearch_pair_uplift_pass_rate"]
            ) and (
                float(metrics.get("deepsearch_pair_external_reduction_rate", 0.0) or 0.0)
                >= strict_thresholds["deepsearch_pair_external_reduction_rate"]
            )

    reedit_ok = True
    if include_reedit:
        reedit_ok = (
            float(metrics.get("reedit_overwrite_success_rate", 0.0) or 0.0)
            >= strict_thresholds["reedit_overwrite_success_rate"]
            and float(metrics.get("reedit_truncation_correct_rate", 0.0) or 0.0)
            >= strict_thresholds["reedit_truncation_correct_rate"]
            and float(metrics.get("reedit_history_consistency_rate", 0.0) or 0.0)
            >= strict_thresholds["reedit_history_consistency_rate"]
        )

    judge_enabled = bool(metrics.get("judge_enabled", False))
    judge_score = float(metrics.get("judge_overall_score", 0.0) or 0.0)
    judge_ok = (judge_score >= strict_thresholds["judge_overall_score"]) if judge_enabled else False

    if (
        primary_ok
        and clarify_ok
        and limit_ok
        and contract_ok
        and latency_ok
        and deepsearch_ok
        and reedit_ok
        and (judge_ok or not judge_enabled)
    ):
        return "good"

    watch_gate = (
        float(metrics.get("primary_hit_rate", 0.0) or 0.0) >= 0.80
        and float(metrics.get("clarify_correct_rate", 0.0) or 0.0) >= 0.85
        and float(metrics.get("execution_limit_violations", 999) or 0.0) <= 1.0
        and float(metrics.get("contract_valid_rate", 0.0) or 0.0) >= 0.95
        and float(metrics.get("non_causal_p90_ms", 999999.0) or 0.0) <= 12000.0
        and ((judge_score >= 75.0) if judge_enabled else True)
    )
    if include_reedit:
        watch_gate = watch_gate and float(metrics.get("reedit_overwrite_success_rate", 0.0) or 0.0) >= 0.85
    if int(metrics.get("deepsearch_expect_total", 0) or 0) > 0:
        watch_gate = watch_gate and float(metrics.get("deepsearch_expectation_rate", 0.0) or 0.0) >= 0.80
    if watch_gate:
        return "watch"
    return "bad"


def _build_recommendations(
    *,
    metrics: dict[str, Any],
    status: str,
    strict_thresholds: dict[str, float],
    judge_enabled: bool,
    include_reedit: bool,
) -> list[str]:
    recs: list[str] = []
    if float(metrics.get("primary_hit_rate", 0.0) or 0.0) < strict_thresholds["primary_hit_rate"]:
        recs.append("Improve domain/capability routing prompts to raise primary hit rate.")
    if float(metrics.get("clarify_correct_rate", 0.0) or 0.0) < strict_thresholds["clarify_correct_rate"]:
        recs.append("Strengthen conflict and low-confidence clarify policy in route prompts.")
    if float(metrics.get("execution_limit_violations", 0.0) or 0.0) > strict_thresholds["execution_limit_violations"]:
        recs.append("Fix coordinator queue split logic to enforce max 2 executed capabilities per turn.")
    if float(metrics.get("contract_valid_rate", 0.0) or 0.0) < strict_thresholds["contract_valid_rate"]:
        recs.append("Enforce response contract serializer for done/pending/next_actions in every path.")
    if float(metrics.get("non_causal_p90_ms", 0.0) or 0.0) > strict_thresholds["non_causal_p90_ms"]:
        recs.append(
            "Reduce non-causal latency by minimizing route/execution LLM calls and context assembly overhead."
        )
    if int(metrics.get("deepsearch_expect_total", 0) or 0) > 0:
        if float(metrics.get("deepsearch_expectation_rate", 0.0) or 0.0) < strict_thresholds["deepsearch_expectation_rate"]:
            recs.append("Fix internal DeepSearch cold/warm expectation mismatches for school-query cases.")
        if int(metrics.get("deepsearch_pair_total", 0) or 0) > 0:
            if (
                float(metrics.get("deepsearch_pair_uplift_pass_rate", 0.0) or 0.0)
                < strict_thresholds["deepsearch_pair_uplift_pass_rate"]
            ):
                recs.append("Improve cold->warm DB hit uplift behavior in paired school-query flows.")
            if (
                float(metrics.get("deepsearch_pair_external_reduction_rate", 0.0) or 0.0)
                < strict_thresholds["deepsearch_pair_external_reduction_rate"]
            ):
                recs.append("Reduce warm-pass external DeepSearch calls to improve reuse efficiency.")

    if include_reedit:
        if float(metrics.get("reedit_overwrite_success_rate", 0.0) or 0.0) < strict_thresholds["reedit_overwrite_success_rate"]:
            recs.append("Fix re-edit overwrite flow to reliably rewrite target user turns.")
        if float(metrics.get("reedit_truncation_correct_rate", 0.0) or 0.0) < strict_thresholds["reedit_truncation_correct_rate"]:
            recs.append("Fix post-edit truncation to avoid stale timeline residues.")
        if float(metrics.get("reedit_history_consistency_rate", 0.0) or 0.0) < strict_thresholds["reedit_history_consistency_rate"]:
            recs.append("Fix DB history replay consistency after overwrite edits.")

    if judge_enabled and float(metrics.get("judge_overall_score", 0.0) or 0.0) < strict_thresholds["judge_overall_score"]:
        recs.append("Improve assistant summary quality and timeline integrity behavior for judge criteria.")
    if not judge_enabled:
        recs.append("Enable judge stage for full strict-gate evaluation.")
    if status == "good":
        recs.append("Strict merged gate passed. Safe to compare against previous runs for regression monitoring.")
    if not recs:
        recs.append("No major regression signal detected; continue tracking drift over time.")
    return recs


def _select_eval_cases(
    cases: list[AdvisorOrchestratorEvalCase],
    *,
    sample_size: int,
    case_ids: list[str] | None,
) -> list[AdvisorOrchestratorEvalCase]:
    ordered = sorted(cases, key=lambda item: item.case_id)
    return select_eval_cases(
        ordered,
        sample_size=sample_size,
        case_ids=case_ids,
        mini_quotas=_ORCHESTRATOR_MINI_CATEGORY_QUOTAS,
    )


def _select_reedit_cases(
    cases: list[AdvisorReeditEvalCase],
    *,
    sample_size: int | None,
    case_ids: list[str] | None,
) -> list[AdvisorReeditEvalCase]:
    ordered = sorted(cases, key=lambda item: item.case_id)
    return select_reedit_cases(
        ordered,
        sample_size=sample_size,
        case_ids=case_ids,
        mini_quotas=_REEDIT_MINI_CATEGORY_QUOTAS,
    )


def _select_orchestrator_mini_cases(
    ordered_cases: list[AdvisorOrchestratorEvalCase],
) -> list[AdvisorOrchestratorEvalCase]:
    return select_stratified_cases(
        ordered_cases,
        quotas=_ORCHESTRATOR_MINI_CATEGORY_QUOTAS,
        sample_size=sum(_ORCHESTRATOR_MINI_CATEGORY_QUOTAS.values()),
        label="orchestrator",
    )


def _select_reedit_mini_cases(
    ordered_cases: list[AdvisorReeditEvalCase],
) -> list[AdvisorReeditEvalCase]:
    return select_stratified_cases(
        ordered_cases,
        quotas=_REEDIT_MINI_CATEGORY_QUOTAS,
        sample_size=sum(_REEDIT_MINI_CATEGORY_QUOTAS.values()),
        label="reedit",
    )


def _select_stratified_cases(
    ordered_cases: list[AdvisorOrchestratorEvalCase] | list[AdvisorReeditEvalCase],
    *,
    quotas: dict[str, int],
    sample_size: int,
    label: str,
) -> list[AdvisorOrchestratorEvalCase] | list[AdvisorReeditEvalCase]:
    return select_stratified_cases(
        list(ordered_cases),
        quotas=quotas,
        sample_size=sample_size,
        label=label,
    )


async def _collect_token_usage(
    *,
    eval_run_id: str,
    caller_prefixes: tuple[str, ...] | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    if not enabled:
        return {
            "calls": 0,
            "errors": 0,
            "tokens": 0,
            "p95_latency_ms": 0.0,
            "rate_limit_errors": 0,
            "latency_total_ms": 0,
            "disabled": True,
        }

    try:
        from sqlalchemy import select

        from scholarpath.db.models import TokenUsage
        from scholarpath.db.session import async_session_factory
    except Exception as exc:  # pragma: no cover - runtime env dependent
        return {
            "calls": 0,
            "errors": 0,
            "tokens": 0,
            "p95_latency_ms": 0.0,
            "rate_limit_errors": 0,
            "latency_total_ms": 0,
            "error": str(exc),
        }

    pattern = f"%#{eval_run_id}%"
    try:
        async with async_session_factory() as session:
            stmt = select(
                TokenUsage.total_tokens,
                TokenUsage.error,
                TokenUsage.latency_ms,
                TokenUsage.caller,
            ).where(TokenUsage.caller.like(pattern))
            rows = (await session.execute(stmt)).all()
    except Exception as exc:  # pragma: no cover - runtime env dependent
        return {
            "calls": 0,
            "errors": 0,
            "tokens": 0,
            "p95_latency_ms": 0.0,
            "rate_limit_errors": 0,
            "latency_total_ms": 0,
            "error": str(exc),
        }

    prefixes = tuple(
        str(prefix).strip().lower()
        for prefix in (caller_prefixes or ())
        if str(prefix).strip()
    )
    filtered = []
    for total, error, latency, caller in rows:
        caller_text = str(caller or "").strip().lower()
        if prefixes and not caller_text.startswith(prefixes):
            continue
        filtered.append((total, error, latency))

    calls = len(filtered)
    errors = sum(1 for _, err, _ in filtered if err)
    tokens = int(sum(int(total or 0) for total, _, _ in filtered))
    latencies = [int(lat) for _, _, lat in filtered if lat is not None]
    latency_total_ms = int(sum(latencies))
    p95_latency = _percentile(latencies, ratio=0.95) if latencies else 0.0
    rate_limit_errors = sum(
        1
        for _, err, _ in filtered
        if isinstance(err, str) and "rate limit" in err.lower()
    )
    return {
        "calls": calls,
        "errors": errors,
        "tokens": tokens,
        "p95_latency_ms": round(p95_latency, 2),
        "rate_limit_errors": rate_limit_errors,
        "latency_total_ms": latency_total_ms,
    }


def _percentile(values: list[int | float], *, ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(len(ordered) * ratio) - 1))
    return float(ordered[idx])


def _json_default(value: Any) -> Any:
    return json_default(value)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    write_json(path, payload)


def _write_cases_jsonl(
    path: Path,
    cases: list[AdvisorOrchestratorCaseReport] | list[AdvisorReeditCaseReport],
) -> None:
    write_cases_jsonl(path, list(cases))


def _write_markdown_summary(path: Path, report: AdvisorOrchestratorEvalReport) -> None:
    write_markdown_summary(path, report)


def _append_history(path: Path, report: AdvisorOrchestratorEvalReport) -> None:
    append_history(path, report)


def _ratio(num: int, den: int) -> float:
    if den <= 0:
        return 0.0
    return round(num / den, 6)


def _slice(values: list[str], start: int, end: int) -> list[str]:
    if not values:
        return []
    n = len(values)
    if start < 0:
        start += n
    if end < 0:
        end += n
    start = max(0, start)
    end = min(n - 1, end)
    if start > end or start >= n:
        return []
    return values[start : end + 1]
