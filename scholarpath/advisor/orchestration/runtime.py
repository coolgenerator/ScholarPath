"""Runtime support services for advisor orchestration façade."""

from __future__ import annotations

import logging
import time
from typing import Literal

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.advisor.contracts import (
    AdvisorAction,
    AdvisorCapability,
    AdvisorDomain,
    AdvisorRequest,
    AdvisorResponse,
    AdvisorRouteMeta,
    MemoryIngestEvent,
    PendingStep,
)
from scholarpath.advisor.memory_context import ContextAssembler, ContextMetrics, persist_turn_message
from scholarpath.chat.memory import ChatMemory
from scholarpath.config import settings

from .composer import ResponseComposer
from .constants import ALLOWED_DOMAINS, FAILED_STEPS_KEY, PENDING_QUEUE_KEY, TRIGGER_ACTIONS
from .registry import CapabilityRegistry
from .types import (
    CapabilityContext,
    CapabilityDefinition,
    CapabilityResult,
    IntentCandidate,
    RouteDecision,
    RouteDiagnostics,
)
from .utils import (
    dedupe_actions,
    dedupe_pending,
    parse_pending_steps,
    safe_capability,
    student_id_validation_error,
)

logger = logging.getLogger(__name__)


class OrchestratorRuntime:
    """Runtime services used by AdvisorOrchestrator façade."""

    def __init__(
        self,
        *,
        session: AsyncSession,
        redis: aioredis.Redis,
        registry: CapabilityRegistry,
        memory: ChatMemory | None = None,
        context_assembler: ContextAssembler | None = None,
    ) -> None:
        self._session = session
        self._memory = memory or ChatMemory(redis)
        self._registry = registry
        self._context_assembler = context_assembler or ContextAssembler(session=session, memory=self._memory)

    @property
    def memory(self) -> ChatMemory:
        return self._memory

    async def save_user_message(self, *, session_id: str, message: str) -> None:
        await self._memory.save_message(session_id, "user", message)

    def build_invalid_input_actions(
        self,
        *,
        message: str,
        reason: Literal["invalid_input", "trigger_invalid"],
    ) -> list[AdvisorAction]:
        actions: list[AdvisorAction] = []
        lowered = message.lower()
        if "student_id" in lowered:
            actions.append(
                AdvisorAction(
                    action_id="input.fix_student_id",
                    label="修正 student_id",
                    payload={
                        "field": "student_id",
                        "required_format": "uuid",
                    },
                )
            )
        if "capability_hint" in lowered or reason == "trigger_invalid":
            actions.append(
                AdvisorAction(
                    action_id="input.fix_capability_hint",
                    label="修正 capability_hint",
                    payload={
                        "field": "capability_hint",
                        "allowed_capabilities": self.allowed_trigger_capabilities(),
                    },
                )
            )
        actions.append(
            AdvisorAction(
                action_id="route.clarify",
                label="先澄清优先级",
                payload={"client_context": {"trigger": "route.clarify"}},
            )
        )
        return dedupe_actions(actions)

    def early_input_error(
        self,
        *,
        request: AdvisorRequest,
        trigger: str,
    ) -> dict[str, Literal["invalid_input", "trigger_invalid"] | str] | None:
        student_error = student_id_validation_error(request.student_id)
        if student_error is not None:
            return {"reason": "invalid_input", "message": student_error}

        if trigger in TRIGGER_ACTIONS:
            if not request.capability_hint:
                return {
                    "reason": "trigger_invalid",
                    "message": f"capability_hint is required for trigger: {trigger}",
                }
            requested = self._registry.get(str(request.capability_hint))
            if requested is None:
                return {
                    "reason": "trigger_invalid",
                    "message": f"Unknown capability_hint: {request.capability_hint}",
                }
        return None

    async def load_stored_pending(self, session_id: str) -> list[PendingStep]:
        common_ctx = await self._memory.get_context(session_id, domain="common")
        return parse_pending_steps(common_ctx.get(PENDING_QUEUE_KEY))

    def allowed_trigger_capabilities(self) -> list[AdvisorCapability]:
        allowed: list[AdvisorCapability] = []
        for domain in ("undergrad", "offer", "common"):
            for definition in self._registry.list_by_domain(domain):
                allowed.append(definition.capability_id)
        deduped: list[AdvisorCapability] = []
        seen: set[str] = set()
        for capability in allowed:
            if capability in seen:
                continue
            seen.add(capability)
            deduped.append(capability)
        return deduped

    def build_route_meta(
        self,
        *,
        started: float,
        domain_confidence: float,
        capability_confidence: float,
        fallback_used: bool,
        guard_result: Literal["pass", "clarify", "invalid_input"],
        guard_reason: Literal["low_confidence", "conflict", "invalid_input", "trigger_invalid", "none"],
        primary_capability: AdvisorCapability | None,
        done_count: int,
        pending_count: int,
        metrics: ContextMetrics,
        diagnostics: RouteDiagnostics,
    ) -> AdvisorRouteMeta:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return AdvisorRouteMeta(
            domain_confidence=domain_confidence,
            capability_confidence=capability_confidence,
            router_model=settings.ZAI_MODEL,
            latency_ms=latency_ms,
            fallback_used=fallback_used,
            context_tokens=metrics.context_tokens,
            memory_hits=metrics.memory_hits,
            rag_hits=metrics.rag_hits,
            rag_latency_ms=metrics.rag_latency_ms,
            memory_degraded=metrics.memory_degraded,
            guard_result=guard_result,
            guard_reason=guard_reason,
            primary_capability=primary_capability,
            executed_count=done_count,
            pending_count=pending_count,
            planner_ms=diagnostics.planner_ms,
            route_context_ms=diagnostics.route_context_ms,
            execution_context_ms=diagnostics.execution_context_ms,
            capability_exec_ms=diagnostics.capability_exec_ms,
            llm_calls=diagnostics.llm_calls,
        )

    async def finalize_response(
        self,
        *,
        request: AdvisorRequest,
        response: AdvisorResponse,
    ) -> AdvisorResponse:
        await self._memory.save_message(request.session_id, "assistant", response.assistant_text)
        await self.persist_recovery_state(request.session_id, response.pending, response.done)
        await self.record_turn_event(
            event=MemoryIngestEvent(
                turn_id=response.turn_id,
                session_id=request.session_id,
                student_id=request.student_id,
                domain=response.domain,
                capability=response.capability,
                role="assistant",
                content=response.assistant_text,
                artifacts=[a.model_dump(mode="json") for a in response.artifacts],
                done=response.done,
                pending=response.pending,
                next_actions=response.next_actions,
            )
        )
        return response

    async def assemble_context(
        self,
        *,
        session_id: str,
        stage: str,
        message: str,
        student_id,
        domain: AdvisorDomain | None,
    ) -> tuple[dict[str, object], ContextMetrics]:
        try:
            return await self._context_assembler.assemble(
                stage=stage,
                session_id=session_id,
                student_id=student_id,
                message=message,
                domain=domain,
            )
        except Exception:
            logger.warning("Context assembler failed; fallback to Redis-only context", exc_info=True)
            history = await self._memory.get_history(session_id, limit=10)
            recent_messages = "\n".join(f"{m['role']}: {m['content']}" for m in history[-5:])
            return (
                {
                    "recent_messages": recent_messages,
                    "route_prompt_context": recent_messages,
                    "undergrad": await self._memory.get_context(session_id, domain="undergrad"),
                    "offer": await self._memory.get_context(session_id, domain="offer"),
                    "common": await self._memory.get_context(session_id, domain="common"),
                    "memory_items": [],
                    "memory_conflicts": [],
                    "retrieved_chunks": [],
                },
                ContextMetrics(
                    context_tokens=max(len(recent_messages) // 4, 1) if recent_messages else 0,
                    memory_degraded=True,
                ),
            )

    async def execute_with_retry(
        self,
        definition: CapabilityDefinition,
        runtime_ctx: CapabilityContext,
    ) -> tuple[CapabilityResult | None, int, Exception | None]:
        attempts = 0
        last_error: Exception | None = None
        while attempts < 2:
            try:
                result = await definition.handler(runtime_ctx)
                return result, attempts, None
            except ModuleNotFoundError:
                raise
            except Exception as exc:
                attempts += 1
                last_error = exc
                logger.warning(
                    "Capability %s failed on attempt %d",
                    definition.capability_id,
                    attempts,
                    exc_info=True,
                )
                if attempts >= 2:
                    break
        return None, 1, last_error

    async def persist_recovery_state(
        self,
        session_id: str,
        pending,
        done,
    ) -> None:
        pending_payload = [step.model_dump(mode="json") for step in pending]
        failed_payload = [
            step.model_dump(mode="json")
            for step in done
            if step.status in {"failed", "degraded"}
        ]
        await self._memory.save_context(
            session_id,
            PENDING_QUEUE_KEY,
            pending_payload,
            domain="common",
        )
        await self._memory.save_context(
            session_id,
            FAILED_STEPS_KEY,
            failed_payload,
            domain="common",
        )

    async def record_turn_event(self, *, event: MemoryIngestEvent) -> None:
        """Persist advisor message row and enqueue async memory ingestion."""
        message_row = None
        try:
            message_row = await persist_turn_message(session=self._session, event=event)
        except Exception:
            logger.warning("Failed to persist advisor message row", exc_info=True)
            return

        try:
            from scholarpath.tasks import celery_app

            celery_app.send_task(
                "scholarpath.tasks.advisor_memory.advisor_memory_ingest_message",
                kwargs={"message_id": str(message_row.id)},
            )
        except Exception:
            logger.warning("Failed to enqueue advisor memory ingest task", exc_info=True)

    async def build_clarify_response(
        self,
        *,
        composer: ResponseComposer,
        turn_id: str,
        started: float,
        decision: RouteDecision,
        reason: Literal["low_confidence", "conflict"],
        pending_candidates: list[IntentCandidate],
        existing_pending: list[PendingStep],
        route_metrics: ContextMetrics,
        diagnostics: RouteDiagnostics,
    ) -> AdvisorResponse:
        clarify_text = "我需要先确认你这轮的主要目标：本科择校、offer取舍、先聊聊、还是先做情绪支持？"
        clarify_actions = [
            AdvisorAction(
                action_id="route.clarify",
                label="先澄清优先级",
                payload={"client_context": {"trigger": "route.clarify"}},
            )
        ]
        pending_steps = list(existing_pending)
        pending_reason = "conflict" if reason == "conflict" else "low_confidence"
        for candidate in pending_candidates:
            pending_steps.append(
                PendingStep(
                    capability=candidate.capability,
                    reason=pending_reason,  # type: ignore[arg-type]
                    message=f"confidence={candidate.confidence:.2f}; source={candidate.source}",
                )
            )
        pending_steps = dedupe_pending(pending_steps, executed=[])

        force_clarify = bool(route_metrics.memory_conflicts)
        return composer.compose_clarify(
            turn_id=turn_id,
            reason=reason,
            clarify_text=clarify_text,
            clarify_actions=clarify_actions,
            pending_steps=pending_steps,
            force_clarify=force_clarify,
            route_meta=self.build_route_meta(
                started=started,
                domain_confidence=decision.domain_confidence,
                capability_confidence=decision.capability_confidence,
                fallback_used=True,
                guard_result="clarify",
                guard_reason=reason,
                primary_capability=decision.primary.capability if decision.primary is not None else None,
                done_count=1,
                pending_count=len(pending_steps),
                metrics=route_metrics,
                diagnostics=diagnostics,
            ),
        )

    async def record_user_turn_event(self, *, request: AdvisorRequest, turn_id: str) -> None:
        await self.record_turn_event(
            event=MemoryIngestEvent(
                turn_id=turn_id,
                session_id=request.session_id,
                student_id=request.student_id,
                domain=request.domain_hint if request.domain_hint in ALLOWED_DOMAINS else "common",
                capability=safe_capability(request.capability_hint, fallback="common.general"),
                role="user",
                content=request.message,
            )
        )
