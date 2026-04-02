"""Advisor orchestrator façade with DI over orchestration components."""

from __future__ import annotations

import logging
import time
import uuid

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.advisor.contracts import AdvisorRequest, AdvisorResponse
from scholarpath.advisor.memory_context import ContextMetrics
from scholarpath.llm.client import LLMClient

from .orchestration import (
    CapabilityRegistry,
    ClarifyGate,
    Coordinator,
    Executor,
    IntentCandidate,
    OrchestratorRuntime,
    Planner,
    ResponseComposer,
    RouteDiagnostics,
)
from .orchestration.constants import (
    CAPABILITY_THRESHOLD_DEFAULT,
    CAPABILITY_THRESHOLD_MAP_DEFAULT,
    DOMAIN_THRESHOLD_DEFAULT,
    INTENT_CLARITY_THRESHOLD_DEFAULT,
)
from .orchestration.utils import (
    conflict_group,
    dedupe_pending,
    merge_context_metrics,
    parse_uuid_or_none,
)

logger = logging.getLogger(__name__)


class AdvisorOrchestrator:
    """Coordinator-style advisor orchestrator façade."""

    def __init__(
        self,
        *,
        llm: LLMClient,
        session: AsyncSession,
        redis: aioredis.Redis,
        registry: CapabilityRegistry,
        domain_confidence_threshold: float = DOMAIN_THRESHOLD_DEFAULT,
        capability_confidence_threshold: float = CAPABILITY_THRESHOLD_DEFAULT,
        intent_clarity_threshold: float = INTENT_CLARITY_THRESHOLD_DEFAULT,
        capability_threshold_map: dict[str, float] | None = None,
        planner: Planner | None = None,
        guard: ClarifyGate | None = None,
        coordinator: Coordinator | None = None,
        executor: Executor | None = None,
        composer: ResponseComposer | None = None,
        runtime: OrchestratorRuntime | None = None,
    ) -> None:
        self._llm = llm
        self._session = session
        self._registry = registry
        self._capability_threshold_map = {
            **CAPABILITY_THRESHOLD_MAP_DEFAULT,
            **(capability_threshold_map or {}),
        }

        self._runtime = runtime or OrchestratorRuntime(
            session=session,
            redis=redis,
            registry=registry,
        )
        self._planner = planner or Planner(llm=llm, registry=registry)
        self._guard = guard or ClarifyGate(
            registry=registry,
            domain_threshold=domain_confidence_threshold,
            intent_clarity_threshold=intent_clarity_threshold,
            capability_threshold_map={
                "default": capability_confidence_threshold,
                **self._capability_threshold_map,
            },
        )
        self._coordinator = coordinator or Coordinator()
        self._composer = composer or ResponseComposer()
        self._executor = executor or Executor(
            llm=llm,
            session=session,
            memory=self._runtime.memory,
            registry=registry,
            execute_with_retry=self._runtime.execute_with_retry,
        )

    async def process(self, request: AdvisorRequest) -> AdvisorResponse:
        """Process one advisor turn with multi-intent coordination."""
        started = time.perf_counter()
        diagnostics = RouteDiagnostics()
        turn_id = request.turn_id or str(uuid.uuid4())
        client_context = request.client_context or {}
        trigger = str(client_context.get("trigger", "")).strip()
        rough_student_id = parse_uuid_or_none(request.student_id)

        await self._runtime.save_user_message(session_id=request.session_id, message=request.message)
        await self._runtime.record_user_turn_event(request=request, turn_id=turn_id)

        early_error = self._runtime.early_input_error(request=request, trigger=trigger)
        if early_error is not None:
            stored_pending = await self._runtime.load_stored_pending(request.session_id)
            recover_actions = self._runtime.build_invalid_input_actions(
                message=early_error["message"],
                reason=early_error["reason"],
            )
            response = self._composer.compose_invalid_input(
                turn_id=turn_id,
                message=early_error["message"],
                pending_steps=stored_pending,
                recover_actions=recover_actions,
                route_meta=self._runtime.build_route_meta(
                    started=started,
                    domain_confidence=0.0,
                    capability_confidence=0.0,
                    fallback_used=True,
                    guard_result="invalid_input",
                    guard_reason=early_error["reason"],
                    primary_capability=None,
                    done_count=0,
                    pending_count=len(stored_pending),
                    metrics=ContextMetrics(),
                    diagnostics=diagnostics,
                ),
            )
            return await self._runtime.finalize_response(request=request, response=response)

        route_context_started = time.perf_counter()
        route_context, route_metrics = await self._runtime.assemble_context(
            session_id=request.session_id,
            stage="route",
            message=request.message,
            student_id=rough_student_id,
            domain=request.domain_hint,
        )
        diagnostics.route_context_ms = int((time.perf_counter() - route_context_started) * 1000)
        stored_pending = await self._runtime.load_stored_pending(request.session_id)

        planner_started = time.perf_counter()
        decision = await self._planner.plan(
            request=request,
            context=route_context,
            trigger=trigger,
        )
        diagnostics.planner_ms = int((time.perf_counter() - planner_started) * 1000)
        diagnostics.llm_calls += decision.llm_calls
        guard = self._guard.evaluate(request=request, decision=decision)

        if guard.result == "invalid_input":
            recover_actions = self._runtime.build_invalid_input_actions(
                message=guard.message or "Invalid input",
                reason=guard.reason,
            )
            response = self._composer.compose_invalid_input(
                turn_id=turn_id,
                message=guard.message or "Invalid input",
                pending_steps=stored_pending,
                recover_actions=recover_actions,
                route_meta=self._runtime.build_route_meta(
                    started=started,
                    domain_confidence=decision.domain_confidence,
                    capability_confidence=decision.capability_confidence,
                    fallback_used=True,
                    guard_result="invalid_input",
                    guard_reason=guard.reason,
                    primary_capability=decision.primary.capability if decision.primary is not None else None,
                    done_count=0,
                    pending_count=len(stored_pending),
                    metrics=route_metrics,
                    diagnostics=diagnostics,
                ),
            )
            return await self._runtime.finalize_response(request=request, response=response)

        if guard.result == "clarify":
            response = await self._runtime.build_clarify_response(
                composer=self._composer,
                turn_id=turn_id,
                started=started,
                decision=decision,
                reason=guard.reason,
                pending_candidates=decision.candidates,
                existing_pending=stored_pending,
                route_metrics=route_metrics,
                diagnostics=diagnostics,
            )
            return await self._runtime.finalize_response(request=request, response=response)

        if guard.requested_definition is not None:
            requested_definition = guard.requested_definition
            execution_queue = [
                IntentCandidate(
                    capability=requested_definition.capability_id,
                    confidence=1.0,
                    conflict_group=conflict_group(requested_definition.capability_id),
                    source="trigger",
                )
            ]
            pending_steps = [
                row for row in stored_pending if row.capability != requested_definition.capability_id
            ]
            domain = requested_definition.domain
        else:
            execution_queue, pending_from_split = self._coordinator.build_execution_queue(decision.candidates)
            pending_steps = pending_from_split + stored_pending
            domain = decision.domain

        if not execution_queue:
            response = await self._runtime.build_clarify_response(
                composer=self._composer,
                turn_id=turn_id,
                started=started,
                decision=decision,
                reason="low_confidence",
                pending_candidates=decision.candidates,
                existing_pending=stored_pending,
                route_metrics=route_metrics,
                diagnostics=diagnostics,
            )
            return await self._runtime.finalize_response(request=request, response=response)

        student_id = parse_uuid_or_none(request.student_id)

        execution_context_started = time.perf_counter()
        execution_context, execution_metrics = await self._runtime.assemble_context(
            session_id=request.session_id,
            stage="execution",
            message=request.message,
            student_id=student_id,
            domain=domain,
        )
        diagnostics.execution_context_ms = int((time.perf_counter() - execution_context_started) * 1000)

        capability_exec_started = time.perf_counter()
        execution_result = await self._executor.run(
            turn_id=turn_id,
            request=request,
            execution_queue=execution_queue,
            execution_context=execution_context,
            student_id=student_id,
        )
        diagnostics.capability_exec_ms = int((time.perf_counter() - capability_exec_started) * 1000)
        diagnostics.llm_calls += execution_result.llm_calls

        pending_steps.extend(execution_result.failed_for_retry)
        pending_steps = dedupe_pending(
            pending_steps,
            executed=[d.capability for d in execution_result.done_steps if d.status != "failed"],
        )

        merged_metrics = merge_context_metrics(route_metrics, execution_metrics)
        force_clarify = bool(merged_metrics.memory_conflicts)

        primary_capability = execution_queue[0].capability if execution_queue else None
        primary_definition = (
            self._registry.get(primary_capability)
            if primary_capability is not None
            else None
        )

        response = self._composer.compose_execution(
            turn_id=turn_id,
            domain=primary_definition.domain if primary_definition is not None else "common",
            capability=primary_capability or "common.general",
            done_steps=execution_result.done_steps,
            pending_steps=pending_steps,
            artifacts=execution_result.artifacts,
            compatibility_actions=execution_result.compatibility_actions,
            step_outputs=execution_result.step_outputs,
            force_clarify=force_clarify,
            route_meta=self._runtime.build_route_meta(
                started=started,
                domain_confidence=decision.domain_confidence,
                capability_confidence=decision.capability_confidence,
                fallback_used=False,
                guard_result="pass",
                guard_reason="none",
                primary_capability=primary_capability,
                done_count=len(execution_result.done_steps),
                pending_count=len(pending_steps),
                metrics=merged_metrics,
                diagnostics=diagnostics,
            ),
        )
        return await self._runtime.finalize_response(request=request, response=response)
