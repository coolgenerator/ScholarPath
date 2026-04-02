"""Capability executor for advisor orchestration."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.advisor.contracts import AdvisorRequest, PendingStep, DoneStep
from scholarpath.chat.memory import ChatMemory
from scholarpath.llm.client import LLMClient

from .registry import CapabilityRegistry
from .types import (
    CapabilityContext,
    CapabilityDefinition,
    CapabilityResult,
    ExecutionResult,
    IntentCandidate,
)
from .utils import compact


class Executor:
    """Executor: execute capabilities with one retry policy."""

    def __init__(
        self,
        *,
        llm: LLMClient,
        session: AsyncSession,
        memory: ChatMemory,
        registry: CapabilityRegistry,
        execute_with_retry: Callable[
            [CapabilityDefinition, CapabilityContext],
            Awaitable[tuple[CapabilityResult | None, int, Exception | None]],
        ],
    ) -> None:
        self._llm = llm
        self._session = session
        self._memory = memory
        self._registry = registry
        self._execute_with_retry = execute_with_retry

    async def run(
        self,
        *,
        turn_id: str,
        request: AdvisorRequest,
        execution_queue: list[IntentCandidate],
        execution_context: dict[str, object],
        student_id: uuid.UUID | None,
    ) -> ExecutionResult:
        done_steps: list[DoneStep] = []
        all_artifacts: list[object] = []
        compatibility_actions = []
        step_outputs: list[str] = []
        failed_for_retry: list[PendingStep] = []
        llm_calls = 0

        for candidate in execution_queue:
            definition = self._registry.get(candidate.capability)
            if definition is None:
                done_steps.append(
                    DoneStep(
                        capability=candidate.capability,
                        status="failed",
                        message="Capability not found in registry.",
                        retry_count=0,
                    )
                )
                failed_for_retry.append(
                    PendingStep(
                        capability=candidate.capability,
                        reason="requires_user_trigger",
                        message="Capability unavailable. Retry after fix.",
                    )
                )
                continue

            if definition.requires_student and student_id is None:
                done_steps.append(
                    DoneStep(
                        capability=definition.capability_id,
                        status="failed",
                        message="This capability requires student_id.",
                        retry_count=0,
                    )
                )
                failed_for_retry.append(
                    PendingStep(
                        capability=definition.capability_id,
                        reason="requires_user_trigger",
                        message="Provide student_id and retry.",
                    )
                )
                continue

            runtime_ctx = CapabilityContext(
                turn_id=turn_id,
                session_id=request.session_id,
                student_id=student_id,
                message=request.message,
                locale=request.locale,
                domain=definition.domain,
                capability=definition.capability_id,
                client_context=request.client_context or {},
                llm=self._llm,
                session=self._session,
                memory=self._memory,
                conversation_context=execution_context,
            )

            result, retry_count, error = await self._execute_with_retry(definition, runtime_ctx)
            if result is not None:
                status = "degraded" if retry_count > 0 else "succeeded"
                step_msg = (
                    result.step_summary.get("message")
                    if isinstance(result.step_summary, dict)
                    else None
                )
                done_steps.append(
                    DoneStep(
                        capability=definition.capability_id,
                        status=status,
                        message=step_msg or compact(result.assistant_text),
                        retry_count=retry_count,
                    )
                )
                if result.assistant_text.strip():
                    step_outputs.append(f"{definition.capability_id}: {result.assistant_text.strip()}")
                all_artifacts.extend(result.artifacts)
                compatibility_actions.extend(result.actions)
                if isinstance(result.metadata, dict):
                    try:
                        llm_calls += int(result.metadata.get("llm_calls", 0) or 0)
                    except (TypeError, ValueError):
                        llm_calls += 0
            else:
                done_steps.append(
                    DoneStep(
                        capability=definition.capability_id,
                        status="failed",
                        message=compact(str(error) if error else "Capability failed."),
                        retry_count=1,
                    )
                )
                failed_for_retry.append(
                    PendingStep(
                        capability=definition.capability_id,
                        reason="requires_user_trigger",
                        message="Step failed after retry. Use retry action.",
                    )
                )

        return ExecutionResult(
            done_steps=done_steps,
            artifacts=all_artifacts,
            compatibility_actions=compatibility_actions,
            step_outputs=step_outputs,
            failed_for_retry=failed_for_retry,
            llm_calls=llm_calls,
        )
