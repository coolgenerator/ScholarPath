"""Response composer for advisor orchestration."""

from __future__ import annotations

from scholarpath.advisor.contracts import (
    AdvisorAction,
    AdvisorError,
    AdvisorResponse,
    AdvisorRouteMeta,
    AdvisorCapability,
    AdvisorDomain,
    DoneStep,
    PendingStep,
)

from .utils import build_next_actions, build_summary_text, dedupe_actions


class ResponseComposer:
    """Composer: convert execution/clarify/error state into AdvisorResponse."""

    def compose_execution(
        self,
        *,
        turn_id: str,
        domain: AdvisorDomain,
        capability: AdvisorCapability,
        done_steps: list[DoneStep],
        pending_steps: list[PendingStep],
        artifacts: list[object],
        compatibility_actions: list[AdvisorAction],
        step_outputs: list[str],
        force_clarify: bool,
        route_meta: AdvisorRouteMeta,
    ) -> AdvisorResponse:
        next_actions = build_next_actions(
            pending=pending_steps,
            done=done_steps,
            force_clarify=force_clarify,
        )
        summary_text = build_summary_text(
            step_outputs=step_outputs,
            done=done_steps,
            pending=pending_steps,
            next_actions=next_actions,
        )
        return AdvisorResponse(
            turn_id=turn_id,
            domain=domain,
            capability=capability,
            assistant_text=summary_text,
            artifacts=artifacts,
            actions=dedupe_actions(compatibility_actions + next_actions),
            done=done_steps,
            pending=pending_steps,
            next_actions=next_actions,
            route_meta=route_meta,
        )

    def compose_clarify(
        self,
        *,
        turn_id: str,
        reason: str,
        clarify_text: str,
        clarify_actions: list[AdvisorAction],
        pending_steps: list[PendingStep],
        force_clarify: bool,
        route_meta: AdvisorRouteMeta,
    ) -> AdvisorResponse:
        done = [DoneStep(capability="common.clarify", status="degraded", message=reason, retry_count=0)]
        next_actions = dedupe_actions(
            clarify_actions
            + build_next_actions(
                pending=pending_steps,
                done=[],
                force_clarify=force_clarify,
            )
        )
        summary_text = build_summary_text(
            step_outputs=[f"common.clarify: {clarify_text}"],
            done=done,
            pending=pending_steps,
            next_actions=next_actions,
        )
        return AdvisorResponse(
            turn_id=turn_id,
            domain="common",
            capability="common.clarify",
            assistant_text=summary_text,
            artifacts=[],
            actions=next_actions,
            done=done,
            pending=pending_steps,
            next_actions=next_actions,
            route_meta=route_meta,
        )

    def compose_invalid_input(
        self,
        *,
        turn_id: str,
        message: str,
        pending_steps: list[PendingStep],
        recover_actions: list[AdvisorAction],
        route_meta: AdvisorRouteMeta,
    ) -> AdvisorResponse:
        next_actions = dedupe_actions(
            recover_actions
            + build_next_actions(
                pending=pending_steps,
                done=[],
                force_clarify=False,
            )
        )
        summary_text = build_summary_text(
            step_outputs=[f"error: {message}"],
            done=[],
            pending=pending_steps,
            next_actions=next_actions,
        )
        return AdvisorResponse(
            turn_id=turn_id,
            domain="common",
            capability="common.general",
            assistant_text=summary_text,
            artifacts=[],
            actions=next_actions,
            done=[],
            pending=pending_steps,
            next_actions=next_actions,
            route_meta=route_meta,
            error=AdvisorError(
                code="INVALID_INPUT",
                message=message,
                retriable=False,
                detail=None,
            ),
        )
