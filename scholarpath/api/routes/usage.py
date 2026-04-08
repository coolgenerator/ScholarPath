"""Token usage tracking routes."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from scholarpath.api.deps import SessionDep
from scholarpath.db.models.token_usage import TokenUsage

router = APIRouter(prefix="/usage", tags=["usage"])


class UsageSummary(BaseModel):
    total_calls: int
    total_prompt_tokens: int
    total_completion_tokens: int
    total_tokens: int
    by_provider: dict[str, Any]
    by_caller: dict[str, Any]
    by_model: dict[str, Any]
    error_count: int


class UsageEntry(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    created_at: datetime
    model: str
    provider: str
    caller: str
    method: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    error: str | None = None
    latency_ms: int | None = None


class LLMEndpointWindowStats(BaseModel):
    index: int
    endpoint_id: str
    key_id: str
    requests_total: int
    errors_total: int
    rate_limit_total: int
    timeout_total: int
    same_task_retry_triggered: int = 0
    same_task_retry_success: int = 0
    same_task_retry_failed: int = 0
    preferred_route_hits: int = 0
    policy_applied_counts_by_method: dict[str, int] = Field(default_factory=dict)
    required_output_missing: int = 0
    parse_fail: int = 0
    non_json: int = 0
    schema_mismatch: int = 0
    requests_window: float
    errors_window: float
    rate_limit_window: float
    timeout_window: float
    latency_ms_avg: float
    cooldown_active: bool


class LLMEndpointHealth(BaseModel):
    window_seconds: int
    active_mode: str | None = None
    active_policy: str | None = None
    observer_enabled: bool
    observer_error: str | None = None
    endpoints: list[LLMEndpointWindowStats]


@router.get("/summary", response_model=UsageSummary)
async def get_usage_summary(
    session: SessionDep,
    days: int | None = Query(
        default=None,
        ge=1,
        description="Optional time window in days. Omit to return all records.",
    ),
) -> dict:
    """Get aggregated token usage summary."""
    filters = []
    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        filters.append(TokenUsage.created_at >= cutoff)

    # Total counts
    totals_stmt = select(
        func.count(TokenUsage.id).label("calls"),
        func.coalesce(func.sum(TokenUsage.prompt_tokens), 0).label("prompt"),
        func.coalesce(func.sum(TokenUsage.completion_tokens), 0).label("completion"),
        func.coalesce(func.sum(TokenUsage.total_tokens), 0).label("total"),
        func.count(TokenUsage.error).label("errors"),
    )
    if filters:
        totals_stmt = totals_stmt.where(*filters)
    totals = await session.execute(totals_stmt)
    row = totals.one()

    # By provider
    by_provider_stmt = select(
        TokenUsage.provider,
        func.count(TokenUsage.id).label("calls"),
        func.coalesce(func.sum(TokenUsage.total_tokens), 0).label("tokens"),
    )
    if filters:
        by_provider_stmt = by_provider_stmt.where(*filters)
    by_provider_stmt = by_provider_stmt.group_by(TokenUsage.provider)
    by_provider_q = await session.execute(by_provider_stmt)
    by_provider = {r.provider: {"calls": r.calls, "tokens": int(r.tokens)} for r in by_provider_q.all()}

    # By caller
    by_caller_stmt = select(
        TokenUsage.caller,
        func.count(TokenUsage.id).label("calls"),
        func.coalesce(func.sum(TokenUsage.total_tokens), 0).label("tokens"),
    )
    if filters:
        by_caller_stmt = by_caller_stmt.where(*filters)
    by_caller_stmt = by_caller_stmt.group_by(TokenUsage.caller)
    by_caller_q = await session.execute(by_caller_stmt)
    by_caller = {r.caller: {"calls": r.calls, "tokens": int(r.tokens)} for r in by_caller_q.all()}

    # By model
    by_model_stmt = select(
        TokenUsage.model,
        func.count(TokenUsage.id).label("calls"),
        func.coalesce(func.sum(TokenUsage.total_tokens), 0).label("tokens"),
    )
    if filters:
        by_model_stmt = by_model_stmt.where(*filters)
    by_model_stmt = by_model_stmt.group_by(TokenUsage.model)
    by_model_q = await session.execute(by_model_stmt)
    by_model = {r.model: {"calls": r.calls, "tokens": int(r.tokens)} for r in by_model_q.all()}

    return {
        "total_calls": row.calls,
        "total_prompt_tokens": int(row.prompt),
        "total_completion_tokens": int(row.completion),
        "total_tokens": int(row.total),
        "by_provider": by_provider,
        "by_caller": by_caller,
        "by_model": by_model,
        "error_count": row.errors,
    }


@router.get("/llm-endpoints", response_model=LLMEndpointHealth)
async def get_llm_endpoint_health(
    window_seconds: int = Query(
        default=60,
        ge=1,
        le=3600,
        description="Rolling window in seconds for per-endpoint activity.",
    ),
) -> dict[str, Any]:
    """Get per-key LLM endpoint health and recent-window counters."""
    from scholarpath.llm.client import get_llm_client

    llm = get_llm_client()
    return await llm.endpoint_health(window_seconds=window_seconds)


@router.get("/recent", response_model=list[UsageEntry])
async def get_recent_usage(
    session: SessionDep,
    limit: int = Query(50, ge=1, le=500),
) -> list:
    """Get recent token usage entries."""
    stmt = (
        select(TokenUsage)
        .order_by(TokenUsage.created_at.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())
