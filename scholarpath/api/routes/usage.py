"""Token usage tracking routes."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel
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


@router.get("/summary", response_model=UsageSummary)
async def get_usage_summary(session: SessionDep) -> dict:
    """Get aggregated token usage summary."""
    # Total counts
    totals = await session.execute(
        select(
            func.count(TokenUsage.id).label("calls"),
            func.coalesce(func.sum(TokenUsage.prompt_tokens), 0).label("prompt"),
            func.coalesce(func.sum(TokenUsage.completion_tokens), 0).label("completion"),
            func.coalesce(func.sum(TokenUsage.total_tokens), 0).label("total"),
            func.count(TokenUsage.error).label("errors"),
        )
    )
    row = totals.one()

    # By provider
    by_provider_q = await session.execute(
        select(
            TokenUsage.provider,
            func.count(TokenUsage.id).label("calls"),
            func.coalesce(func.sum(TokenUsage.total_tokens), 0).label("tokens"),
        ).group_by(TokenUsage.provider)
    )
    by_provider = {r.provider: {"calls": r.calls, "tokens": int(r.tokens)} for r in by_provider_q.all()}

    # By caller
    by_caller_q = await session.execute(
        select(
            TokenUsage.caller,
            func.count(TokenUsage.id).label("calls"),
            func.coalesce(func.sum(TokenUsage.total_tokens), 0).label("tokens"),
        ).group_by(TokenUsage.caller)
    )
    by_caller = {r.caller: {"calls": r.calls, "tokens": int(r.tokens)} for r in by_caller_q.all()}

    # By model
    by_model_q = await session.execute(
        select(
            TokenUsage.model,
            func.count(TokenUsage.id).label("calls"),
            func.coalesce(func.sum(TokenUsage.total_tokens), 0).label("tokens"),
        ).group_by(TokenUsage.model)
    )
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
