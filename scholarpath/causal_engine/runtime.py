"""Causal runtime with shadow and fallback control."""

from __future__ import annotations

import random
from dataclasses import dataclass

from scholarpath.config import settings
from scholarpath.causal_engine.legacy_engine import LegacyCausalEngine
from scholarpath.causal_engine.pywhy_engine import PyWhyCausalEngine
from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalExplainResult,
    CausalInterventionResult,
    CausalRequestContext,
)
from scholarpath.db.models import CausalShadowComparison
from scholarpath.db.session import async_session_factory


@dataclass(slots=True)
class RuntimeConfig:
    mode: str
    pywhy_primary_percent: int


class CausalRuntime:
    """Routes requests between legacy and pywhy engines."""

    def __init__(self, config: RuntimeConfig) -> None:
        self._config = config
        self._legacy = LegacyCausalEngine()
        self._pywhy = PyWhyCausalEngine()

    async def estimate(self, request: CausalRequestContext) -> CausalEstimateResult:
        mode = self._config.mode
        if mode == "legacy":
            return await self._legacy.estimate(request)
        if mode == "pywhy":
            return await self._estimate_primary_pywhy(request)
        # shadow/default: pywhy primary with legacy shadow comparison.
        use_pywhy = random.randint(1, 100) <= max(0, min(100, self._config.pywhy_primary_percent))
        if use_pywhy:
            primary = await self._estimate_primary_pywhy(request)
            await self._shadow_compare(request, primary_engine="pywhy")
            return primary
        primary = await self._legacy.estimate(request)
        await self._shadow_compare(request, primary_engine="legacy")
        return primary

    async def intervene(
        self,
        request: CausalRequestContext,
        interventions: dict[str, float],
    ) -> CausalInterventionResult:
        if self._config.mode == "legacy":
            return await self._legacy.intervene(request, interventions)
        try:
            return await self._pywhy.intervene(request, interventions)
        except Exception as exc:
            fallback = await self._legacy.intervene(request, interventions)
            fallback.fallback_used = True
            fallback.fallback_reason = str(exc)
            return fallback

    async def explain(self, request: CausalRequestContext) -> CausalExplainResult:
        if self._config.mode == "legacy":
            return await self._legacy.explain(request)
        try:
            return await self._pywhy.explain(request)
        except Exception:
            return await self._legacy.explain(request)

    async def _estimate_primary_pywhy(self, request: CausalRequestContext) -> CausalEstimateResult:
        try:
            return await self._pywhy.estimate(request)
        except Exception as exc:
            fallback = await self._legacy.estimate(request)
            fallback.fallback_used = True
            fallback.fallback_reason = str(exc)
            return fallback

    async def _shadow_compare(
        self,
        request: CausalRequestContext,
        *,
        primary_engine: str,
    ) -> None:
        try:
            legacy = await self._legacy.estimate(request)
            pywhy = await self._estimate_primary_pywhy(request)
            diff = {
                key: round(pywhy.scores.get(key, 0.0) - legacy.scores.get(key, 0.0), 6)
                for key in legacy.scores
            }
            async with async_session_factory() as session:
                row = CausalShadowComparison(
                    request_id=str(request.metadata.get("request_id") or request.student_id),
                    context=request.context,
                    student_id=request.student_id,
                    school_id=request.school_id,
                    offer_id=request.offer_id,
                    engine_mode=self._config.mode,
                    causal_model_version=pywhy.model_version,
                    legacy_scores=legacy.scores,
                    pywhy_scores=pywhy.scores,
                    diff_scores=diff,
                    fallback_used=bool(pywhy.fallback_used),
                    fallback_reason=pywhy.fallback_reason,
                    error_json=None,
                )
                session.add(row)
                await session.commit()
        except Exception:
            # Shadow logging is best-effort.
            return


_RUNTIME_SINGLETON: CausalRuntime | None = None


def get_causal_runtime() -> CausalRuntime:
    global _RUNTIME_SINGLETON
    if _RUNTIME_SINGLETON is None:
        mode = str(getattr(settings, "CAUSAL_ENGINE_MODE", "shadow")).strip().lower() or "shadow"
        percent = int(getattr(settings, "CAUSAL_PYWHY_PRIMARY_PERCENT", 100))
        _RUNTIME_SINGLETON = CausalRuntime(
            RuntimeConfig(
                mode=mode,
                pywhy_primary_percent=max(0, min(100, percent)),
            )
        )
    return _RUNTIME_SINGLETON
