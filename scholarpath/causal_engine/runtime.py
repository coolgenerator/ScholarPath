"""Runtime orchestrator for legacy/pywhy/shadow causal execution."""

from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.causal_engine.feature_builder import FeatureBuilder
from scholarpath.causal_engine.legacy_engine import LegacyCausalEngine
from scholarpath.causal_engine.proxy_labels import build_proxy_labels
from scholarpath.causal_engine.pywhy_engine import PyWhyCausalEngine
from scholarpath.causal_engine.types import (
    CausalEstimateResult,
    CausalInterventionResult,
    CausalRequestContext,
)
from scholarpath.config import settings
from scholarpath.db.models import (
    CausalFeatureSnapshot,
    CausalOutcomeEvent,
    CausalShadowComparison,
    Offer,
    School,
    Student,
)

logger = logging.getLogger(__name__)


class CausalRuntime:
    """Unified runtime for causal estimation and interventions."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        model_version_hint: str | None = None,
    ) -> None:
        self._session = session
        self._feature_builder = FeatureBuilder()
        self._legacy = LegacyCausalEngine()
        self._pywhy = PyWhyCausalEngine(
            session=session,
            model_version_hint=str(model_version_hint or settings.CAUSAL_MODEL_VERSION),
        )

    async def _maybe_warmup_pywhy(self, outcomes: list[str]) -> None:
        try:
            await self._pywhy.warmup(outcomes)
        except Exception:
            # Warmup is an optimization; inference-level fallback remains authoritative.
            logger.warning("PyWhy warmup failed; continue with normal inference path", exc_info=True)

    async def _estimate_pywhy(self, ctx: CausalRequestContext, outcomes: list[str]) -> CausalEstimateResult:
        await self._maybe_warmup_pywhy(outcomes)
        return await self._pywhy.estimate(ctx, outcomes)

    async def _intervene_pywhy(
        self,
        ctx: CausalRequestContext,
        interventions: dict[str, float],
        outcomes: list[str],
    ) -> CausalInterventionResult:
        await self._maybe_warmup_pywhy(outcomes)
        return await self._pywhy.intervene(ctx, interventions, outcomes)

    async def estimate(
        self,
        *,
        student: Student,
        school: School | None,
        offer: Offer | None,
        context: str,
        outcomes: list[str],
        request_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[CausalEstimateResult, CausalRequestContext]:
        ctx = self._build_context(
            student=student,
            school=school,
            offer=offer,
            context=context,
            request_id=request_id,
            metadata=metadata,
        )
        await self._persist_snapshot(ctx)
        await self._persist_proxy_outcomes(student=student, school=school, offer=offer)

        mode = (settings.CAUSAL_ENGINE_MODE or "shadow").strip().lower()
        if mode == "legacy":
            result = await self._legacy.estimate(ctx, outcomes)
            return result, ctx

        if mode == "pywhy":
            try:
                result = await self._estimate_pywhy(ctx, outcomes)
                return result, ctx
            except Exception as exc:
                logger.warning("PyWhy estimate failed, fallback to legacy", exc_info=True)
                fallback = await self._legacy.estimate(ctx, outcomes)
                fallback.fallback_used = True
                fallback.fallback_reason = str(exc)
                fallback.metadata["pywhy_error"] = str(exc)
                return fallback, ctx

        use_pywhy_primary, rollout_percent, rollout_bucket = self._resolve_rollout_decision(ctx)
        if use_pywhy_primary:
            pywhy_primary: CausalEstimateResult | None = None
            pywhy_error: Exception | None = None
            try:
                pywhy_primary = await self._estimate_pywhy(ctx, outcomes)
                primary = pywhy_primary
            except Exception as exc:
                pywhy_error = exc
                logger.warning("Rollout pywhy primary estimate failed, fallback to legacy", exc_info=True)
                primary = await self._legacy.estimate(ctx, outcomes)
                primary.fallback_used = True
                primary.fallback_reason = str(exc)
                primary.metadata["pywhy_error"] = str(exc)

            legacy_shadow = primary if pywhy_primary is None else await self._legacy.estimate(ctx, outcomes)
            if settings.CAUSAL_SHADOW_LOGGING:
                await self._log_shadow(
                    ctx=ctx,
                    legacy_result=legacy_shadow,
                    pywhy_result=pywhy_primary,
                    pywhy_error=pywhy_error,
                    engine_mode="shadow_pywhy",
                )

            self._attach_rollout_metadata(
                result=primary,
                rollout_percent=rollout_percent,
                rollout_bucket=rollout_bucket,
                selected_primary=("pywhy" if pywhy_primary is not None else "legacy_fallback"),
            )
            return primary, ctx

        # shadow mode default: serve legacy and compare with pywhy.
        primary = await self._legacy.estimate(ctx, outcomes)
        pywhy_result: CausalEstimateResult | None = None
        pywhy_error: Exception | None = None
        try:
            pywhy_result = await self._estimate_pywhy(ctx, outcomes)
        except Exception as exc:
            pywhy_error = exc
            logger.warning("Shadow pywhy estimate failed", exc_info=True)

        if settings.CAUSAL_SHADOW_LOGGING:
            await self._log_shadow(
                ctx=ctx,
                legacy_result=primary,
                pywhy_result=pywhy_result,
                pywhy_error=pywhy_error,
                engine_mode=("shadow_legacy" if rollout_percent > 0 else "shadow"),
            )

        self._attach_rollout_metadata(
            result=primary,
            rollout_percent=rollout_percent,
            rollout_bucket=rollout_bucket,
            selected_primary="legacy",
        )
        return primary, ctx

    async def intervene(
        self,
        *,
        student: Student,
        school: School | None,
        offer: Offer | None,
        context: str,
        interventions: dict[str, float],
        outcomes: list[str],
        request_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[CausalInterventionResult, CausalRequestContext]:
        ctx = self._build_context(
            student=student,
            school=school,
            offer=offer,
            context=context,
            request_id=request_id,
            metadata=metadata,
        )
        await self._persist_snapshot(ctx)
        await self._persist_proxy_outcomes(student=student, school=school, offer=offer)

        mode = (settings.CAUSAL_ENGINE_MODE or "shadow").strip().lower()
        if mode == "legacy":
            return await self._legacy.intervene(ctx, interventions, outcomes), ctx

        if mode == "pywhy":
            try:
                return await self._intervene_pywhy(ctx, interventions, outcomes), ctx
            except Exception as exc:
                fallback = await self._legacy.intervene(ctx, interventions, outcomes)
                fallback.fallback_used = True
                fallback.fallback_reason = str(exc)
                fallback.metadata["pywhy_error"] = str(exc)
                return fallback, ctx

        use_pywhy_primary, rollout_percent, rollout_bucket = self._resolve_rollout_decision(ctx)
        if use_pywhy_primary:
            pywhy_primary: CausalInterventionResult | None = None
            pywhy_error: Exception | None = None
            try:
                pywhy_primary = await self._intervene_pywhy(ctx, interventions, outcomes)
                primary = pywhy_primary
            except Exception as exc:
                pywhy_error = exc
                logger.warning("Rollout pywhy primary intervention failed, fallback to legacy", exc_info=True)
                primary = await self._legacy.intervene(ctx, interventions, outcomes)
                primary.fallback_used = True
                primary.fallback_reason = str(exc)
                primary.metadata["pywhy_error"] = str(exc)

            legacy_shadow = primary if pywhy_primary is None else await self._legacy.intervene(ctx, interventions, outcomes)
            if settings.CAUSAL_SHADOW_LOGGING:
                await self._log_shadow_intervention(
                    ctx=ctx,
                    legacy_result=legacy_shadow,
                    pywhy_result=pywhy_primary,
                    pywhy_error=pywhy_error,
                    engine_mode="shadow_pywhy",
                )

            self._attach_rollout_metadata(
                result=primary,
                rollout_percent=rollout_percent,
                rollout_bucket=rollout_bucket,
                selected_primary=("pywhy" if pywhy_primary is not None else "legacy_fallback"),
            )
            return primary, ctx

        # shadow default: serve legacy intervention and shadow-run pywhy.
        legacy_res = await self._legacy.intervene(ctx, interventions, outcomes)
        pywhy_res: CausalInterventionResult | None = None
        pywhy_error: Exception | None = None
        try:
            pywhy_res = await self._intervene_pywhy(ctx, interventions, outcomes)
        except Exception as exc:
            pywhy_error = exc
            logger.warning("Shadow pywhy intervention failed", exc_info=True)

        if settings.CAUSAL_SHADOW_LOGGING:
            await self._log_shadow_intervention(
                ctx=ctx,
                legacy_result=legacy_res,
                pywhy_result=pywhy_res,
                pywhy_error=pywhy_error,
                engine_mode=("shadow_legacy" if rollout_percent > 0 else "shadow"),
            )

        self._attach_rollout_metadata(
            result=legacy_res,
            rollout_percent=rollout_percent,
            rollout_bucket=rollout_bucket,
            selected_primary="legacy",
        )
        return legacy_res, ctx

    def _build_context(
        self,
        *,
        student: Student,
        school: School | None,
        offer: Offer | None,
        context: str,
        request_id: str | None,
        metadata: dict[str, Any] | None,
    ) -> CausalRequestContext:
        bundle = self._feature_builder.build(student=student, school=school, offer=offer)
        return CausalRequestContext(
            request_id=request_id or str(uuid.uuid4()),
            context=context,
            student_id=student.id,
            school_id=school.id if school else None,
            offer_id=offer.id if offer else None,
            student_features=bundle.student_features,
            school_features=bundle.school_features,
            interaction_features=bundle.interaction_features,
            metadata=metadata or {},
        )

    def _resolve_rollout_decision(
        self,
        ctx: CausalRequestContext,
    ) -> tuple[bool, int, int | None]:
        rollout_percent = self._normalized_rollout_percent()
        if rollout_percent <= 0:
            return False, 0, None

        bucket = self._rollout_bucket(ctx)
        return bucket < rollout_percent, rollout_percent, bucket

    def _normalized_rollout_percent(self) -> int:
        raw = settings.CAUSAL_PYWHY_PRIMARY_PERCENT
        try:
            value = int(raw)
        except (TypeError, ValueError):
            logger.warning("Invalid CAUSAL_PYWHY_PRIMARY_PERCENT=%r, fallback to 0", raw)
            return 0
        return max(0, min(100, value))

    def _rollout_bucket(self, ctx: CausalRequestContext) -> int:
        stable_key = f"{ctx.student_id}:{ctx.school_id}:{ctx.offer_id}:{ctx.context}"
        digest = hashlib.sha256(stable_key.encode("utf-8")).hexdigest()
        return int(digest[:8], 16) % 100

    def _attach_rollout_metadata(
        self,
        *,
        result: CausalEstimateResult | CausalInterventionResult,
        rollout_percent: int,
        rollout_bucket: int | None,
        selected_primary: str,
    ) -> None:
        payload: dict[str, Any] = {
            "mode": (settings.CAUSAL_ENGINE_MODE or "shadow").strip().lower(),
            "pywhy_primary_percent": rollout_percent,
            "selected_primary": selected_primary,
        }
        if rollout_bucket is not None:
            payload["bucket"] = rollout_bucket
        result.metadata["rollout"] = payload

    async def _persist_snapshot(self, ctx: CausalRequestContext) -> None:
        if ctx.school_id is None:
            return
        payload = {
            "student_features": ctx.student_features,
            "school_features": ctx.school_features,
            "interaction_features": ctx.interaction_features,
            "metadata": ctx.metadata,
        }
        self._session.add(
            CausalFeatureSnapshot(
                student_id=ctx.student_id,
                school_id=ctx.school_id,
                offer_id=ctx.offer_id,
                context=ctx.context,
                feature_payload=payload,
                observed_at=datetime.now(UTC),
            )
        )

    async def _persist_proxy_outcomes(
        self,
        *,
        student: Student,
        school: School | None,
        offer: Offer | None,
    ) -> None:
        if not settings.CAUSAL_PROXY_LABELS_ENABLED or school is None:
            return
        for label in build_proxy_labels(student=student, school=school, offer=offer):
            self._session.add(
                CausalOutcomeEvent(
                    student_id=student.id,
                    school_id=school.id,
                    offer_id=offer.id if offer else None,
                    outcome_name=label.outcome_name,
                    outcome_value=label.outcome_value,
                    label_type=label.label_type,
                    label_confidence=label.label_confidence,
                    source=label.source,
                    observed_at=label.observed_at,
                    metadata_=label.metadata,
                )
            )

    async def _log_shadow(
        self,
        *,
        ctx: CausalRequestContext,
        legacy_result: CausalEstimateResult,
        pywhy_result: CausalEstimateResult | None,
        pywhy_error: Exception | None,
        engine_mode: str,
    ) -> None:
        pywhy_scores = pywhy_result.scores if pywhy_result else {}
        legacy_scores = legacy_result.scores
        diff_scores = {
            key: round(pywhy_scores.get(key, legacy_scores.get(key, 0.0)) - legacy_scores.get(key, 0.0), 6)
            for key in set(legacy_scores) | set(pywhy_scores)
        }
        self._session.add(
            CausalShadowComparison(
                request_id=ctx.request_id,
                context=ctx.context,
                student_id=ctx.student_id,
                school_id=ctx.school_id,
                offer_id=ctx.offer_id,
                engine_mode=engine_mode,
                causal_model_version=(pywhy_result.causal_model_version if pywhy_result else None),
                legacy_scores=legacy_scores,
                pywhy_scores=pywhy_scores,
                diff_scores=diff_scores,
                fallback_used=pywhy_error is not None,
                fallback_reason=(str(pywhy_error) if pywhy_error else None),
                error_json={"pywhy_error": str(pywhy_error)} if pywhy_error else None,
            )
        )

    async def _log_shadow_intervention(
        self,
        *,
        ctx: CausalRequestContext,
        legacy_result: CausalInterventionResult,
        pywhy_result: CausalInterventionResult | None,
        pywhy_error: Exception | None,
        engine_mode: str,
    ) -> None:
        pywhy_scores = pywhy_result.modified_scores if pywhy_result else {}
        legacy_scores = legacy_result.modified_scores
        diff_scores = {
            key: round(pywhy_scores.get(key, legacy_scores.get(key, 0.0)) - legacy_scores.get(key, 0.0), 6)
            for key in set(legacy_scores) | set(pywhy_scores)
        }
        self._session.add(
            CausalShadowComparison(
                request_id=ctx.request_id,
                context=f"{ctx.context}:intervention",
                student_id=ctx.student_id,
                school_id=ctx.school_id,
                offer_id=ctx.offer_id,
                engine_mode=engine_mode,
                causal_model_version=(pywhy_result.causal_model_version if pywhy_result else None),
                legacy_scores=legacy_scores,
                pywhy_scores=pywhy_scores,
                diff_scores=diff_scores,
                fallback_used=pywhy_error is not None,
                fallback_reason=(str(pywhy_error) if pywhy_error else None),
                error_json={"pywhy_error": str(pywhy_error)} if pywhy_error else None,
            )
        )
