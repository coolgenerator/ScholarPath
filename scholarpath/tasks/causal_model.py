"""Celery tasks for causal data pipeline and model lifecycle."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import select

from scholarpath.causal_engine.training import promote_model, train_full_graph_model
from scholarpath.db.models import CausalDatasetVersion, CausalShadowComparison
from scholarpath.db.session import async_session_factory
from scholarpath.evals.causal_gold_live import run_causal_gold_eval
from scholarpath.evals.causal_rollout_quality import run_causal_rollout_quality_gate
from scholarpath.tasks.celery_app import celery_app


@celery_app.task(name="scholarpath.tasks.causal_model.causal_ingest_official_facts")
def causal_ingest_official_facts(
    school_names: list[str],
    cycle_year: int | None = None,
    fields: list[str] | None = None,
    run_id: str | None = None,
) -> dict:
    return asyncio.run(
        _ingest_official_facts_async(
            school_names=school_names,
            cycle_year=cycle_year or datetime.now(timezone.utc).year,
            fields=fields,
            run_id=run_id or f"causal-ingest-{uuid4().hex[:8]}",
        )
    )


@celery_app.task(name="scholarpath.tasks.causal_model.causal_ingest_ipeds_college_navigator")
def causal_ingest_ipeds_college_navigator(
    top_schools: int = 1000,
    years: int = 5,
    selection_metric: str = "applicants_total",
    run_id: str | None = None,
) -> dict:
    return asyncio.run(
        _ingest_ipeds_college_navigator_async(
            top_schools=top_schools,
            years=years,
            selection_metric=selection_metric,
            run_id=run_id or f"causal-ipeds-{uuid4().hex[:8]}",
        )
    )


async def _ingest_ipeds_college_navigator_async(
    *,
    top_schools: int,
    years: int,
    selection_metric: str,
    run_id: str,
) -> dict:
    from scholarpath.services.causal_data_service import ingest_ipeds_school_pool

    async with async_session_factory() as session:
        result = await ingest_ipeds_school_pool(
            session,
            run_id=run_id,
            top_schools=top_schools,
            years=years,
            selection_metric=selection_metric,
        )
        await session.commit()
        return result


@celery_app.task(name="scholarpath.tasks.causal_model.causal_ingest_common_app_trends")
def causal_ingest_common_app_trends(
    years: int = 5,
    run_id: str | None = None,
) -> dict:
    return asyncio.run(
        _ingest_common_app_trends_async(
            years=years,
            run_id=run_id or f"causal-common-app-{uuid4().hex[:8]}",
        )
    )


async def _ingest_common_app_trends_async(
    *,
    years: int,
    run_id: str,
) -> dict:
    from scholarpath.services.causal_data_service import ingest_common_app_trends

    async with async_session_factory() as session:
        result = await ingest_common_app_trends(
            session,
            run_id=run_id,
            years=years,
        )
        await session.commit()
        return result


async def _ingest_official_facts_async(
    *,
    school_names: list[str],
    cycle_year: int,
    fields: list[str] | None,
    run_id: str,
) -> dict:
    from scholarpath.services.causal_data_service import ingest_official_facts

    async with async_session_factory() as session:
        result = await ingest_official_facts(
            session,
            school_names=school_names,
            cycle_year=cycle_year,
            fields=fields,
            run_id=run_id,
        )
        await session.commit()
        return result


@celery_app.task(name="scholarpath.tasks.causal_model.causal_ingest_admission_events")
def causal_ingest_admission_events(events: list[dict]) -> dict:
    return asyncio.run(_ingest_admission_events_async(events))


async def _ingest_admission_events_async(events: list[dict]) -> dict:
    from scholarpath.services.causal_data_service import register_admission_event

    inserted = 0
    errors: list[dict[str, str]] = []
    async with async_session_factory() as session:
        for event in events:
            try:
                await register_admission_event(
                    session,
                    student_id=str(event["student_id"]),
                    school_id=str(event["school_id"]),
                    cycle_year=int(event["cycle_year"]),
                    major_bucket=event.get("major_bucket"),
                    stage=str(event["stage"]),
                    happened_at=event.get("happened_at"),
                    evidence_ref=str(event["evidence_ref"]) if event.get("evidence_ref") else None,
                    source_name=str(event.get("source_name") or "batch"),
                    metadata=event.get("metadata"),
                )
                inserted += 1
            except Exception as exc:
                errors.append({"event": str(event), "error": str(exc)})
        await session.commit()
    return {"status": "ok", "inserted": inserted, "errors": errors}


@celery_app.task(name="scholarpath.tasks.causal_model.causal_clean_and_judge_facts")
def causal_clean_and_judge_facts(
    run_id: str | None = None,
    limit: int = 200,
) -> dict:
    return asyncio.run(
        _clean_and_judge_facts_async(
            run_id=run_id or f"causal-clean-{uuid4().hex[:8]}",
            limit=limit,
        )
    )


async def _clean_and_judge_facts_async(*, run_id: str, limit: int) -> dict:
    from scholarpath.services.causal_data_service import reprocess_quarantine

    async with async_session_factory() as session:
        result = await reprocess_quarantine(session, run_id=run_id, limit=limit)
        await session.commit()
        return result


@celery_app.task(name="scholarpath.tasks.causal_model.causal_build_dataset_version")
def causal_build_dataset_version(
    version: str | None = None,
    lookback_days: int = 540,
    include_proxy: bool = True,
    min_true_per_outcome: int = 100,
) -> dict:
    return asyncio.run(
        _build_dataset_version_async(
            version=version or f"causal-dataset-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}",
            lookback_days=lookback_days,
            include_proxy=include_proxy,
            min_true_per_outcome=min_true_per_outcome,
        )
    )


async def _build_dataset_version_async(
    *,
    version: str,
    lookback_days: int,
    include_proxy: bool,
    min_true_per_outcome: int,
) -> dict:
    from scholarpath.services.causal_data_service import build_dataset_version

    async with async_session_factory() as session:
        result = await build_dataset_version(
            session,
            version=version,
            lookback_days=lookback_days,
            include_proxy=include_proxy,
            min_true_per_outcome=min_true_per_outcome,
        )
        await session.commit()
        return result


@celery_app.task(name="scholarpath.tasks.causal_model.causal_dataset_quality_gate")
def causal_dataset_quality_gate(
    run_id: str | None = None,
    dataset_version: str | None = None,
    metrics: dict | None = None,
) -> dict:
    return asyncio.run(
        _dataset_quality_gate_async(
            run_id=run_id or f"causal-gate-{uuid4().hex[:8]}",
            dataset_version=dataset_version,
            metrics=metrics or {},
        )
    )


async def _dataset_quality_gate_async(
    *,
    run_id: str,
    dataset_version: str | None,
    metrics: dict,
) -> dict:
    from scholarpath.services.causal_data_service import run_mini_gate

    async with async_session_factory() as session:
        result = await run_mini_gate(
            session,
            run_id=run_id,
            dataset_version=dataset_version,
            metrics=metrics,
        )
        await session.commit()
        return result


@celery_app.task(name="scholarpath.tasks.causal_model.causal_train_from_dataset")
def causal_train_from_dataset(
    dataset_version: str | None = None,
    profile: str = "high_quality",
    lookback_days: int = 540,
    min_rows_per_outcome: int = 200,
    calibration_enabled: bool = True,
) -> dict:
    return asyncio.run(
        train_full_graph_model(
            dataset_version=dataset_version,
            profile=profile,
            lookback_days=lookback_days,
            min_rows_per_outcome=min_rows_per_outcome,
            calibration_enabled=calibration_enabled,
        )
    )


# ---------------------------------------------------------------------------
# Compatibility task names used by previous rollout scripts
# ---------------------------------------------------------------------------


@celery_app.task(name="scholarpath.tasks.causal_model.causal_train_full_graph")
def causal_train_full_graph(
    dataset_version: str | None = None,
    profile: str = "high_quality",
    lookback_days: int = 540,
    min_rows_per_outcome: int = 200,
    calibration_enabled: bool = True,
) -> dict:
    return causal_train_from_dataset(
        dataset_version=dataset_version,
        profile=profile,
        lookback_days=lookback_days,
        min_rows_per_outcome=min_rows_per_outcome,
        calibration_enabled=calibration_enabled,
    )


@celery_app.task(name="scholarpath.tasks.causal_model.causal_promote_model")
def causal_promote_model(model_version: str) -> dict:
    return asyncio.run(promote_model(model_version=model_version))


@celery_app.task(name="scholarpath.tasks.causal_model.causal_shadow_audit")
def causal_shadow_audit(window_hours: int = 24) -> dict:
    return asyncio.run(_shadow_audit_async(window_hours=window_hours))


async def _shadow_audit_async(*, window_hours: int) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, window_hours))
    async with async_session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(CausalShadowComparison).where(
                        CausalShadowComparison.created_at >= cutoff,
                    )
                )
            )
            .scalars()
            .all()
        )
    total = len(rows)
    fallback = sum(1 for row in rows if row.fallback_used)
    return {
        "status": "ok",
        "window_hours": window_hours,
        "rows": total,
        "fallback_rate": round((fallback / total), 4) if total else 0.0,
    }


@celery_app.task(name="scholarpath.tasks.causal_model.causal_rollout_quality_gate")
def causal_rollout_quality_gate(
    window_hours: int = 24,
    target_percent: int = 100,
    min_rows: int = 3,
) -> dict:
    return asyncio.run(
        _run_rollout_quality_gate_async(
            window_hours=window_hours,
            target_percent=target_percent,
            min_rows=min_rows,
        )
    )


async def _run_rollout_quality_gate_async(
    *,
    window_hours: int,
    target_percent: int,
    min_rows: int,
) -> dict:
    report = await run_causal_rollout_quality_gate(
        window_hours=window_hours,
        target_percent=target_percent,
        min_rows=min_rows,
    )
    return report.to_dict()


@celery_app.task(name="scholarpath.tasks.causal_model.causal_daily_gold_eval")
def causal_daily_gold_eval(
    dataset_version: str | None = None,
    sample_size: int = 40,
    judge_enabled: bool = True,
) -> dict:
    return asyncio.run(
        _daily_gold_eval_async(
            dataset_version=dataset_version,
            sample_size=sample_size,
            judge_enabled=judge_enabled,
        )
    )


async def _daily_gold_eval_async(
    dataset_version: str | None,
    sample_size: int,
    judge_enabled: bool,
) -> dict:
    async with async_session_factory() as session:
        stmt = select(CausalDatasetVersion)
        if dataset_version:
            stmt = stmt.where(CausalDatasetVersion.version == dataset_version)
        stmt = stmt.order_by(CausalDatasetVersion.updated_at.desc()).limit(1)
        row = (await session.execute(stmt)).scalars().first()
        if row is None:
            return {"status": "partial", "reason": "no_dataset_version"}
    report = await run_causal_gold_eval(
        dataset_path="scholarpath/evals/datasets/causal_gold_v1.json",
        judge_enabled=judge_enabled,
        sample_size=sample_size,
        max_rpm_total=180,
        eval_run_id=f"daily-gold-{uuid4().hex[:8]}",
    )
    payload = report.to_dict()
    payload["dataset_version"] = row.version
    payload["mini_gate_passed"] = row.mini_gate_passed
    payload["truth_ratio_by_outcome"] = row.truth_ratio_by_outcome or {}
    return payload
