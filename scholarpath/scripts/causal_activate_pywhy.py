"""Activate a pywhy model through repeatable data-seed + train + promote flow."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, select

from scholarpath.causal_engine.training import promote_model, train_full_graph_model
from scholarpath.db.models import (
    AdmissionEvent,
    CanonicalFact,
    CausalDatasetVersion,
    CausalFeatureSnapshot,
    CausalModelRegistry,
    CausalOutcomeEvent,
    CausalShadowComparison,
    EvidenceArtifact,
    FactLineage,
    FactQuarantine,
    School,
    Student,
)
from scholarpath.db.session import async_session_factory
from scholarpath.evals.causal_gold_live import load_gold_dataset


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed causal assets and activate one pywhy model.",
    )
    parser.add_argument(
        "--dataset",
        default="scholarpath/evals/datasets/causal_gold_v1.json",
        help="Gold dataset path used for seeding (default: causal_gold_v1.json).",
    )
    parser.add_argument(
        "--student-id",
        default="",
        help="Optional student_id for seeding; default picks first student.",
    )
    parser.add_argument(
        "--seed-cases",
        type=int,
        default=40,
        help="How many cases to seed into causal assets (default: 40).",
    )
    parser.add_argument(
        "--reset-causal-assets",
        dest="reset_causal_assets",
        action="store_true",
        default=False,
        help="Clear causal asset tables before seeding.",
    )
    parser.add_argument(
        "--bootstrap-iters",
        type=int,
        default=100,
        help="Recorded in metadata only; compatibility with previous pipeline.",
    )
    parser.add_argument(
        "--stability-threshold",
        type=float,
        default=0.7,
        help="Recorded in metadata only; compatibility with previous pipeline.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="Training lookback window days (default: 365).",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="Celery polling interval in seconds.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=900,
        help="Timeout for train/promote wait when using celery.",
    )
    parser.add_argument(
        "--use-celery",
        dest="use_celery",
        action="store_true",
        default=True,
        help="Use celery path first (default: enabled).",
    )
    parser.add_argument(
        "--no-use-celery",
        dest="use_celery",
        action="store_false",
        help="Disable celery path and run training locally.",
    )
    return parser


async def _precheck(student_id: str | None) -> dict[str, Any]:
    missing_deps = [
        name
        for name in ("dowhy", "econml", "causallearn")
        if importlib.util.find_spec(name) is None
    ]
    async with async_session_factory() as session:
        student_count = int((await session.scalar(select(func.count()).select_from(Student))) or 0)
        school_count = int((await session.scalar(select(func.count()).select_from(School))) or 0)
        student_exists = True
        if student_id:
            student_exists = bool(await session.get(Student, student_id))

    errors: list[str] = []
    if missing_deps:
        errors.append(f"missing dependencies: {missing_deps}")
    if student_count < 1:
        errors.append("need at least 1 student")
    if school_count < 40:
        errors.append("need at least 40 schools")
    if student_id and not student_exists:
        errors.append(f"student_id {student_id} not found")
    return {
        "ok": not errors,
        "errors": errors,
        "student_count": student_count,
        "school_count": school_count,
        "missing_dependencies": missing_deps,
    }


async def _reset_assets() -> dict[str, int]:
    async with async_session_factory() as session:
        counts = {
            "fact_lineage": await _delete_all(session, FactLineage),
            "canonical_facts": await _delete_all(session, CanonicalFact),
            "fact_quarantine": await _delete_all(session, FactQuarantine),
            "admission_events": await _delete_all(session, AdmissionEvent),
            "evidence_artifacts": await _delete_all(session, EvidenceArtifact),
            "causal_shadow_comparisons": await _delete_all(session, CausalShadowComparison),
            "causal_model_registry": await _delete_all(session, CausalModelRegistry),
            "causal_outcome_events": await _delete_all(session, CausalOutcomeEvent),
            "causal_feature_snapshots": await _delete_all(session, CausalFeatureSnapshot),
            "causal_dataset_versions": await _delete_all(session, CausalDatasetVersion),
        }
        await session.commit()
    return counts


async def _delete_all(session, model) -> int:
    result = await session.execute(delete(model))
    return int(result.rowcount or 0)


async def _seed_assets(
    *,
    dataset_path: Path,
    seed_cases: int,
    student_id: str | None,
) -> dict[str, Any]:
    cases = load_gold_dataset(dataset_path)
    if not cases:
        return {"status": "failed_precondition", "reason": "dataset_empty"}
    cases = cases[: max(1, seed_cases)]

    async with async_session_factory() as session:
        if student_id:
            student = await session.get(Student, student_id)
        else:
            student = (
                (await session.execute(select(Student).order_by(Student.created_at.asc()).limit(1)))
                .scalars()
                .first()
            )
        if student is None:
            return {"status": "failed_precondition", "reason": "no_student"}

        schools = (
            (await session.execute(select(School).order_by(School.name.asc()).limit(max(40, len(cases)))))
            .scalars()
            .all()
        )
        if len(schools) < 1:
            return {"status": "failed_precondition", "reason": "no_school"}

        seeded_snapshots = 0
        seeded_outcomes = 0
        now = datetime.now(timezone.utc)
        for index, case in enumerate(cases):
            school = schools[index % len(schools)]
            observed_at = now - timedelta(days=index)
            session.add(
                CausalFeatureSnapshot(
                    student_id=student.id,
                    school_id=school.id,
                    offer_id=None,
                    context=case.context,
                    feature_payload={
                        "student_features": case.student_features,
                        "school_features": case.school_features,
                        "interaction_features": case.interaction_features,
                        "cohort": case.cohort,
                        "case_id": case.case_id,
                    },
                    observed_at=observed_at,
                )
            )
            seeded_snapshots += 1
            for outcome, value in case.gold_outcomes.items():
                label_type = "true" if outcome == "admission_probability" else case.label_type
                confidence = 0.99 if label_type == "true" else 0.85
                session.add(
                    CausalOutcomeEvent(
                        student_id=student.id,
                        school_id=school.id,
                        offer_id=None,
                        outcome_name=outcome,
                        outcome_value=float(value),
                        label_type=label_type,
                        label_confidence=confidence,
                        source="causal_gold_seed",
                        observed_at=observed_at,
                        metadata_={
                            "case_id": case.case_id,
                            "cohort": case.cohort,
                        },
                    )
                )
                seeded_outcomes += 1
        await session.commit()

    return {
        "status": "ok",
        "seeded_snapshots": seeded_snapshots,
        "seeded_outcomes": seeded_outcomes,
    }


async def _run_train_and_promote(
    *,
    lookback_days: int,
    timeout_seconds: int,
    poll_interval_seconds: float,
    use_celery: bool,
) -> dict[str, Any]:
    if use_celery:
        try:
            from scholarpath.tasks.causal_model import causal_promote_model, causal_train_full_graph

            train_job = causal_train_full_graph.delay(
                None,
                "high_quality",
                lookback_days,
                200,
                True,
            )
            train_payload = await _wait_celery(train_job.id, timeout_seconds, poll_interval_seconds)
            if train_payload.get("status") != "ok":
                return {
                    "status": "failed",
                    "stage": "train",
                    "train_task_id": train_job.id,
                    "train_result": train_payload,
                    "execution_mode": "celery",
                }
            model_version = str(train_payload.get("model_version") or "")
            if not model_version:
                return {
                    "status": "failed",
                    "stage": "train",
                    "train_task_id": train_job.id,
                    "train_result": train_payload,
                    "execution_mode": "celery",
                    "reason": "missing_model_version",
                }
            promote_job = causal_promote_model.delay(model_version)
            promote_payload = await _wait_celery(promote_job.id, timeout_seconds, poll_interval_seconds)
            return {
                "status": "ok" if promote_payload.get("status") == "ok" else "failed",
                "execution_mode": "celery",
                "train_task_id": train_job.id,
                "promote_task_id": promote_job.id,
                "model_version": model_version,
                "train_result": train_payload,
                "promote_result": promote_payload,
            }
        except Exception as exc:
            # fall through to local path
            fallback_error = str(exc)
        else:
            fallback_error = ""
    else:
        fallback_error = ""

    train_payload = await train_full_graph_model(
        dataset_version=None,
        profile="high_quality",
        lookback_days=lookback_days,
        min_rows_per_outcome=200,
        calibration_enabled=True,
    )
    if train_payload.get("status") != "ok":
        return {
            "status": "failed",
            "stage": "train",
            "execution_mode": "local",
            "train_result": train_payload,
            "celery_error": fallback_error or None,
        }
    model_version = str(train_payload.get("model_version") or "")
    promote_payload = await promote_model(model_version=model_version)
    return {
        "status": "ok" if promote_payload.get("status") == "ok" else "failed",
        "execution_mode": "local",
        "model_version": model_version,
        "train_result": train_payload,
        "promote_result": promote_payload,
        "celery_error": fallback_error or None,
    }


async def _wait_celery(task_id: str, timeout_seconds: int, poll_interval_seconds: float) -> dict[str, Any]:
    from scholarpath.tasks import celery_app

    deadline = datetime.now(timezone.utc) + timedelta(seconds=max(10, timeout_seconds))
    while datetime.now(timezone.utc) < deadline:
        result = celery_app.AsyncResult(task_id)
        if result.status == "SUCCESS":
            payload = result.result
            return payload if isinstance(payload, dict) else {"status": "ok", "result": payload}
        if result.status == "FAILURE":
            return {"status": "failed", "error": str(result.result)}
        await asyncio.sleep(max(0.2, poll_interval_seconds))
    return {"status": "failed", "error": "timeout"}


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    precheck = await _precheck(args.student_id or None)
    if not precheck["ok"]:
        payload = {"status": "failed_precondition", "precheck": precheck}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return payload

    reset_counts = None
    if args.reset_causal_assets:
        reset_counts = await _reset_assets()

    seed_payload = await _seed_assets(
        dataset_path=Path(args.dataset),
        seed_cases=args.seed_cases,
        student_id=args.student_id or None,
    )
    if seed_payload.get("status") != "ok":
        payload = {"status": "failed", "stage": "seed", "seed_result": seed_payload}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return payload

    train_payload = await _run_train_and_promote(
        lookback_days=args.lookback_days,
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
        use_celery=args.use_celery,
    )
    status = "ok" if train_payload.get("status") == "ok" else "failed"
    payload = {
        "status": status,
        "precheck": precheck,
        "reset_counts": reset_counts,
        "seeded_snapshots": seed_payload.get("seeded_snapshots", 0),
        "seeded_outcomes": seed_payload.get("seeded_outcomes", 0),
        "bootstrap_iters": args.bootstrap_iters,
        "stability_threshold": args.stability_threshold,
        "lookback_days": args.lookback_days,
        "active_model_version": train_payload.get("model_version"),
        "train_task_id": train_payload.get("train_task_id"),
        "promote_task_id": train_payload.get("promote_task_id"),
        "execution_mode": train_payload.get("execution_mode"),
        "train_result": train_payload.get("train_result"),
        "promote_result": train_payload.get("promote_result"),
        "celery_error": train_payload.get("celery_error"),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
