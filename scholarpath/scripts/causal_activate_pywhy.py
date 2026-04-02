"""Activate a PyWhy model via deterministic seed + Celery train/promote flow."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, select

from scholarpath.db.models import (
    CausalFeatureSnapshot,
    CausalModelRegistry,
    CausalOutcomeEvent,
    CausalShadowComparison,
    School,
    Student,
)
from scholarpath.db.session import async_session_factory
from scholarpath.evals.causal_gold_live import (
    DEFAULT_CAUSAL_GOLD_DATASET_PATH,
    load_causal_gold_dataset,
)

DEFAULT_OUTPUT_DIR = Path(".benchmarks/causal")


@dataclass
class ActivationContext:
    student_id: uuid.UUID
    school_ids: list[uuid.UUID]
    student_count: int
    school_count: int


def check_pywhy_dependencies() -> list[str]:
    """Fail fast when PyWhy stack is unavailable."""
    required_modules = ("dowhy", "econml", "causallearn")
    missing: list[str] = []
    for module_name in required_modules:
        try:
            importlib.import_module(module_name)
        except Exception as exc:  # pragma: no cover - depends on runtime env
            missing.append(f"{module_name} ({exc})")
    if missing:
        raise RuntimeError(
            "Missing PyWhy dependencies: " + ", ".join(missing),
        )
    return list(required_modules)


def choose_seed_cases(
    *,
    dataset_cases: list[Any],
    seed_cases: int,
) -> list[Any]:
    if seed_cases <= 0:
        raise ValueError("seed_cases must be > 0")
    ordered_cases = sorted(dataset_cases, key=lambda case: case.case_id)
    if seed_cases > len(ordered_cases):
        raise ValueError(
            f"seed_cases must be <= dataset case count ({len(ordered_cases)})",
        )
    return ordered_cases[:seed_cases]


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def build_augmented_seed_cases(
    *,
    seed_cases: list[Any],
    synthetic_multiplier: int,
    rng_seed: int = 42,
) -> list[Any]:
    """Create synthetic variants with monotonic-constrained perturbation."""
    multiplier = max(0, int(synthetic_multiplier))
    if multiplier == 0:
        return list(seed_cases)

    rng = random.Random(rng_seed)
    augmented: list[Any] = list(seed_cases)
    for case in seed_cases:
        base_student = dict(case.student_features)
        base_school = dict(case.school_features)
        base_offer = dict(case.offer_features)
        base_outcomes = dict(case.gold_outcomes)
        base_tolerance = dict(case.gold_tolerance)
        for i in range(multiplier):
            delta_selectivity = rng.uniform(-0.08, 0.08)
            delta_gpa = rng.uniform(-0.06, 0.06)
            delta_budget = rng.uniform(-0.07, 0.07)
            delta_affordability = rng.uniform(-0.08, 0.08)

            student_features = dict(base_student)
            school_features = dict(base_school)
            offer_features = dict(base_offer)
            gold_outcomes = dict(base_outcomes)

            student_features["student_gpa_norm"] = _clip01(
                float(student_features.get("student_gpa_norm", 0.6)) + delta_gpa
            )
            student_features["student_budget_norm"] = _clip01(
                float(student_features.get("student_budget_norm", 0.5)) + delta_budget
            )
            school_features["school_selectivity"] = _clip01(
                float(school_features.get("school_selectivity", 0.5)) + delta_selectivity
            )
            school_features["school_net_price_norm"] = _clip01(
                float(school_features.get("school_net_price_norm", 0.5))
                - 0.4 * delta_budget
                + 0.2 * delta_affordability
            )
            offer_features["affordability_ratio_norm"] = _clip01(
                float(offer_features.get("affordability_ratio_norm", 0.5))
                + delta_affordability
                + 0.35 * delta_budget
            )

            gold_outcomes["admission_probability"] = _clip01(
                float(gold_outcomes.get("admission_probability", 0.5))
                + 0.35 * delta_selectivity
                + 0.25 * delta_gpa
                - 0.10 * delta_affordability
            )
            gold_outcomes["academic_outcome"] = _clip01(
                float(gold_outcomes.get("academic_outcome", 0.5))
                + 0.30 * delta_gpa
                + 0.15 * delta_selectivity
            )
            gold_outcomes["career_outcome"] = _clip01(
                float(gold_outcomes.get("career_outcome", 0.5))
                + 0.18 * delta_selectivity
                + 0.12 * delta_affordability
            )
            gold_outcomes["life_satisfaction"] = _clip01(
                float(gold_outcomes.get("life_satisfaction", 0.5))
                + 0.25 * delta_affordability
                + 0.08 * delta_budget
            )
            gold_outcomes["phd_probability"] = _clip01(
                float(gold_outcomes.get("phd_probability", 0.5))
                + 0.20 * delta_selectivity
                + 0.15 * delta_gpa
            )

            synthetic_case = type(case)(
                case_id=f"{case.case_id}-syn-{i + 1}",
                cohort=case.cohort,
                context=f"{case.context}:synthetic",
                student_features=student_features,
                school_features=school_features,
                offer_features=offer_features,
                gold_outcomes=gold_outcomes,
                gold_tolerance=base_tolerance,
                label_type="proxy",
                intervention_checks=list(case.intervention_checks),
            )
            augmented.append(synthetic_case)

    return augmented


def inspect_active_queues(celery_app: Any) -> dict[str, list[str]]:
    inspector = celery_app.control.inspect(timeout=5)
    if inspector is None:
        return {}
    payload = inspector.active_queues() or {}
    out: dict[str, list[str]] = {}
    for worker, rows in payload.items():
        names: list[str] = []
        for item in rows or []:
            if isinstance(item, dict):
                queue_name = str(item.get("name") or "").strip()
                if queue_name:
                    names.append(queue_name)
        out[str(worker)] = names
    return out


def ensure_celery_queue_available(queue_snapshot: dict[str, list[str]], *, queue_name: str) -> None:
    if not queue_snapshot:
        raise RuntimeError("No active celery worker queue info available")
    flattened = {name for queues in queue_snapshot.values() for name in queues}
    if queue_name not in flattened:
        raise RuntimeError(f"Celery workers do not listen on queue '{queue_name}'")


async def ensure_seed_prerequisites(
    *,
    student_id: str | None,
    seed_cases: int,
) -> ActivationContext:
    async with async_session_factory() as session:
        student_count = int(
            (
                await session.execute(
                    select(func.count()).select_from(Student),
                )
            ).scalar_one()
            or 0
        )
        school_count = int(
            (
                await session.execute(
                    select(func.count()).select_from(School),
                )
            ).scalar_one()
            or 0
        )

        if student_count < 1:
            raise RuntimeError("Need at least 1 student before activation seeding")
        if school_count < 40:
            raise RuntimeError("Need at least 40 schools before activation seeding")
        if school_count < seed_cases:
            raise RuntimeError(
                f"Need at least {seed_cases} schools for seeding; found {school_count}",
            )

        selected_student: Student | None
        if student_id:
            try:
                requested_id = uuid.UUID(str(student_id))
            except ValueError as exc:
                raise RuntimeError(f"Invalid student_id: {student_id}") from exc
            selected_student = await session.get(Student, requested_id)
            if selected_student is None:
                raise RuntimeError(f"student_id not found: {student_id}")
        else:
            selected_student = (
                (
                    await session.execute(
                        select(Student).order_by(Student.created_at.asc()).limit(1),
                    )
                )
                .scalars()
                .first()
            )
            if selected_student is None:
                raise RuntimeError("No student available for seed data")

        seed_schools = (
            (
                await session.execute(
                    select(School.id).order_by(School.name.asc()).limit(seed_cases),
                )
            )
            .scalars()
            .all()
        )
        school_ids = [sid for sid in seed_schools]
        if len(school_ids) != seed_cases:
            raise RuntimeError(
                f"Failed to pick {seed_cases} schools for seed; got {len(school_ids)}",
            )

        return ActivationContext(
            student_id=selected_student.id,
            school_ids=school_ids,
            student_count=student_count,
            school_count=school_count,
        )


async def reset_causal_assets() -> dict[str, int]:
    async with async_session_factory() as session:
        shadow_deleted = await session.execute(delete(CausalShadowComparison))
        outcome_deleted = await session.execute(delete(CausalOutcomeEvent))
        snapshot_deleted = await session.execute(delete(CausalFeatureSnapshot))
        model_deleted = await session.execute(delete(CausalModelRegistry))
        await session.commit()
    return {
        "causal_shadow_comparisons": int(shadow_deleted.rowcount or 0),
        "causal_outcome_events": int(outcome_deleted.rowcount or 0),
        "causal_feature_snapshots": int(snapshot_deleted.rowcount or 0),
        "causal_model_registry": int(model_deleted.rowcount or 0),
    }


async def seed_training_assets(
    *,
    student_id: uuid.UUID,
    school_ids: list[uuid.UUID],
    seed_cases: list[Any],
    synthetic_proxy_label_confidence: float = 0.55,
    real_proxy_label_confidence: float = 0.7,
) -> dict[str, int]:
    inserted_snapshots = 0
    inserted_outcomes = 0
    synthetic_snapshots = 0
    synthetic_outcomes = 0
    now = datetime.now(UTC)
    async with async_session_factory() as session:
        for idx, case in enumerate(seed_cases):
            school_id = school_ids[idx % len(school_ids)]
            case_id = str(case.case_id)
            is_synthetic = case_id.find("-syn-") >= 0 or ":synthetic" in str(case.context)
            snapshot_context = "causal_seed_syn" if is_synthetic else "causal_seed_real"
            session.add(
                CausalFeatureSnapshot(
                    student_id=student_id,
                    school_id=school_id,
                    offer_id=None,
                    context=snapshot_context,
                    feature_payload={
                        "student_features": dict(case.student_features),
                        "school_features": dict(case.school_features),
                        "interaction_features": dict(case.offer_features),
                        "metadata": {
                            "seed_case_id": case_id,
                            "seed_cohort": case.cohort,
                            "data_origin": "synthetic" if is_synthetic else "real",
                        },
                    },
                    observed_at=now,
                )
            )
            inserted_snapshots += 1
            if is_synthetic:
                synthetic_snapshots += 1

            label_confidence = 0.9 if case.label_type == "true" else (0.55 if is_synthetic else 0.7)
            if case.label_type != "true":
                label_confidence = (
                    _clip01(float(synthetic_proxy_label_confidence))
                    if is_synthetic
                    else _clip01(float(real_proxy_label_confidence))
                )
            for outcome_name, outcome_value in case.gold_outcomes.items():
                session.add(
                    CausalOutcomeEvent(
                        student_id=student_id,
                        school_id=school_id,
                        offer_id=None,
                        outcome_name=str(outcome_name),
                        outcome_value=float(outcome_value),
                        label_type=case.label_type,
                        label_confidence=label_confidence,
                        source="causal_gold_seed_synthetic" if is_synthetic else "causal_gold_seed",
                        observed_at=now,
                        metadata_={
                            "seed_case_id": case_id,
                            "seed_stage": "activation_seed",
                            "data_origin": "synthetic" if is_synthetic else "real",
                        },
                    )
                )
                inserted_outcomes += 1
                if is_synthetic:
                    synthetic_outcomes += 1
        await session.commit()

    return {
        "snapshots": inserted_snapshots,
        "outcomes": inserted_outcomes,
        "snapshots_real": inserted_snapshots - synthetic_snapshots,
        "snapshots_synthetic": synthetic_snapshots,
        "outcomes_real": inserted_outcomes - synthetic_outcomes,
        "outcomes_synthetic": synthetic_outcomes,
    }


async def wait_for_task_result(
    celery_app: Any,
    *,
    task_id: str,
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + float(timeout_seconds)
    while True:
        async_result = celery_app.AsyncResult(task_id)
        status = str(async_result.status or "PENDING")
        if status == "SUCCESS":
            result_payload = async_result.result
            if isinstance(result_payload, dict):
                return result_payload
            return {"value": result_payload}
        if status == "REVOKED":
            raise RuntimeError(f"Celery task revoked ({task_id})")
        if status == "FAILURE":
            raise RuntimeError(f"Celery task failed ({task_id}): {async_result.result}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Celery task timeout ({task_id}, status={status})")
        await asyncio.sleep(max(0.1, float(poll_interval_seconds)))


async def ensure_single_active_model() -> str:
    async with async_session_factory() as session:
        active_versions = (
            (
                await session.execute(
                    select(CausalModelRegistry.model_version)
                    .where(CausalModelRegistry.is_active.is_(True))
                    .order_by(CausalModelRegistry.updated_at.desc()),
                )
            )
            .scalars()
            .all()
        )
    if len(active_versions) != 1:
        raise RuntimeError(
            f"Expected exactly one active model, found {len(active_versions)}",
        )
    return str(active_versions[0])


async def run_activation(args: argparse.Namespace) -> dict[str, Any]:
    required_modules = check_pywhy_dependencies()

    from scholarpath.tasks import celery_app

    queue_snapshot = await asyncio.to_thread(inspect_active_queues, celery_app)
    ensure_celery_queue_available(queue_snapshot, queue_name="causal_train")

    dataset = load_causal_gold_dataset(args.dataset)
    selected_seed_cases = choose_seed_cases(dataset_cases=dataset.cases, seed_cases=args.seed_cases)
    synthetic_multiplier = int(getattr(args, "synthetic_multiplier", 0) or 0)
    seed_cases = build_augmented_seed_cases(
        seed_cases=selected_seed_cases,
        synthetic_multiplier=synthetic_multiplier,
    )
    precheck = await ensure_seed_prerequisites(
        student_id=args.student_id,
        seed_cases=len(selected_seed_cases),
    )

    deleted_rows = {
        "causal_shadow_comparisons": 0,
        "causal_outcome_events": 0,
        "causal_feature_snapshots": 0,
        "causal_model_registry": 0,
    }
    if args.reset_causal_assets:
        deleted_rows = await reset_causal_assets()

    seeded = await seed_training_assets(
        student_id=precheck.student_id,
        school_ids=precheck.school_ids,
        seed_cases=seed_cases,
    )

    train_task = celery_app.send_task(
        "scholarpath.tasks.causal_model.causal_train_full_graph",
        kwargs={
            "profile": str(getattr(args, "profile", "high_quality") or "high_quality"),
            "bootstrap_iters": args.bootstrap_iters,
            "stability_threshold": args.stability_threshold,
            "lookback_days": args.lookback_days,
            "bootstrap_parallelism": int(getattr(args, "bootstrap_parallelism", 1) or 1),
            "checkpoint_interval": int(getattr(args, "checkpoint_interval", 25) or 25),
            "resume_from_checkpoint": bool(getattr(args, "resume_from_checkpoint", False)),
            "early_stop_patience": int(getattr(args, "early_stop_patience", 0) or 0),
            "discovery_sample_rows": int(getattr(args, "discovery_sample_rows", 300) or 300),
            "discovery_max_features": int(getattr(args, "discovery_max_features", 12) or 12),
            "min_rows_per_outcome": int(getattr(args, "min_rows_per_outcome", 200) or 200),
            "calibration_enabled": bool(getattr(args, "calibration_enabled", True)),
            "warning_mode": str(getattr(args, "warning_mode", "count_silent") or "count_silent"),
        },
    )
    train_result = await wait_for_task_result(
        celery_app,
        task_id=train_task.id,
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )
    model_version = str(train_result.get("model_version") or "").strip()
    if not model_version:
        raise RuntimeError("Training succeeded but model_version missing in result payload")

    promote_task = celery_app.send_task(
        "scholarpath.tasks.causal_model.causal_promote_model",
        kwargs={"model_version": model_version},
    )
    promote_result = await wait_for_task_result(
        celery_app,
        task_id=promote_task.id,
        timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )
    active_model_version = await ensure_single_active_model()

    now = datetime.now(UTC)
    activation_run_id = (
        "causal-activate-"
        + now.strftime("%Y%m%d-%H%M%S")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    output_dir = DEFAULT_OUTPUT_DIR / activation_run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "activation_run_id": activation_run_id,
        "generated_at": now.isoformat(),
        "status": "ok",
        "dataset": str(args.dataset),
        "seed_case_ids": [case.case_id for case in selected_seed_cases],
        "seed_case_ids_with_synthetic": [case.case_id for case in seed_cases],
        "synthetic_multiplier": synthetic_multiplier,
        "seed_case_count_real": len(selected_seed_cases),
        "seed_case_count_total": len(seed_cases),
        "seeded_snapshots": seeded["snapshots"],
        "seeded_outcomes": seeded["outcomes"],
        "seeded_snapshots_real": int(
            seeded.get("snapshots_real", seeded.get("snapshots", 0))
        ),
        "seeded_snapshots_synthetic": int(seeded.get("snapshots_synthetic", 0)),
        "seeded_outcomes_real": int(
            seeded.get("outcomes_real", seeded.get("outcomes", 0))
        ),
        "seeded_outcomes_synthetic": int(seeded.get("outcomes_synthetic", 0)),
        "deleted_rows": deleted_rows,
        "student_id": str(precheck.student_id),
        "student_count": precheck.student_count,
        "school_count": precheck.school_count,
        "training_profile": str(getattr(args, "profile", "high_quality") or "high_quality"),
        "min_rows_per_outcome": int(getattr(args, "min_rows_per_outcome", 200) or 200),
        "calibration_enabled": bool(getattr(args, "calibration_enabled", True)),
        "required_modules": required_modules,
        "queue_snapshot": queue_snapshot,
        "train_task_id": train_task.id,
        "promote_task_id": promote_task.id,
        "train_result": train_result,
        "promote_result": promote_result,
        "active_model_version": active_model_version,
    }
    (output_dir / "activation.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Activate pywhy model via deterministic seed + celery train/promote.",
    )
    parser.add_argument(
        "--dataset",
        default=str(DEFAULT_CAUSAL_GOLD_DATASET_PATH),
        help="Path to causal gold dataset JSON.",
    )
    parser.add_argument(
        "--student-id",
        default=None,
        help="Optional student UUID used for seeded snapshots/outcomes.",
    )
    parser.add_argument(
        "--seed-cases",
        type=int,
        default=40,
        help="How many dataset cases to seed into causal runtime assets (default: 40).",
    )
    parser.add_argument(
        "--reset-causal-assets",
        action="store_true",
        help="Delete causal_* tables before seeding.",
    )
    parser.add_argument(
        "--bootstrap-iters",
        type=int,
        default=100,
        help="Bootstrap iterations for causal_train_full_graph.",
    )
    parser.add_argument(
        "--stability-threshold",
        type=float,
        default=0.7,
        help="Stability threshold for edge consensus.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="Lookback window days for training dataset build.",
    )
    parser.add_argument(
        "--bootstrap-parallelism",
        type=int,
        default=4,
        help="Parallel bootstrap workers for graph discovery (default: 4).",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=25,
        help="Checkpoint write interval in bootstrap iterations (default: 25).",
    )
    parser.add_argument(
        "--resume-from-checkpoint",
        action="store_true",
        help="Resume graph discovery from checkpoint when available.",
    )
    parser.add_argument(
        "--early-stop-patience",
        type=int,
        default=40,
        help="Early stop patience in stable iterations (default: 40).",
    )
    parser.add_argument(
        "--synthetic-multiplier",
        type=int,
        default=0,
        help="Synthetic augmentation copies per real seed case (default: 0).",
    )
    parser.add_argument(
        "--discovery-sample-rows",
        type=int,
        default=300,
        help="Max rows sampled for graph discovery stage (default: 300).",
    )
    parser.add_argument(
        "--discovery-max-features",
        type=int,
        default=12,
        help="Max feature columns used in graph discovery stage (default: 12).",
    )
    parser.add_argument(
        "--profile",
        default="high_quality",
        help="Training profile preset: high_quality|standard (default: high_quality).",
    )
    parser.add_argument(
        "--min-rows-per-outcome",
        type=int,
        default=200,
        help="Fail-fast threshold per outcome before training (default: 200).",
    )
    parser.add_argument(
        "--calibration-enabled",
        dest="calibration_enabled",
        action="store_true",
        default=True,
        help="Enable outcome calibration fitting during training (default: enabled).",
    )
    parser.add_argument(
        "--no-calibration",
        dest="calibration_enabled",
        action="store_false",
        help="Disable outcome calibration fitting.",
    )
    parser.add_argument(
        "--warning-mode",
        default="count_silent",
        help="Warning behavior for training fit stages: count_silent|silent|verbose (default: count_silent).",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="Polling interval for celery task state.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=1800,
        help="Timeout per celery stage (train/promote).",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    result = asyncio.run(run_activation(args))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
