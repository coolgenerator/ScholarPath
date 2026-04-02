from __future__ import annotations

import argparse
import sys
import types
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from scholarpath.db.models import (
    CausalFeatureSnapshot,
    CausalModelRegistry,
    CausalOutcomeEvent,
    CausalShadowComparison,
    School,
    Student,
)
from scholarpath.scripts import causal_activate_pywhy as activate_script


class _SessionFactory:
    def __init__(self, session):
        self._session = session

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _build_student(name: str) -> Student:
    return Student(
        name=name,
        gpa=3.7,
        gpa_scale="4.0",
        sat_total=1450,
        curriculum_type="AP",
        intended_majors=["Computer Science"],
        budget_usd=50000,
        target_year=2027,
    )


def _build_school(name: str) -> School:
    return School(
        name=name,
        city="Boston",
        state="MA",
        school_type="university",
        size_category="medium",
        acceptance_rate=0.2,
        avg_net_price=35000,
        graduation_rate_4yr=0.88,
        student_faculty_ratio=10.0,
    )


def test_check_pywhy_dependencies_fail_fast(monkeypatch) -> None:
    def fake_import_module(name: str):
        if name == "dowhy":
            raise ModuleNotFoundError("dowhy not installed")
        return object()

    monkeypatch.setattr(activate_script.importlib, "import_module", fake_import_module)
    with pytest.raises(RuntimeError, match="Missing PyWhy dependencies"):
        activate_script.check_pywhy_dependencies()


def test_choose_seed_cases_deterministic() -> None:
    dataset = activate_script.load_causal_gold_dataset(
        activate_script.DEFAULT_CAUSAL_GOLD_DATASET_PATH
    )
    selected = activate_script.choose_seed_cases(
        dataset_cases=dataset.cases,
        seed_cases=3,
    )
    assert [case.case_id for case in selected] == ["cg-001", "cg-002", "cg-003"]


def test_build_augmented_seed_cases_adds_synthetic_variants() -> None:
    dataset = activate_script.load_causal_gold_dataset(
        activate_script.DEFAULT_CAUSAL_GOLD_DATASET_PATH
    )
    base = activate_script.choose_seed_cases(dataset_cases=dataset.cases, seed_cases=2)
    augmented = activate_script.build_augmented_seed_cases(
        seed_cases=base,
        synthetic_multiplier=2,
        rng_seed=7,
    )
    assert len(augmented) == 6
    synthetic_ids = [case.case_id for case in augmented if "-syn-" in case.case_id]
    assert len(synthetic_ids) == 4
    for case in augmented:
        if "-syn-" in case.case_id:
            assert case.label_type == "proxy"


@pytest.mark.asyncio
async def test_ensure_seed_prerequisites_fail_without_students(session, monkeypatch) -> None:
    monkeypatch.setattr(
        activate_script,
        "async_session_factory",
        _SessionFactory(session),
    )

    with pytest.raises(RuntimeError, match="at least 1 student"):
        await activate_script.ensure_seed_prerequisites(
            student_id=None,
            seed_cases=40,
        )


@pytest.mark.asyncio
async def test_reset_causal_assets_only_affects_causal_tables(session, monkeypatch) -> None:
    student = _build_student("Seed Student")
    school = _build_school("Seed University")
    session.add(student)
    session.add(school)
    await session.flush()

    now = datetime.now(UTC)
    session.add(
        CausalFeatureSnapshot(
            student_id=student.id,
            school_id=school.id,
            offer_id=None,
            context="test",
            feature_payload={
                "student_features": {"student_gpa_norm": 0.8},
                "school_features": {"school_selectivity": 0.7},
                "interaction_features": {"academic_match": 0.75},
            },
            observed_at=now,
        )
    )
    session.add(
        CausalOutcomeEvent(
            student_id=student.id,
            school_id=school.id,
            offer_id=None,
            outcome_name="admission_probability",
            outcome_value=0.7,
            label_type="proxy",
            label_confidence=0.7,
            source="test",
            observed_at=now,
            metadata_={"seed": True},
        )
    )
    session.add(
        CausalModelRegistry(
            model_name="pywhy_full_graph",
            model_version="pywhy-test-v1",
            status="trained",
            engine_type="pywhy",
            discovery_method="test",
            estimator_method="test",
            artifact_uri=None,
            graph_json={"nodes": [], "edges": []},
            metrics_json={},
            refuter_json={},
            training_window_start=now,
            training_window_end=now,
            is_active=False,
        )
    )
    session.add(
        CausalShadowComparison(
            request_id="req-1",
            context="test",
            student_id=student.id,
            school_id=school.id,
            offer_id=None,
            engine_mode="shadow",
            causal_model_version=None,
            legacy_scores={"admission_probability": 0.6},
            pywhy_scores={"admission_probability": 0.62},
            diff_scores={"admission_probability": 0.02},
            fallback_used=False,
            fallback_reason=None,
            error_json=None,
        )
    )
    await session.flush()

    monkeypatch.setattr(
        activate_script,
        "async_session_factory",
        _SessionFactory(session),
    )
    deleted = await activate_script.reset_causal_assets()

    snapshots_left = (
        await session.execute(select(CausalFeatureSnapshot))
    ).scalars().all()
    outcomes_left = (
        await session.execute(select(CausalOutcomeEvent))
    ).scalars().all()
    models_left = (
        await session.execute(select(CausalModelRegistry))
    ).scalars().all()
    shadows_left = (
        await session.execute(select(CausalShadowComparison))
    ).scalars().all()
    students_left = (await session.execute(select(Student))).scalars().all()
    schools_left = (await session.execute(select(School))).scalars().all()

    assert deleted["causal_feature_snapshots"] >= 1
    assert deleted["causal_outcome_events"] >= 1
    assert deleted["causal_model_registry"] >= 1
    assert deleted["causal_shadow_comparisons"] >= 1
    assert len(snapshots_left) == 0
    assert len(outcomes_left) == 0
    assert len(models_left) == 0
    assert len(shadows_left) == 0
    assert len(students_left) == 1
    assert len(schools_left) == 1


@pytest.mark.asyncio
async def test_wait_for_task_result_success_and_failure() -> None:
    class _AsyncResult:
        def __init__(self, status: str, result: object):
            self.status = status
            self.result = result

    class _FakeCeleryApp:
        def __init__(self, rows):
            self._rows = list(rows)

        def AsyncResult(self, task_id: str):
            if len(self._rows) == 1:
                status, result = self._rows[0]
            else:
                status, result = self._rows.pop(0)
            return _AsyncResult(status, result)

    success_app = _FakeCeleryApp(
        [
            ("PENDING", None),
            ("SUCCESS", {"model_version": "pywhy-v1"}),
        ]
    )
    result = await activate_script.wait_for_task_result(
        success_app,
        task_id="task-1",
        timeout_seconds=3,
        poll_interval_seconds=0.01,
    )
    assert result["model_version"] == "pywhy-v1"

    fail_app = _FakeCeleryApp([("FAILURE", RuntimeError("train failed"))])
    with pytest.raises(RuntimeError, match="Celery task failed"):
        await activate_script.wait_for_task_result(
            fail_app,
            task_id="task-2",
            timeout_seconds=3,
            poll_interval_seconds=0.01,
        )

    revoked_app = _FakeCeleryApp([("REVOKED", None)])
    with pytest.raises(RuntimeError, match="Celery task revoked"):
        await activate_script.wait_for_task_result(
            revoked_app,
            task_id="task-3",
            timeout_seconds=3,
            poll_interval_seconds=0.01,
        )


@pytest.mark.asyncio
async def test_run_activation_happy_path_with_mocked_celery(
    monkeypatch,
    tmp_path,
) -> None:
    fake_student_id = uuid.uuid4()
    fake_school_ids = [uuid.uuid4() for _ in range(40)]

    class _FakeTask:
        def __init__(self, task_id: str):
            self.id = task_id

    class _FakeCeleryApp:
        def __init__(self):
            self.sent = []

        def send_task(self, name: str, kwargs: dict):
            task_id = f"task-{len(self.sent) + 1}"
            self.sent.append((task_id, name, kwargs))
            return _FakeTask(task_id)

    fake_celery = _FakeCeleryApp()

    monkeypatch.setattr(activate_script, "DEFAULT_OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(
        activate_script,
        "check_pywhy_dependencies",
        lambda: ["dowhy", "econml", "causallearn"],
    )
    monkeypatch.setattr(
        activate_script,
        "inspect_active_queues",
        lambda app: {"worker@local": ["deep_search", "conflict", "causal_train"]},
    )
    monkeypatch.setattr(
        activate_script,
        "ensure_seed_prerequisites",
        AsyncMock(
            return_value=activate_script.ActivationContext(
                student_id=fake_student_id,
                school_ids=fake_school_ids,
                student_count=1,
                school_count=64,
            )
        ),
    )
    monkeypatch.setattr(
        activate_script,
        "seed_training_assets",
        AsyncMock(return_value={"snapshots": 40, "outcomes": 200}),
    )
    monkeypatch.setattr(
        activate_script,
        "ensure_single_active_model",
        AsyncMock(return_value="pywhy-activated-v1"),
    )

    async def fake_wait_for_task_result(celery_app, *, task_id: str, **kwargs):
        if task_id == "task-1":
            return {"model_version": "pywhy-activated-v1"}
        return {"model_version": "pywhy-activated-v1", "status": "active"}

    monkeypatch.setattr(
        activate_script,
        "wait_for_task_result",
        fake_wait_for_task_result,
    )

    monkeypatch.setitem(
        sys.modules,
        "scholarpath.tasks",
        types.SimpleNamespace(celery_app=fake_celery),
    )

    args = argparse.Namespace(
        dataset=str(activate_script.DEFAULT_CAUSAL_GOLD_DATASET_PATH),
        student_id=None,
        seed_cases=40,
        reset_causal_assets=False,
        bootstrap_iters=100,
        stability_threshold=0.7,
        lookback_days=365,
        poll_interval_seconds=0.1,
        timeout_seconds=60,
    )
    result = await activate_script.run_activation(args)

    assert result["status"] == "ok"
    assert result["seeded_snapshots"] == 40
    assert result["seeded_outcomes"] == 200
    assert result["active_model_version"] == "pywhy-activated-v1"
    assert result["train_task_id"] == "task-1"
    assert result["promote_task_id"] == "task-2"

    activation_dir = tmp_path / result["activation_run_id"]
    assert (activation_dir / "activation.json").exists()
