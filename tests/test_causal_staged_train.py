from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from scholarpath.scripts import causal_staged_train as staged


class _FakeSession:
    def __init__(self) -> None:
        self.committed = False

    async def commit(self) -> None:
        self.committed = True


class _SessionFactory:
    def __init__(self, session: _FakeSession) -> None:
        self._session = session

    def __call__(self):
        return self

    async def __aenter__(self) -> _FakeSession:
        return self._session

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


def _base_eval_metrics() -> dict[str, float | int | bool | str]:
    return {
        "judge_overall_score": 81.0,
        "mae_overall_legacy": 0.23,
        "mae_overall_pywhy": 0.21,
        "judge_field_pass_rate_pywhy": 0.62,
        "fallback_rate_pywhy": 0.01,
        "rate_limit_error_count": 0,
        "pywhy_case_count": 40,
        "pywhy_status": "ok",
        "rollout_passed": True,
    }


def _write_stage_history(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "run_id",
                "generated_at",
                "stage",
                "status",
                "passed",
                "candidate_count",
                "champion_model_version",
                "champion_score",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_rollout_history(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "run_id",
                "generated_at",
                "status",
                "passed",
                "target_percent",
                "alerts_count",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_stage_sequence_and_configs() -> None:
    assert staged._stage_sequence("all") == [1, 2, 3, 4]
    assert staged._stage_sequence("2") == [2]
    with pytest.raises(ValueError):
        staged._stage_sequence("9")

    stage4 = staged.STAGE_CONFIGS[4]
    assert stage4.snapshots_target == 15000
    assert stage4.outcome_target == 15000
    assert stage4.admission_true_target == 3000
    assert stage4.other_anchor_target == 1000
    assert stage4.min_rows_per_outcome == 1000
    assert stage4.judge_min == 80.0
    assert stage4.field_pass_min == 0.60
    assert stage4.fallback_max == 0.02
    assert stage4.require_rate_limit_zero is True


def test_evaluate_candidate_gate_respects_stage_thresholds() -> None:
    metrics = _base_eval_metrics()
    ok, reasons = staged._evaluate_candidate_gate(
        config=staged.STAGE_CONFIGS[2],
        eval_metrics=metrics,
    )
    assert ok is True
    assert reasons == []

    low_field = dict(metrics)
    low_field["judge_field_pass_rate_pywhy"] = 0.49
    ok, reasons = staged._evaluate_candidate_gate(
        config=staged.STAGE_CONFIGS[2],
        eval_metrics=low_field,
    )
    assert ok is False
    assert any("judge_field_pass_rate_pywhy" in reason for reason in reasons)

    no_pywhy = dict(metrics)
    no_pywhy["pywhy_case_count"] = 0
    no_pywhy["pywhy_status"] = "failed_precondition"
    ok, reasons = staged._evaluate_candidate_gate(
        config=staged.STAGE_CONFIGS[1],
        eval_metrics=no_pywhy,
    )
    assert ok is False
    assert any("pywhy_case_count" in reason for reason in reasons)
    assert any("pywhy_status" in reason for reason in reasons)

    stage4_rate_limit = dict(metrics)
    stage4_rate_limit["rate_limit_error_count"] = 1
    ok, reasons = staged._evaluate_candidate_gate(
        config=staged.STAGE_CONFIGS[4],
        eval_metrics=stage4_rate_limit,
    )
    assert ok is False
    assert any("rate_limit_error_count" in reason for reason in reasons)


def test_has_two_consecutive_stage4_passes() -> None:
    rows = [
        {"stage": "4", "passed": "true"},
        {"stage": "3", "passed": "true"},
        {"stage": "4", "passed": "true"},
        {"stage": "4", "passed": "true"},
    ]
    assert staged._has_two_consecutive_stage4_passes(rows) is True

    bad_rows = [
        {"stage": "4", "passed": "true"},
        {"stage": "4", "passed": "false"},
    ]
    assert staged._has_two_consecutive_stage4_passes(bad_rows) is False


def test_shadow_window_clean_filters_target_percent_and_min_rows(tmp_path: Path) -> None:
    history = tmp_path / "rollout" / "history.csv"
    now = datetime.now(UTC)
    _write_rollout_history(
        history,
        rows=[
            {
                "run_id": "old-target",
                "generated_at": (now - timedelta(hours=26)).isoformat(),
                "status": "watch",
                "passed": "true",
                "target_percent": "100",
                "alerts_count": "0",
            },
            {
                "run_id": "window-mismatch-target",
                "generated_at": (now - timedelta(hours=1)).isoformat(),
                "status": "watch",
                "passed": "true",
                "target_percent": "50",
                "alerts_count": "0",
            },
            {
                "run_id": "window-target-ok",
                "generated_at": (now - timedelta(minutes=30)).isoformat(),
                "status": "watch",
                "passed": "true",
                "target_percent": "100",
                "alerts_count": "0",
            },
        ],
    )

    ok, reasons = staged._is_shadow_window_clean(
        hours=24,
        history_path=history,
        target_percent=100,
        min_rows=2,
    )
    assert ok is False
    assert any("shadow_min_rows" in reason for reason in reasons)

    ok, reasons = staged._is_shadow_window_clean(
        hours=24,
        history_path=history,
        target_percent=100,
        min_rows=1,
    )
    assert ok is True
    assert reasons == []


def test_shadow_window_clean_fails_on_unpassed_or_alerts(tmp_path: Path) -> None:
    history = tmp_path / "rollout" / "history.csv"
    now = datetime.now(UTC)
    _write_rollout_history(
        history,
        rows=[
            {
                "run_id": "r1",
                "generated_at": (now - timedelta(hours=1)).isoformat(),
                "status": "bad",
                "passed": "false",
                "target_percent": "100",
                "alerts_count": "0",
            },
            {
                "run_id": "r2",
                "generated_at": (now - timedelta(minutes=10)).isoformat(),
                "status": "watch",
                "passed": "true",
                "target_percent": "100",
                "alerts_count": "2",
            },
        ],
    )

    ok, reasons = staged._is_shadow_window_clean(
        hours=24,
        history_path=history,
        target_percent=100,
        min_rows=2,
    )
    assert ok is False
    assert any("not passed" in reason for reason in reasons)
    assert any("alerts_count=2" in reason for reason in reasons)


@pytest.mark.asyncio
async def test_promote_if_ready_requires_history_and_shadow_window(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "staged_root"
    history = output_root / "history.csv"
    shadow_history = tmp_path / "rollout" / "history.csv"
    _write_rollout_history(
        shadow_history,
        rows=[
            {
                "run_id": "rollout-1",
                "generated_at": datetime.now(UTC).isoformat(),
                "status": "watch",
                "passed": "true",
                "target_percent": "100",
                "alerts_count": "0",
            },
        ],
    )

    disabled = await staged._promote_if_ready(
        output_root=output_root,
        champion_model_version="pywhy-v1",
        promote_on_final_pass=False,
        require_shadow_window_hours=24,
        shadow_history_path=shadow_history,
        shadow_target_percent=100,
        shadow_min_rows=1,
        shadow_refresh_before_promote=False,
    )
    assert disabled.promoted is False
    assert disabled.attempted is False

    missing_history = await staged._promote_if_ready(
        output_root=output_root,
        champion_model_version="pywhy-v1",
        promote_on_final_pass=True,
        require_shadow_window_hours=24,
        shadow_history_path=shadow_history,
        shadow_target_percent=100,
        shadow_min_rows=1,
        shadow_refresh_before_promote=False,
    )
    assert missing_history.promoted is False
    assert missing_history.attempted is True
    assert any("consecutive pass count < 2" in item for item in missing_history.reasons)

    now = datetime.now(UTC)
    _write_stage_history(
        history,
        rows=[
            {
                "run_id": "r1",
                "generated_at": (now - timedelta(hours=2)).isoformat(),
                "stage": "4",
                "status": "passed",
                "passed": "true",
                "candidate_count": "3",
                "champion_model_version": "pywhy-a",
                "champion_score": "0.81",
            },
            {
                "run_id": "r2",
                "generated_at": (now - timedelta(hours=1)).isoformat(),
                "stage": "4",
                "status": "passed",
                "passed": "true",
                "candidate_count": "3",
                "champion_model_version": "pywhy-b",
                "champion_score": "0.84",
            },
        ],
    )

    monkeypatch.setattr(staged, "_is_shadow_window_clean", lambda **_: (True, []))
    called: dict[str, str] = {}

    async def _fake_promote_model(session, *, model_version: str) -> None:
        called["model_version"] = model_version

    fake_session = _FakeSession()
    monkeypatch.setattr(staged, "promote_model", _fake_promote_model)
    monkeypatch.setattr(staged, "async_session_factory", _SessionFactory(fake_session))

    promoted = await staged._promote_if_ready(
        output_root=output_root,
        champion_model_version="pywhy-b",
        promote_on_final_pass=True,
        require_shadow_window_hours=24,
        shadow_history_path=shadow_history,
        shadow_target_percent=100,
        shadow_min_rows=1,
        shadow_refresh_before_promote=False,
    )
    assert promoted.promoted is True
    assert promoted.attempted is True
    assert called["model_version"] == "pywhy-b"
    assert fake_session.committed is True


@pytest.mark.asyncio
async def test_promote_if_ready_refreshes_shadow_gate_before_check(
    monkeypatch,
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "staged_root"
    history = output_root / "history.csv"
    now = datetime.now(UTC)
    _write_stage_history(
        history,
        rows=[
            {
                "run_id": "r1",
                "generated_at": (now - timedelta(hours=2)).isoformat(),
                "stage": "4",
                "status": "passed",
                "passed": "true",
                "candidate_count": "3",
                "champion_model_version": "pywhy-a",
                "champion_score": "0.81",
            },
            {
                "run_id": "r2",
                "generated_at": (now - timedelta(hours=1)).isoformat(),
                "stage": "4",
                "status": "passed",
                "passed": "true",
                "candidate_count": "3",
                "champion_model_version": "pywhy-b",
                "champion_score": "0.84",
            },
        ],
    )

    shadow_history = tmp_path / "rollout" / "history.csv"
    _write_rollout_history(
        shadow_history,
        rows=[
            {
                "run_id": "rollout-1",
                "generated_at": now.isoformat(),
                "status": "watch",
                "passed": "true",
                "target_percent": "100",
                "alerts_count": "0",
            },
        ],
    )

    refresh_calls: dict[str, int] = {"count": 0}

    async def _fake_refresh(**kwargs):
        refresh_calls["count"] += 1
        return SimpleNamespace(run_id="rollout-run", decision=SimpleNamespace(status="watch", passed=True))

    monkeypatch.setattr(staged, "run_causal_rollout_quality_gate", _fake_refresh)
    monkeypatch.setattr(staged, "_is_shadow_window_clean", lambda **_: (True, []))

    fake_session = _FakeSession()
    monkeypatch.setattr(staged, "promote_model", AsyncMock(return_value=None))
    monkeypatch.setattr(staged, "async_session_factory", _SessionFactory(fake_session))

    promoted = await staged._promote_if_ready(
        output_root=output_root,
        champion_model_version="pywhy-b",
        promote_on_final_pass=True,
        require_shadow_window_hours=24,
        shadow_history_path=shadow_history,
        shadow_target_percent=100,
        shadow_min_rows=1,
        shadow_refresh_before_promote=True,
    )
    assert promoted.attempted is True
    assert refresh_calls["count"] == 1


def _build_stage_args(*, stage: str, output_dir: Path) -> argparse.Namespace:
    return argparse.Namespace(
        stage=stage,
        train_candidates_per_stage=1,
        max_rpm_total=120,
        judge_concurrency=2,
        engine_case_concurrency=4,
        promote_on_final_pass=True,
        seed_cases=40,
        shadow_window_hours=24,
        shadow_history_path=".benchmarks/causal_rollout/history.csv",
        shadow_target_percent=100,
        shadow_min_rows=3,
        shadow_refresh_before_promote=True,
        output_dir=str(output_dir),
        student_id=None,
        reset_causal_assets=False,
    )


def _build_fake_gold_report() -> SimpleNamespace:
    return SimpleNamespace(
        run_id="gold-run-1",
        metrics={
            "judge_overall_score": 88.0,
            "mae_overall_legacy": 0.24,
            "mae_overall_pywhy": 0.20,
            "judge_field_pass_rate_pywhy": 0.66,
            "fallback_rate_pywhy": 0.01,
            "rate_limit_error_count": 0,
        },
        config={"output_dir": "tmp/gold"},
        pywhy_pass=SimpleNamespace(status="ok", case_count=40),
    )


def _build_fake_rollout_report() -> SimpleNamespace:
    return SimpleNamespace(
        run_id="rollout-run-1",
        decision=SimpleNamespace(status="good", passed=True),
        metrics=SimpleNamespace(fallback_rate=0.01),
    )


@pytest.mark.asyncio
async def test_run_staged_training_stage1_does_not_attempt_promote(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        staged,
        "_prepare_stage_assets",
        AsyncMock(
            return_value=(
                {"snapshots": 1000, "outcomes_by_outcome": {}, "admission_true": 100, "anchors_by_outcome": {}},
                {"snapshots": 3000, "outcomes_by_outcome": {name: 3000 for name in staged._OUTCOMES}, "admission_true": 400, "anchors_by_outcome": {name: 150 for name in staged._AUX_OUTCOMES}},
                {"snapshots": 3000, "outcomes": 15000},
                True,
                [],
            )
        ),
    )
    monkeypatch.setattr(
        staged,
        "_train_candidate_model",
        AsyncMock(
            side_effect=lambda **kwargs: {
                "model_version": kwargs["model_version"],
                "metrics": {"loss": 0.1},
                "artifact_uri": "artifact://mock",
            }
        ),
    )
    monkeypatch.setattr(
        staged,
        "_run_candidate_eval",
        AsyncMock(return_value=(_build_fake_gold_report(), _build_fake_rollout_report())),
    )
    monkeypatch.setattr(staged, "_annotate_candidate", AsyncMock(return_value=None))

    calls = {"promote": 0}

    async def _fake_promote_if_ready(**kwargs):
        calls["promote"] += 1
        return staged.PromotionDecision(
            attempted=True,
            promoted=False,
            model_version=str(kwargs.get("champion_model_version")),
            reasons=["not-needed"],
        )

    monkeypatch.setattr(staged, "_promote_if_ready", _fake_promote_if_ready)

    args = _build_stage_args(stage="1", output_dir=tmp_path / "staged")
    result = await staged.run_staged_training(args)

    assert result["status"] == "ok"
    assert len(result["stages"]) == 1
    assert result["stages"][0]["status"] == "passed"
    assert calls["promote"] == 0

    run_dir = Path(result["config"]["output_dir"])
    assert (run_dir / "stage_1" / "stage_summary.json").exists()
    assert (run_dir / "stage_1" / "gate_results.json").exists()
    assert (run_dir / "promotion_decision.json").exists()
    assert (run_dir / "run_summary.json").exists()


@pytest.mark.asyncio
async def test_run_staged_training_stage4_calls_promote_with_champion(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        staged,
        "_prepare_stage_assets",
        AsyncMock(
            return_value=(
                {"snapshots": 12000, "outcomes_by_outcome": {}, "admission_true": 2200, "anchors_by_outcome": {}},
                {"snapshots": 15000, "outcomes_by_outcome": {name: 15000 for name in staged._OUTCOMES}, "admission_true": 3000, "anchors_by_outcome": {name: 1000 for name in staged._AUX_OUTCOMES}},
                {"snapshots": 3000, "outcomes": 15000},
                True,
                [],
            )
        ),
    )
    monkeypatch.setattr(
        staged,
        "_train_candidate_model",
        AsyncMock(
            side_effect=lambda **kwargs: {
                "model_version": kwargs["model_version"],
                "metrics": {"loss": 0.1},
                "artifact_uri": "artifact://mock",
            }
        ),
    )
    monkeypatch.setattr(
        staged,
        "_run_candidate_eval",
        AsyncMock(return_value=(_build_fake_gold_report(), _build_fake_rollout_report())),
    )
    monkeypatch.setattr(staged, "_annotate_candidate", AsyncMock(return_value=None))

    captured: dict[str, str] = {}

    async def _fake_promote_if_ready(**kwargs):
        captured["champion"] = str(kwargs.get("champion_model_version") or "")
        return staged.PromotionDecision(
            attempted=True,
            promoted=False,
            model_version=captured["champion"],
            reasons=["mock-no-promote"],
        )

    monkeypatch.setattr(staged, "_promote_if_ready", _fake_promote_if_ready)

    args = _build_stage_args(stage="4", output_dir=tmp_path / "staged")
    result = await staged.run_staged_training(args)

    stage_summary = result["stages"][0]
    assert stage_summary["status"] == "passed"
    assert captured["champion"] == stage_summary["champion_model_version"]
    assert result["promotion_decision"]["attempted"] is True
