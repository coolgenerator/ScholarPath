from __future__ import annotations

import argparse
import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from scholarpath.scripts import causal_promote_if_ready as promote_script
from scholarpath.scripts.causal_staged_train import PromotionDecision


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


def test_latest_stage4_champion_uses_latest_timestamp(tmp_path: Path) -> None:
    history = tmp_path / "history.csv"
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
                "champion_model_version": "pywhy-old",
                "champion_score": "0.8",
            },
            {
                "run_id": "r2",
                "generated_at": (now - timedelta(hours=1)).isoformat(),
                "stage": "4",
                "status": "passed",
                "passed": "true",
                "candidate_count": "3",
                "champion_model_version": "pywhy-new",
                "champion_score": "0.9",
            },
        ],
    )

    latest = promote_script._latest_stage4_champion(tmp_path)
    assert latest == "pywhy-new"


@pytest.mark.asyncio
async def test_run_uses_latest_stage4_champion_and_returns_decision(monkeypatch, tmp_path: Path) -> None:
    history = tmp_path / "history.csv"
    now = datetime.now(UTC)
    _write_stage_history(
        history,
        rows=[
            {
                "run_id": "r1",
                "generated_at": now.isoformat(),
                "stage": "4",
                "status": "passed",
                "passed": "true",
                "candidate_count": "3",
                "champion_model_version": "pywhy-latest",
                "champion_score": "0.91",
            },
        ],
    )

    captured: dict[str, str] = {}

    async def _fake_promote_if_ready(**kwargs):
        captured["model_version"] = str(kwargs.get("champion_model_version") or "")
        return PromotionDecision(
            attempted=True,
            promoted=False,
            model_version=captured["model_version"],
            reasons=["mocked"],
        )

    monkeypatch.setattr(promote_script, "_promote_if_ready", _fake_promote_if_ready)

    args = argparse.Namespace(
        staged_output_dir=str(tmp_path),
        shadow_window_hours=24,
        shadow_min_rows=3,
        shadow_target_percent=100,
        shadow_history_path=str(tmp_path / "rollout_history.csv"),
        shadow_refresh_before_promote=False,
    )
    payload = await promote_script._run(args)
    assert payload["attempted"] is True
    assert payload["promoted"] is False
    assert payload["model_version"] == "pywhy-latest"
    assert captured["model_version"] == "pywhy-latest"
