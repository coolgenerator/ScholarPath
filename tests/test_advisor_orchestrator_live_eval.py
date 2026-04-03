from __future__ import annotations

import json
from pathlib import Path

import pytest

from scholarpath.evals.advisor_orchestrator_live import run_advisor_orchestrator_eval


@pytest.mark.asyncio
async def test_advisor_orchestrator_eval_mini_10_plus_6(tmp_path: Path):
    report = await run_advisor_orchestrator_eval(
        include_reedit=True,
        sample_size=10,
        reedit_sample_size=6,
        execution_lane="stub",
        judge_enabled=False,
        output_dir=tmp_path,
    )
    payload = report.to_dict()
    assert payload["config"]["sample_size"] == 10
    assert len(payload["config"]["selected_case_ids"]) == 10
    assert payload["reedit_metrics"]["case_count"] == 6
    assert len(payload["config"]["selected_reedit_case_ids"]) == 6
    assert payload["gate"]["execution_limit_violations"] == 0

    run_dir = tmp_path / payload["run_id"]
    assert (run_dir / "report.json").exists()
    assert (run_dir / "summary.md").exists()
    assert (run_dir / "cases.jsonl").exists()
    assert (run_dir / "reedit_cases.jsonl").exists()
    assert (tmp_path / "history.csv").exists()


@pytest.mark.asyncio
async def test_advisor_orchestrator_eval_full_40_plus_12_default_reedit(tmp_path: Path):
    report = await run_advisor_orchestrator_eval(
        include_reedit=True,
        sample_size=40,
        reedit_sample_size=None,
        execution_lane="both",
        judge_enabled=False,
        output_dir=tmp_path,
    )
    payload = report.to_dict()
    assert len(payload["config"]["selected_case_ids"]) == 40
    # Compatibility default: include_reedit and no explicit size -> full 12
    assert payload["reedit_metrics"]["case_count"] == 12
    assert payload["gate"]["contract_valid_rate"] == 1.0


@pytest.mark.asyncio
async def test_reedit_case_ids_priority_and_unknown_error(tmp_path: Path):
    ok = await run_advisor_orchestrator_eval(
        include_reedit=True,
        sample_size=10,
        reedit_sample_size=6,
        reedit_case_ids=["edge_01", "history_01"],
        execution_lane="stub",
        judge_enabled=False,
        output_dir=tmp_path,
    )
    ok_payload = ok.to_dict()
    assert ok_payload["reedit_metrics"]["case_count"] == 2
    assert ok_payload["config"]["selected_reedit_case_ids"] == ["edge_01", "history_01"]

    with pytest.raises(ValueError):
        await run_advisor_orchestrator_eval(
            include_reedit=True,
            sample_size=10,
            reedit_case_ids=["not_exists"],
            execution_lane="stub",
            judge_enabled=False,
            output_dir=tmp_path / "bad",
        )
