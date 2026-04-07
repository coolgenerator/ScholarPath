from __future__ import annotations

import asyncio
from pathlib import Path

from scholarpath.evals.recommendation_gold_live import (
    DEFAULT_DATASET_PATH,
    load_recommendation_gold_dataset,
    run_recommendation_gold_eval,
)


def test_recommendation_gold_dataset_loads() -> None:
    cases = load_recommendation_gold_dataset(DEFAULT_DATASET_PATH)
    assert len(cases) >= 5
    assert all(case.case_id for case in cases)
    assert all(case.schools for case in cases)


def test_recommendation_gold_eval_default_passes(tmp_path: Path) -> None:
    report = asyncio.run(
        run_recommendation_gold_eval(
            dataset_path=DEFAULT_DATASET_PATH,
            output_dir=tmp_path,
            eval_run_id="recommendation-gold-test",
        )
    )
    assert report.status == "ok"
    assert report.metrics["case_count"] >= 5
    assert report.metrics["determinism_pass_rate"] == 1.0
    assert report.metrics["scenario_shape_pass_rate"] == 1.0
    assert report.metrics["budget_hard_gate_pass_rate"] == 1.0
    assert report.metrics["case_pass_rate"] == 1.0
    assert (tmp_path / "recommendation-gold-test" / "report.json").exists()
