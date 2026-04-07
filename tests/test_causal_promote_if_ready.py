from __future__ import annotations

import json
from pathlib import Path

from scholarpath.scripts.causal_promote_if_ready import _load_latest_stage4_candidate


def _write_stage4_run(
    run_dir: Path,
    *,
    model_version: str,
    passed: bool,
    stage4_min_admission_rows: int | None = None,
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "gate_results.json").write_text(
        json.dumps(
            {
                "stage_4": {
                    "passed": bool(passed),
                    "champion_model_version": model_version,
                }
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "stage_summary.json").write_text(
        json.dumps(
            {
                "stage_4": {
                    "champion": {
                        "model_version": model_version,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    report_payload = {"overrides_applied": {}}
    if stage4_min_admission_rows is not None:
        report_payload["overrides_applied"] = {
            "stage4_min_admission_rows": int(stage4_min_admission_rows)
        }
    (run_dir / "report.json").write_text(json.dumps(report_payload), encoding="utf-8")


def test_load_latest_stage4_candidate_skips_override_runs(tmp_path: Path) -> None:
    staged_root = tmp_path / "bench"
    _write_stage4_run(
        staged_root / "causal-staged-20260406-000200-abcd12",
        model_version="model-override",
        passed=True,
        stage4_min_admission_rows=14_000,
    )
    _write_stage4_run(
        staged_root / "causal-staged-20260406-000100-efgh34",
        model_version="model-strict",
        passed=True,
        stage4_min_admission_rows=None,
    )

    model_version, context = _load_latest_stage4_candidate(staged_root)
    assert model_version == "model-strict"
    assert "000100" in str(context.get("run_dir", ""))


def test_load_latest_stage4_candidate_returns_none_when_only_override(tmp_path: Path) -> None:
    staged_root = tmp_path / "bench"
    _write_stage4_run(
        staged_root / "causal-staged-20260406-000300-ijkl56",
        model_version="model-override-only",
        passed=True,
        stage4_min_admission_rows=14_000,
    )

    model_version, context = _load_latest_stage4_candidate(staged_root)
    assert model_version is None
    assert context.get("reason") == "no_passed_stage4_candidate"
