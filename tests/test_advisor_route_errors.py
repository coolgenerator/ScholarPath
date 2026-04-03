from __future__ import annotations

import pytest

from scholarpath.evals.advisor_orchestrator_live import run_advisor_orchestrator_eval


@pytest.mark.asyncio
async def test_advisor_eval_rejects_invalid_execution_lane(tmp_path):
    with pytest.raises(ValueError):
        await run_advisor_orchestrator_eval(
            execution_lane="invalid",  # type: ignore[arg-type]
            output_dir=tmp_path,
        )


@pytest.mark.asyncio
async def test_advisor_eval_rejects_rpm_over_200(tmp_path):
    with pytest.raises(ValueError):
        await run_advisor_orchestrator_eval(
            max_rpm_total=201,
            output_dir=tmp_path,
        )
