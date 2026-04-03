"""Rollout quality gate for causal shadow rows."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import and_, select

from scholarpath.db.models import CausalShadowComparison
from scholarpath.db.session import async_session_factory

DEFAULT_OUTPUT_DIR = Path(".benchmarks/causal_rollout")


@dataclass(slots=True)
class CausalRolloutQualityReport:
    run_id: str
    generated_at: str
    window_hours: int
    target_percent: int
    min_rows: int
    rows: int
    fallback_rate: float
    abs_diff_p95: float
    passed: bool
    alerts: list[str]
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def run_causal_rollout_quality_gate(
    *,
    window_hours: int = 24,
    target_percent: int = 100,
    min_rows: int = 3,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> CausalRolloutQualityReport:
    run_id = f"causal-rollout-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, window_hours))
    async with async_session_factory() as session:
        rows = (
            (
                await session.execute(
                    select(CausalShadowComparison).where(
                        and_(
                            CausalShadowComparison.created_at >= cutoff,
                            CausalShadowComparison.engine_mode == "shadow",
                        )
                    )
                )
            )
            .scalars()
            .all()
        )

    fallback_count = sum(1 for row in rows if row.fallback_used)
    fallback_rate = (fallback_count / len(rows)) if rows else 0.0
    abs_diffs: list[float] = []
    for row in rows:
        diff = row.diff_scores or {}
        if isinstance(diff, dict):
            for value in diff.values():
                try:
                    abs_diffs.append(abs(float(value)))
                except (TypeError, ValueError):
                    continue
    abs_diff_p95 = _p95(abs_diffs)

    alerts: list[str] = []
    if len(rows) < min_rows:
        alerts.append(f"insufficient_rows<{min_rows}")
    if fallback_rate > 0.02:
        alerts.append("fallback_rate>0.02")
    if abs_diff_p95 > 0.15:
        alerts.append("abs_diff_p95>0.15")
    passed = len(alerts) == 0
    status = "good" if passed else "bad"

    report = CausalRolloutQualityReport(
        run_id=run_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        window_hours=window_hours,
        target_percent=target_percent,
        min_rows=min_rows,
        rows=len(rows),
        fallback_rate=round(fallback_rate, 6),
        abs_diff_p95=round(abs_diff_p95, 6),
        passed=passed,
        alerts=alerts,
        status=status,
    )
    _write_artifacts(Path(output_dir), report)
    return report


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int(round(0.95 * (len(ordered) - 1)))
    return float(ordered[max(0, min(len(ordered) - 1, idx))])


def _write_artifacts(root: Path, report: CausalRolloutQualityReport) -> None:
    run_dir = root / report.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "report.json").write_text(
        json.dumps(report.to_dict(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (run_dir / "summary.md").write_text(
        "\n".join(
            [
                f"# {report.run_id}",
                "",
                f"- status: `{report.status}`",
                f"- rows: `{report.rows}`",
                f"- fallback_rate: `{report.fallback_rate}`",
                f"- abs_diff_p95: `{report.abs_diff_p95}`",
                f"- alerts: `{', '.join(report.alerts) if report.alerts else 'none'}`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    history = root / "history.csv"
    exists = history.exists()
    row = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "window_hours": report.window_hours,
        "rows": report.rows,
        "fallback_rate": report.fallback_rate,
        "abs_diff_p95": report.abs_diff_p95,
        "passed": report.passed,
        "status": report.status,
    }
    with history.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)
