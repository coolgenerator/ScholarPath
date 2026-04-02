"""Causal rollout quality gate for shadow traffic stages."""

from __future__ import annotations

import csv
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any

from sqlalchemy import select

from scholarpath.causal_engine import CausalRuntime
from scholarpath.config import settings
from scholarpath.db.models import CausalShadowComparison, School, Student
from scholarpath.db.session import async_session_factory

DEFAULT_CAUSAL_ROLLOUT_OUTPUT_DIR = Path(".benchmarks/causal_rollout")
_OUTCOMES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]


@dataclass
class RolloutGateThresholds:
    min_rows: int = 100
    ratio_tolerance: float = 0.05
    fallback_rate_max: float = 0.02
    mae_gap_max: float = 0.03


@dataclass
class RolloutGateMetrics:
    sample_rows: int
    pywhy_primary_rows: int
    legacy_primary_rows: int
    pywhy_primary_ratio: float
    fallback_rate: float
    mae_all: float
    mae_pywhy_primary: float
    mae_legacy_primary: float
    mae_gap_pywhy_minus_legacy: float
    dual_arm_available: bool
    abs_diff_p95: float
    abs_diff_max: float
    by_outcome_mae: dict[str, float] = field(default_factory=dict)


@dataclass
class RolloutGateDecision:
    status: str
    passed: bool
    reasons: list[str] = field(default_factory=list)


@dataclass
class RolloutQualityReport:
    run_id: str
    generated_at: str
    config: dict[str, Any]
    metrics: RolloutGateMetrics
    decision: RolloutGateDecision
    trend: dict[str, Any] = field(default_factory=dict)
    alerts: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["metrics"] = asdict(self.metrics)
        payload["decision"] = asdict(self.decision)
        return payload


def evaluate_rollout_gate(
    *,
    metrics: RolloutGateMetrics,
    target_percent: int,
    thresholds: RolloutGateThresholds,
    trend: dict[str, Any] | None = None,
) -> RolloutGateDecision:
    reasons: list[str] = []

    if metrics.sample_rows < int(thresholds.min_rows):
        reasons.append(
            f"sample_rows {metrics.sample_rows} < min_rows {int(thresholds.min_rows)}",
        )

    target_ratio = max(0, min(100, int(target_percent))) / 100.0
    ratio_deviation = abs(metrics.pywhy_primary_ratio - target_ratio)
    if ratio_deviation > float(thresholds.ratio_tolerance):
        reasons.append(
            "pywhy_primary_ratio deviation "
            f"{ratio_deviation:.4f} > ratio_tolerance {float(thresholds.ratio_tolerance):.4f}",
        )

    if metrics.fallback_rate > float(thresholds.fallback_rate_max):
        reasons.append(
            f"fallback_rate {metrics.fallback_rate:.4f} > fallback_rate_max {float(thresholds.fallback_rate_max):.4f}",
        )

    if metrics.dual_arm_available:
        if metrics.mae_gap_pywhy_minus_legacy > float(thresholds.mae_gap_max):
            reasons.append(
                "mae_gap_pywhy_minus_legacy "
                f"{metrics.mae_gap_pywhy_minus_legacy:.4f} > mae_gap_max {float(thresholds.mae_gap_max):.4f}",
            )

    if isinstance(trend, dict):
        fallback_delta = float(trend.get("fallback_rate_delta", 0.0) or 0.0)
        abs_p95_delta = float(trend.get("abs_diff_p95_delta", 0.0) or 0.0)
        worsening_outcomes = trend.get("by_outcome_mae_worsening")
        if fallback_delta > 0.01:
            reasons.append(f"trend fallback_rate worsening: delta={fallback_delta:.4f}")
        if abs_p95_delta > 0.03:
            reasons.append(f"trend abs_diff_p95 worsening: delta={abs_p95_delta:.4f}")
        if isinstance(worsening_outcomes, list) and worsening_outcomes:
            reasons.append(
                "trend by_outcome_mae worsening: " + ", ".join(str(item) for item in worsening_outcomes),
            )

    if reasons:
        return RolloutGateDecision(status="bad", passed=False, reasons=reasons)

    watch_reasons: list[str] = []
    if metrics.dual_arm_available and metrics.mae_gap_pywhy_minus_legacy > float(thresholds.mae_gap_max) * 0.5:
        watch_reasons.append(
            "mae gap in watch band: "
            f"{metrics.mae_gap_pywhy_minus_legacy:.4f} "
            f"(threshold {float(thresholds.mae_gap_max):.4f})",
        )
    if not metrics.dual_arm_available:
        watch_reasons.append("single-arm rollout; mae gap check skipped")
    if isinstance(trend, dict):
        fallback_delta = float(trend.get("fallback_rate_delta", 0.0) or 0.0)
        abs_p95_delta = float(trend.get("abs_diff_p95_delta", 0.0) or 0.0)
        if fallback_delta > 0.0:
            watch_reasons.append(f"trend fallback_rate delta={fallback_delta:.4f}")
        if abs_p95_delta > 0.0:
            watch_reasons.append(f"trend abs_diff_p95 delta={abs_p95_delta:.4f}")
    if watch_reasons:
        return RolloutGateDecision(status="watch", passed=True, reasons=watch_reasons)

    return RolloutGateDecision(status="good", passed=True, reasons=[])


async def run_causal_rollout_quality_gate(
    *,
    target_percent: int | None = None,
    sample_schools: int = 64,
    contexts: int = 2,
    context_prefix: str = "rollout_quality_gate",
    output_dir: str | Path = DEFAULT_CAUSAL_ROLLOUT_OUTPUT_DIR,
    min_rows: int = 100,
    ratio_tolerance: float = 0.05,
    fallback_rate_max: float = 0.02,
    mae_gap_max: float = 0.03,
    history_window_runs: int = 24,
    emit_alert: bool = True,
    pywhy_model_version_hint: str = "latest_stable",
) -> RolloutQualityReport:
    if int(contexts) <= 0:
        raise ValueError("contexts must be > 0")
    if int(sample_schools) <= 0:
        raise ValueError("sample_schools must be > 0")

    target = (
        max(0, min(100, int(settings.CAUSAL_PYWHY_PRIMARY_PERCENT)))
        if target_percent is None
        else max(0, min(100, int(target_percent)))
    )
    thresholds = RolloutGateThresholds(
        min_rows=max(1, int(min_rows)),
        ratio_tolerance=max(0.0, float(ratio_tolerance)),
        fallback_rate_max=max(0.0, float(fallback_rate_max)),
        mae_gap_max=max(0.0, float(mae_gap_max)),
    )

    run_id = f"rollout-quality-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    run_dir = Path(output_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)

    contexts_used = _build_contexts(
        context_prefix=context_prefix,
        context_count=int(contexts),
    )
    async with async_session_factory() as session:
        student = (await session.execute(select(Student).order_by(Student.created_at.asc()).limit(1))).scalars().first()
        if student is None:
            raise RuntimeError("No student found for rollout quality gate")
        schools = (
            await session.execute(select(School).order_by(School.name.asc()).limit(int(sample_schools)))
        ).scalars().all()
        if not schools:
            raise RuntimeError("No schools found for rollout quality gate")

        runtime = CausalRuntime(
            session,
            model_version_hint=pywhy_model_version_hint,
        )
        for ctx_name in contexts_used:
            for school in schools:
                await runtime.estimate(
                    student=student,
                    school=school,
                    offer=None,
                    context=ctx_name,
                    outcomes=list(_OUTCOMES),
                    metadata={"service": "causal_rollout_quality_gate", "run_id": run_id},
                )
        await session.commit()

    async with async_session_factory() as session:
        rows = (
            await session.execute(
                select(CausalShadowComparison).where(
                    CausalShadowComparison.context.in_(contexts_used),
                    CausalShadowComparison.created_at >= started_at,
                )
            )
        ).scalars().all()

    metrics = _build_metrics(rows)
    history_rows = _load_recent_rollout_history(
        history_path=Path(output_dir) / "history.csv",
        history_window_runs=max(1, int(history_window_runs)),
    )
    trend = _build_trend_snapshot(
        metrics=metrics,
        history_rows=history_rows,
    )
    latest_gold_metrics = _load_latest_gold_eval_metrics()
    decision = evaluate_rollout_gate(
        metrics=metrics,
        target_percent=target,
        thresholds=thresholds,
        trend=trend,
    )
    alerts = _build_alerts(
        metrics=metrics,
        latest_gold_metrics=latest_gold_metrics,
        run_id=run_id,
    )

    report = RolloutQualityReport(
        run_id=run_id,
        generated_at=datetime.now(UTC).isoformat(),
        config={
            "causal_engine_mode": (settings.CAUSAL_ENGINE_MODE or "shadow").strip().lower(),
            "target_percent": target,
            "sample_schools": int(sample_schools),
            "contexts": int(contexts),
            "contexts_used": contexts_used,
            "outcomes": list(_OUTCOMES),
            "thresholds": asdict(thresholds),
            "history_window_runs": max(1, int(history_window_runs)),
            "emit_alert": bool(emit_alert),
            "pywhy_model_version_hint": str(pywhy_model_version_hint or "latest_stable"),
            "latest_gold_metrics": latest_gold_metrics,
        },
        metrics=metrics,
        decision=decision,
        trend=trend,
        alerts=alerts,
    )

    _write_artifacts(
        report=report,
        run_dir=run_dir,
        output_root=Path(output_dir),
    )
    if emit_alert and alerts:
        _append_alerts(Path(output_dir) / "alerts.jsonl", alerts)
    return report


def _build_metrics(rows: list[CausalShadowComparison]) -> RolloutGateMetrics:
    sample_rows = len(rows)
    pywhy_rows = [row for row in rows if row.engine_mode == "shadow_pywhy"]
    legacy_rows = [row for row in rows if row.engine_mode in {"shadow_legacy", "shadow"}]
    fallback_count = sum(1 for row in rows if row.fallback_used)

    all_abs = _collect_abs_diffs(rows)
    pywhy_abs = _collect_abs_diffs(pywhy_rows)
    legacy_abs = _collect_abs_diffs(legacy_rows)
    by_outcome = _collect_outcome_mae(rows)

    pywhy_ratio = (len(pywhy_rows) / sample_rows) if sample_rows else 0.0
    fallback_rate = (fallback_count / sample_rows) if sample_rows else 0.0
    mae_pywhy = mean(pywhy_abs) if pywhy_abs else 0.0
    mae_legacy = mean(legacy_abs) if legacy_abs else 0.0
    dual_arm_available = bool(pywhy_rows and legacy_rows)
    mae_gap = (mae_pywhy - mae_legacy) if dual_arm_available else 0.0

    return RolloutGateMetrics(
        sample_rows=sample_rows,
        pywhy_primary_rows=len(pywhy_rows),
        legacy_primary_rows=len(legacy_rows),
        pywhy_primary_ratio=round(pywhy_ratio, 6),
        fallback_rate=round(fallback_rate, 6),
        mae_all=round(mean(all_abs), 6) if all_abs else 0.0,
        mae_pywhy_primary=round(mae_pywhy, 6),
        mae_legacy_primary=round(mae_legacy, 6),
        mae_gap_pywhy_minus_legacy=round(mae_gap, 6),
        dual_arm_available=dual_arm_available,
        abs_diff_p95=_percentile(all_abs, 95),
        abs_diff_max=round(max(all_abs), 6) if all_abs else 0.0,
        by_outcome_mae=by_outcome,
    )


def _collect_abs_diffs(rows: list[CausalShadowComparison]) -> list[float]:
    vals: list[float] = []
    for row in rows:
        for value in (row.diff_scores or {}).values():
            vals.append(abs(float(value)))
    return vals


def _collect_outcome_mae(rows: list[CausalShadowComparison]) -> dict[str, float]:
    per_outcome: dict[str, list[float]] = {}
    for row in rows:
        for key, value in (row.diff_scores or {}).items():
            per_outcome.setdefault(str(key), []).append(abs(float(value)))
    return {
        outcome: round(mean(values), 6) if values else 0.0
        for outcome, values in sorted(per_outcome.items())
    }


def _percentile(values: list[float], p: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, int(round((p / 100.0) * (len(ordered) - 1)))))
    return round(float(ordered[rank]), 6)


def _load_recent_rollout_history(
    *,
    history_path: Path,
    history_window_runs: int,
) -> list[dict[str, str]]:
    if not history_path.exists():
        return []
    try:
        rows = list(csv.DictReader(history_path.read_text(encoding="utf-8").splitlines()))
    except Exception:
        return []
    if history_window_runs <= 0:
        return rows
    return rows[-history_window_runs:]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _build_trend_snapshot(
    *,
    metrics: RolloutGateMetrics,
    history_rows: list[dict[str, str]],
) -> dict[str, Any]:
    if not history_rows:
        return {
            "history_window_runs": 0,
            "has_baseline": False,
            "fallback_rate_delta": 0.0,
            "abs_diff_p95_delta": 0.0,
            "by_outcome_mae_worsening": [],
        }

    prev_fallback = [_safe_float(row.get("fallback_rate"), 0.0) for row in history_rows]
    prev_abs_p95 = [_safe_float(row.get("abs_diff_p95"), 0.0) for row in history_rows]
    baseline_fallback = mean(prev_fallback) if prev_fallback else 0.0
    baseline_abs_p95 = mean(prev_abs_p95) if prev_abs_p95 else 0.0

    worsening_outcomes: list[str] = []
    baseline_outcome_mae: dict[str, float] = {}
    for outcome in sorted(metrics.by_outcome_mae):
        key = f"outcome_mae_{outcome}"
        hist_vals = [_safe_float(row.get(key), 0.0) for row in history_rows if row.get(key) is not None]
        if not hist_vals:
            continue
        baseline = mean(hist_vals)
        baseline_outcome_mae[outcome] = round(baseline, 6)
        if metrics.by_outcome_mae.get(outcome, 0.0) - baseline > 0.02:
            worsening_outcomes.append(outcome)

    return {
        "history_window_runs": len(history_rows),
        "has_baseline": True,
        "fallback_rate_baseline": round(baseline_fallback, 6),
        "fallback_rate_delta": round(metrics.fallback_rate - baseline_fallback, 6),
        "abs_diff_p95_baseline": round(baseline_abs_p95, 6),
        "abs_diff_p95_delta": round(metrics.abs_diff_p95 - baseline_abs_p95, 6),
        "by_outcome_mae_baseline": baseline_outcome_mae,
        "by_outcome_mae_worsening": worsening_outcomes,
    }


def _load_latest_gold_eval_metrics() -> dict[str, Any]:
    history_path = Path(".benchmarks/causal/history.csv")
    if not history_path.exists():
        return {}
    try:
        rows = list(csv.DictReader(history_path.read_text(encoding="utf-8").splitlines()))
    except Exception:
        return {}
    if not rows:
        return {}

    latest = rows[-1]
    run_id = str(latest.get("run_id") or "").strip()
    if not run_id:
        return {}
    report_path = Path(".benchmarks/causal") / run_id / "report.json"
    if not report_path.exists():
        return {}
    try:
        payload = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        return {}
    return {
        "run_id": run_id,
        "judge_overall_score": _safe_float(metrics.get("judge_overall_score"), 0.0),
        "mae_overall_legacy": _safe_float(metrics.get("mae_overall_legacy"), 0.0),
        "mae_overall_pywhy": _safe_float(metrics.get("mae_overall_pywhy"), 0.0),
        "rate_limit_error_count": int(_safe_float(metrics.get("rate_limit_error_count"), 0.0)),
    }


def _build_alerts(
    *,
    metrics: RolloutGateMetrics,
    latest_gold_metrics: dict[str, Any],
    run_id: str,
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    now_iso = datetime.now(UTC).isoformat()

    if metrics.fallback_rate > 0.02:
        alerts.append(
            {
                "run_id": run_id,
                "timestamp": now_iso,
                "severity": "warning",
                "metric": "fallback_rate",
                "threshold": ">0.02",
                "value": round(metrics.fallback_rate, 6),
                "recommendation": "Keep shadow mode and inspect pywhy fallback reasons before release switch.",
            }
        )

    rate_limit_errors = int(_safe_float(latest_gold_metrics.get("rate_limit_error_count"), 0.0))
    if rate_limit_errors > 0:
        alerts.append(
            {
                "run_id": run_id,
                "timestamp": now_iso,
                "severity": "warning",
                "metric": "rate_limit_error_count",
                "threshold": ">0",
                "value": rate_limit_errors,
                "recommendation": "Lower judge concurrency or RPM cap to stabilize live eval.",
            }
        )

    judge_score = _safe_float(latest_gold_metrics.get("judge_overall_score"), 0.0)
    if latest_gold_metrics and judge_score < 80:
        alerts.append(
            {
                "run_id": run_id,
                "timestamp": now_iso,
                "severity": "warning",
                "metric": "judge_overall_score",
                "threshold": ">=80",
                "value": round(judge_score, 4),
                "recommendation": "Investigate outcome-level errors and recalibration before enabling pywhy publish mode.",
            }
        )

    mae_legacy = _safe_float(latest_gold_metrics.get("mae_overall_legacy"), 0.0)
    mae_pywhy = _safe_float(latest_gold_metrics.get("mae_overall_pywhy"), 0.0)
    if latest_gold_metrics and mae_pywhy > mae_legacy:
        alerts.append(
            {
                "run_id": run_id,
                "timestamp": now_iso,
                "severity": "warning",
                "metric": "mae_overall_pywhy_vs_legacy",
                "threshold": "<=0",
                "value": round(mae_pywhy - mae_legacy, 6),
                "recommendation": "Continue shadow-only and retrain with wider lookback plus calibration diagnostics.",
            }
        )

    return alerts


def _append_alerts(path: Path, alerts: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for alert in alerts:
            fh.write(json.dumps(alert, ensure_ascii=False) + "\n")


def _write_artifacts(
    *,
    report: RolloutQualityReport,
    run_dir: Path,
    output_root: Path,
) -> None:
    payload = report.to_dict()
    (run_dir / "report.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    lines = [
        f"# Causal Rollout Quality Gate ({report.run_id})",
        "",
        f"- Status: {report.decision.status}",
        f"- Passed: {report.decision.passed}",
        f"- Target percent: {report.config['target_percent']}",
        f"- PyWhy primary ratio: {report.metrics.pywhy_primary_ratio}",
        f"- Fallback rate: {report.metrics.fallback_rate}",
        f"- MAE (pywhy primary): {report.metrics.mae_pywhy_primary}",
        f"- MAE (legacy primary): {report.metrics.mae_legacy_primary}",
        f"- MAE gap (pywhy-legacy): {report.metrics.mae_gap_pywhy_minus_legacy}",
        f"- Sample rows: {report.metrics.sample_rows}",
        f"- Trend window runs: {report.trend.get('history_window_runs', 0)}",
        f"- Alerts: {len(report.alerts)}",
    ]
    if report.decision.reasons:
        lines.extend(["", "## Reasons", *[f"- {r}" for r in report.decision.reasons]])
    (run_dir / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    _append_history(output_root / "history.csv", report)


def _append_history(path: Path, report: RolloutQualityReport) -> None:
    row = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "status": report.decision.status,
        "passed": str(report.decision.passed).lower(),
        "target_percent": str(report.config.get("target_percent", "")),
        "sample_rows": str(report.metrics.sample_rows),
        "pywhy_primary_ratio": f"{report.metrics.pywhy_primary_ratio:.6f}",
        "fallback_rate": f"{report.metrics.fallback_rate:.6f}",
        "mae_pywhy_primary": f"{report.metrics.mae_pywhy_primary:.6f}",
        "mae_legacy_primary": f"{report.metrics.mae_legacy_primary:.6f}",
        "mae_gap_pywhy_minus_legacy": f"{report.metrics.mae_gap_pywhy_minus_legacy:.6f}",
        "abs_diff_p95": f"{report.metrics.abs_diff_p95:.6f}",
        "trend_window_runs": str(int(report.trend.get("history_window_runs", 0) or 0)),
        "trend_fallback_rate_delta": f"{_safe_float(report.trend.get('fallback_rate_delta'), 0.0):.6f}",
        "trend_abs_diff_p95_delta": f"{_safe_float(report.trend.get('abs_diff_p95_delta'), 0.0):.6f}",
        "alerts_count": str(len(report.alerts)),
    }
    for outcome, value in sorted(report.metrics.by_outcome_mae.items()):
        row[f"outcome_mae_{outcome}"] = f"{_safe_float(value, 0.0):.6f}"
    fieldnames = list(row.keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def _build_contexts(*, context_prefix: str, context_count: int) -> list[str]:
    raw_prefix = "".join(ch for ch in str(context_prefix or "rqg") if ch.isalnum() or ch in {"_", "-"})
    prefix = raw_prefix[:24] if raw_prefix else "rqg"
    nonce = uuid.uuid4().hex[:6]
    # DB constraint: causal_feature_snapshots.context is VARCHAR(40).
    return [f"{prefix}_{nonce}_{idx + 1}"[:40] for idx in range(context_count)]
