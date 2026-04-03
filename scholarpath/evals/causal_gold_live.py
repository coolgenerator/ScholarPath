"""Causal gold-set live evaluation runner (legacy vs pywhy + LLM judge)."""

from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import statistics
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import and_, or_, select

from scholarpath.causal_engine.legacy_engine import LegacyCausalEngine
from scholarpath.causal_engine.pywhy_engine import PyWhyCausalEngine
from scholarpath.causal_engine.types import CausalRequestContext
from scholarpath.db.models import TokenUsage
from scholarpath.db.session import async_session_factory
from scholarpath.evals.causal_judge import CausalGoldJudge, CausalJudgeCaseResult

DEFAULT_DATASET_PATH = (
    Path(__file__).resolve().parent / "datasets" / "causal_gold_v1.json"
)
DEFAULT_OUTPUT_DIR = Path(".benchmarks/causal")

_OUTCOMES = [
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
]


@dataclass(slots=True)
class GoldCase:
    case_id: str
    cohort: str
    context: str
    student_features: dict[str, float]
    school_features: dict[str, float]
    interaction_features: dict[str, float]
    gold_outcomes: dict[str, float]
    gold_tolerance: dict[str, float]
    label_type: str = "proxy"
    intervention_checks: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class CausalGoldPassReport:
    pass_name: str
    eval_run_id: str
    status: str
    case_count: int
    elapsed_seconds: float
    p95_case_ms: float
    mae_overall: float
    mae_by_outcome: dict[str, float]
    brier_admission: float
    ece_admission: float
    spearman_by_group: dict[str, float]
    intervention_direction_pass_rate: float
    fallback_rate: float
    label_type_counts: dict[str, int]
    judge_calls: int = 0
    judge_tokens_actual: int = 0
    judge_error_rate: float = 0.0
    judge_school_score_avg: float = 0.0
    judge_field_pass_rate: float = 0.0
    cases: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CausalGoldEvalReport:
    run_id: str
    generated_at: str
    status: str
    config: dict[str, Any]
    legacy_pass: CausalGoldPassReport
    pywhy_pass: CausalGoldPassReport
    metrics: dict[str, Any]
    judge_summary: dict[str, Any] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["legacy_pass"] = self.legacy_pass.to_dict()
        payload["pywhy_pass"] = self.pywhy_pass.to_dict()
        return payload


def load_gold_dataset(path: str | Path = DEFAULT_DATASET_PATH) -> list[GoldCase]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("cases")
    if not isinstance(rows, list):
        raise ValueError("Dataset must contain top-level 'cases' list")

    out: list[GoldCase] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"Case {idx} is not an object")
        case_id = str(row.get("case_id") or "").strip()
        if not case_id:
            raise ValueError(f"Case {idx} missing case_id")
        out.append(
            GoldCase(
                case_id=case_id,
                cohort=str(row.get("cohort") or "unknown").strip() or "unknown",
                context=str(row.get("context") or "gold_eval").strip() or "gold_eval",
                student_features=_to_float_dict(row.get("student_features")),
                school_features=_to_float_dict(row.get("school_features")),
                interaction_features=_to_float_dict(row.get("interaction_features")),
                gold_outcomes=_to_float_dict(row.get("gold_outcomes")),
                gold_tolerance=_to_float_dict(row.get("gold_tolerance")),
                label_type=str(row.get("label_type") or "proxy").strip() or "proxy",
                intervention_checks=_to_interventions(row.get("intervention_checks")),
            )
        )
    return out


def select_cases(
    cases: list[GoldCase],
    *,
    sample_size: int | None = None,
    sample_strategy: str = "full",
    case_ids: list[str] | None = None,
) -> list[GoldCase]:
    if case_ids:
        wanted = [item.strip() for item in case_ids if item.strip()]
        ids_map = {item.case_id: item for item in cases}
        missing = [item for item in wanted if item not in ids_map]
        if missing:
            raise ValueError(f"Unknown case_ids: {missing}")
        return [ids_map[item] for item in wanted]

    ordered = sorted(cases, key=lambda item: item.case_id)
    if sample_size is None or sample_size >= len(ordered):
        return ordered

    if sample_strategy == "balanced_fixed":
        size = max(1, int(sample_size))
        half = size // 2
        in_db = [item for item in ordered if item.cohort == "in_db"]
        out_db = [item for item in ordered if item.cohort == "out_db"]
        selected = in_db[:half] + out_db[:half]
        if len(selected) < size:
            remaining = [item for item in ordered if item not in selected]
            selected.extend(remaining[: size - len(selected)])
        return selected[:size]

    return ordered[: max(1, int(sample_size))]


async def run_causal_gold_eval(
    *,
    dataset_path: str | Path = DEFAULT_DATASET_PATH,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    judge_enabled: bool = True,
    judge_concurrency: int = 2,
    judge_temperature: float = 0.1,
    judge_max_tokens: int = 1200,
    max_rpm_total: int = 180,
    sample_size: int | None = 40,
    sample_strategy: str = "full",
    case_ids: list[str] | None = None,
    eval_run_id: str | None = None,
) -> CausalGoldEvalReport:
    if max_rpm_total > 200:
        raise ValueError("max_rpm_total must be <= 200")

    run_id = eval_run_id or f"causal-gold-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    cases = load_gold_dataset(dataset_path)
    selected = select_cases(
        cases,
        sample_size=sample_size,
        sample_strategy=sample_strategy,
        case_ids=case_ids,
    )

    t0 = datetime.now(timezone.utc)
    legacy = LegacyCausalEngine()
    pywhy = PyWhyCausalEngine()
    legacy_report = await _run_engine_pass("legacy", run_id, selected, legacy)
    pywhy_report = await _run_engine_pass("pywhy", run_id, selected, pywhy)

    judge_summary: dict[str, Any] = {}
    if judge_enabled:
        judge = CausalGoldJudge(
            temperature=judge_temperature,
            case_max_tokens=judge_max_tokens,
            run_max_tokens=max(600, judge_max_tokens // 2),
        )
        legacy_judge = await _run_pass_judge(
            judge,
            run_id=run_id,
            pass_name="legacy",
            pass_report=legacy_report,
            concurrency=judge_concurrency,
        )
        pywhy_judge = await _run_pass_judge(
            judge,
            run_id=run_id,
            pass_name="pywhy",
            pass_report=pywhy_report,
            concurrency=judge_concurrency,
        )
        legacy_report.judge_calls = int(legacy_judge["judge_calls"])
        legacy_report.judge_school_score_avg = float(legacy_judge["overall_score"])
        legacy_report.judge_field_pass_rate = float(legacy_judge["field_pass_rate"])
        legacy_report.judge_error_rate = float(legacy_judge["judge_error_rate"])

        pywhy_report.judge_calls = int(pywhy_judge["judge_calls"])
        pywhy_report.judge_school_score_avg = float(pywhy_judge["overall_score"])
        pywhy_report.judge_field_pass_rate = float(pywhy_judge["field_pass_rate"])
        pywhy_report.judge_error_rate = float(pywhy_judge["judge_error_rate"])

        judge_summary = {
            "legacy": legacy_judge,
            "pywhy": pywhy_judge,
            "judge_score_legacy": round(float(legacy_judge["overall_score"]), 4),
            "judge_score_pywhy": round(float(pywhy_judge["overall_score"]), 4),
            "judge_overall_score": round(
                (float(legacy_judge["overall_score"]) + float(pywhy_judge["overall_score"])) / 2.0,
                4,
            ),
            "judge_score_uplift_pywhy_vs_legacy": round(
                float(pywhy_judge["overall_score"]) - float(legacy_judge["overall_score"]),
                4,
            ),
            "judge_cases_legacy": legacy_judge["case_results"],
            "judge_cases_pywhy": pywhy_judge["case_results"],
        }

    t1 = datetime.now(timezone.utc)
    search_usage = await _collect_token_usage(
        start=t0,
        end=t1,
        caller_prefixes=("search.",),
    )
    judge_usage = await _collect_token_usage(
        start=t0,
        end=t1,
        caller_prefixes=("eval.causal.judge.case#", "eval.causal.judge.run#"),
    )
    total_usage = await _collect_token_usage(start=t0, end=t1, caller_prefixes=None)

    legacy_report.judge_tokens_actual = int(judge_usage["tokens"])
    pywhy_report.judge_tokens_actual = int(judge_usage["tokens"])

    metrics = {
        "sampled_case_count": len(selected),
        "brier_admission_legacy": legacy_report.brier_admission,
        "brier_admission_pywhy": pywhy_report.brier_admission,
        "ece_admission_legacy": legacy_report.ece_admission,
        "ece_admission_pywhy": pywhy_report.ece_admission,
        "mae_overall_legacy": legacy_report.mae_overall,
        "mae_overall_pywhy": pywhy_report.mae_overall,
        "judge_score_legacy": float(judge_summary.get("judge_score_legacy", 0.0)),
        "judge_score_pywhy": float(judge_summary.get("judge_score_pywhy", 0.0)),
        "judge_overall_score": float(judge_summary.get("judge_overall_score", 0.0)),
        "judge_score_uplift_pywhy_vs_legacy": float(
            judge_summary.get("judge_score_uplift_pywhy_vs_legacy", 0.0)
        ),
        "rpm_actual_avg": float(total_usage["rpm_actual_avg"]),
        "rate_limit_error_count": int(total_usage["rate_limit_error_count"]),
        "tokens_actual_search": int(search_usage["tokens"]),
        "tokens_actual_judge": int(judge_usage["tokens"]),
        "tokens_actual_total": int(total_usage["tokens"]),
    }

    status = _grade_status(
        judge_score_pywhy=float(metrics["judge_score_pywhy"]),
        pywhy_mae=float(pywhy_report.mae_overall),
        legacy_mae=float(legacy_report.mae_overall),
        pywhy_field_pass_rate=float(pywhy_report.judge_field_pass_rate),
        rate_limit_error_count=int(metrics["rate_limit_error_count"]),
    )
    report = CausalGoldEvalReport(
        run_id=run_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        status=status,
        config={
            "dataset_path": str(dataset_path),
            "judge_enabled": judge_enabled,
            "judge_concurrency": judge_concurrency,
            "judge_temperature": judge_temperature,
            "judge_max_tokens": judge_max_tokens,
            "max_rpm_total": max_rpm_total,
            "sample_size": sample_size,
            "sample_strategy": sample_strategy,
            "case_ids": list(case_ids or []),
            "sampled_case_ids": [item.case_id for item in selected],
        },
        legacy_pass=legacy_report,
        pywhy_pass=pywhy_report,
        metrics=metrics,
        judge_summary=judge_summary,
        recommendations=[],
    )
    _write_artifacts(Path(output_dir), report)
    return report


async def _run_engine_pass(
    pass_name: str,
    run_id: str,
    cases: list[GoldCase],
    engine: Any,
) -> CausalGoldPassReport:
    started = datetime.now(timezone.utc)
    payload_cases: list[dict[str, Any]] = []
    label_type_counts: dict[str, int] = {}
    mae_values: dict[str, list[float]] = {}
    fallback_count = 0
    latency_ms_values: list[float] = []
    intervention_checks = 0
    intervention_passed = 0
    admission_gold: list[float] = []
    admission_pred: list[float] = []
    cohort_admission_pairs: dict[str, list[tuple[float, float]]] = {}

    for case in cases:
        t0 = datetime.now(timezone.utc)
        request = CausalRequestContext(
            context=case.context,
            student_id=f"gold-{case.case_id}",
            school_id=f"school-{case.case_id}",
            student_features=case.student_features,
            school_features=case.school_features,
            interaction_features=case.interaction_features,
            metadata={"case_id": case.case_id, "cohort": case.cohort, "run_id": run_id},
        )
        err: str | None = None
        try:
            estimate = await engine.estimate(request)
            scores = dict(estimate.scores)
            fallback_used = bool(getattr(estimate, "fallback_used", False))
            fallback_reason = getattr(estimate, "fallback_reason", None)
        except Exception as exc:
            scores = {}
            fallback_used = True
            fallback_reason = str(exc)
            err = str(exc)

        elapsed_ms = (datetime.now(timezone.utc) - t0).total_seconds() * 1000.0
        latency_ms_values.append(elapsed_ms)
        if fallback_used:
            fallback_count += 1

        case_errors: dict[str, float] = {}
        for outcome, gold in case.gold_outcomes.items():
            pred = float(scores.get(outcome, 0.0))
            abs_err = abs(pred - float(gold))
            case_errors[outcome] = round(abs_err, 6)
            mae_values.setdefault(outcome, []).append(abs_err)
            if outcome == "admission_probability":
                admission_gold.append(float(gold))
                admission_pred.append(pred)
                cohort_admission_pairs.setdefault(case.cohort, []).append((pred, float(gold)))

        for check in case.intervention_checks:
            intervention_checks += 1
            if _intervention_direction_check(scores, check):
                intervention_passed += 1

        label_type_counts[case.label_type] = label_type_counts.get(case.label_type, 0) + 1
        payload_cases.append(
            {
                "case_id": case.case_id,
                "cohort": case.cohort,
                "label_type": case.label_type,
                "scores": scores,
                "gold_outcomes": case.gold_outcomes,
                "gold_tolerance": case.gold_tolerance,
                "errors": case_errors,
                "error": err,
                "fallback_used": fallback_used,
                "fallback_reason": fallback_reason,
                "latency_ms": round(elapsed_ms, 2),
            }
        )

    mae_by_outcome = {
        outcome: round(float(statistics.fmean(values)), 6) if values else 0.0
        for outcome, values in mae_values.items()
    }
    total_err = sum(sum(values) for values in mae_values.values())
    total_slots = sum(len(values) for values in mae_values.values())
    mae_overall = round(total_err / total_slots, 6) if total_slots else 0.0
    fallback_rate = round(fallback_count / len(cases), 6) if cases else 0.0

    brier = _brier_score(admission_pred, admission_gold)
    ece = _ece_score(admission_pred, admission_gold, bins=10)
    spearman = {
        cohort: _spearman_rank([pred for pred, _ in pairs], [gold for _, gold in pairs])
        for cohort, pairs in cohort_admission_pairs.items()
    }
    intervention_rate = (
        round(intervention_passed / intervention_checks, 6) if intervention_checks else 1.0
    )
    elapsed_seconds = (datetime.now(timezone.utc) - started).total_seconds()

    return CausalGoldPassReport(
        pass_name=pass_name,
        eval_run_id=f"{run_id}-{pass_name}",
        status="ok",
        case_count=len(cases),
        elapsed_seconds=round(elapsed_seconds, 3),
        p95_case_ms=_p95(latency_ms_values),
        mae_overall=mae_overall,
        mae_by_outcome=mae_by_outcome,
        brier_admission=round(brier, 6),
        ece_admission=round(ece, 6),
        spearman_by_group={k: round(v, 6) for k, v in spearman.items()},
        intervention_direction_pass_rate=intervention_rate,
        fallback_rate=fallback_rate,
        label_type_counts=label_type_counts,
        cases=payload_cases,
    )


async def _run_pass_judge(
    judge: CausalGoldJudge,
    *,
    run_id: str,
    pass_name: str,
    pass_report: CausalGoldPassReport,
    concurrency: int,
) -> dict[str, Any]:
    sem = max(1, int(concurrency))
    gate = asyncio.Semaphore(sem)
    case_results: list[CausalJudgeCaseResult] = []

    async def _judge_one(row: dict[str, Any]) -> CausalJudgeCaseResult:
        outcomes: dict[str, dict[str, Any]] = {}
        for outcome, gold in (row.get("gold_outcomes") or {}).items():
            tol = float((row.get("gold_tolerance") or {}).get(outcome, 0.12))
            outcomes[str(outcome)] = {
                "pred": float((row.get("scores") or {}).get(outcome, 0.0)),
                "gold": float(gold),
                "tolerance": tol,
            }
        async with gate:
            return await judge.judge_case(
                run_id=run_id,
                case_id=str(row.get("case_id")),
                outcomes=outcomes,
            )

    judged = await asyncio.gather(*[_judge_one(row) for row in pass_report.cases], return_exceptions=True)
    errors = 0
    for item in judged:
        if isinstance(item, Exception):
            errors += 1
            continue
        case_results.append(item)
        if item.error:
            errors += 1

    summary = await judge.judge_run(
        run_id=run_id,
        pass_name=pass_name,
        case_results=case_results,
        metrics={"mae_overall": pass_report.mae_overall},
    )
    return {
        "status": summary.status,
        "overall_score": round(float(summary.overall_score), 4),
        "field_pass_rate": round(float(summary.field_pass_rate), 4),
        "judge_calls": len(pass_report.cases) + 1,
        "judge_error_rate": round(errors / max(1, len(pass_report.cases)), 4),
        "recommendations": list(summary.recommendations),
        "errors": list(summary.errors),
        "case_results": [asdict(item) for item in case_results],
    }


async def _collect_token_usage(
    *,
    start: datetime,
    end: datetime,
    caller_prefixes: tuple[str, ...] | None,
) -> dict[str, Any]:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return {
            "calls": 0,
            "tokens": 0,
            "errors": 0,
            "p95_latency_ms": 0.0,
            "rate_limit_error_count": 0,
            "rpm_actual_avg": 0.0,
        }
    async with async_session_factory() as session:
        stmt = select(TokenUsage).where(
            and_(TokenUsage.created_at >= start, TokenUsage.created_at <= end),
        )
        if caller_prefixes:
            clauses = [TokenUsage.caller.like(f"{prefix}%") for prefix in caller_prefixes]
            stmt = stmt.where(or_(*clauses))
        rows = (await session.execute(stmt)).scalars().all()

    tokens = sum(int(row.total_tokens or 0) for row in rows)
    calls = len(rows)
    errors = sum(1 for row in rows if row.error)
    latencies = [int(row.latency_ms) for row in rows if row.latency_ms is not None]
    duration_min = max((end - start).total_seconds() / 60.0, 1e-6)
    rate_limit_error_count = sum(
        1
        for row in rows
        if row.error and "rate" in str(row.error).lower() and "limit" in str(row.error).lower()
    )
    return {
        "calls": calls,
        "tokens": tokens,
        "errors": errors,
        "p95_latency_ms": _p95([float(item) for item in latencies]),
        "rate_limit_error_count": rate_limit_error_count,
        "rpm_actual_avg": round(calls / duration_min, 4),
    }


def _grade_status(
    *,
    judge_score_pywhy: float,
    pywhy_mae: float,
    legacy_mae: float,
    pywhy_field_pass_rate: float,
    rate_limit_error_count: int,
) -> str:
    if (
        judge_score_pywhy >= 80.0
        and pywhy_mae <= legacy_mae
        and pywhy_field_pass_rate >= 0.60
        and rate_limit_error_count == 0
    ):
        return "good"
    if (
        judge_score_pywhy >= 72.0
        and pywhy_mae <= legacy_mae + 0.01
        and pywhy_field_pass_rate >= 0.50
    ):
        return "watch"
    return "bad"


def _intervention_direction_check(scores: dict[str, float], check: dict[str, Any]) -> bool:
    outcome = str(check.get("outcome") or "").strip()
    direction = str(check.get("direction") or "").strip().lower()
    baseline = float(check.get("baseline", 0.0))
    predicted = float(scores.get(outcome, baseline))
    if direction == "up":
        return predicted >= baseline
    if direction == "down":
        return predicted <= baseline
    return True


def _brier_score(pred: list[float], gold: list[float]) -> float:
    if not pred or not gold:
        return 0.0
    n = min(len(pred), len(gold))
    return sum((pred[i] - gold[i]) ** 2 for i in range(n)) / n


def _ece_score(pred: list[float], gold: list[float], bins: int = 10) -> float:
    if not pred or not gold:
        return 0.0
    n = min(len(pred), len(gold))
    if n == 0:
        return 0.0
    bin_stats: list[tuple[int, float, float]] = []
    for b in range(bins):
        lo = b / bins
        hi = (b + 1) / bins
        idxs = [i for i in range(n) if (pred[i] >= lo and (pred[i] < hi or (b == bins - 1 and pred[i] <= hi)))]
        if not idxs:
            continue
        conf = sum(pred[i] for i in idxs) / len(idxs)
        acc = sum(gold[i] for i in idxs) / len(idxs)
        bin_stats.append((len(idxs), conf, acc))
    if not bin_stats:
        return 0.0
    return sum((count / n) * abs(conf - acc) for count, conf, acc in bin_stats)


def _spearman_rank(x: list[float], y: list[float]) -> float:
    if len(x) != len(y) or len(x) < 2:
        return 0.0
    rx = _rank(x)
    ry = _rank(y)
    n = len(x)
    num = 6.0 * sum((rx[i] - ry[i]) ** 2 for i in range(n))
    den = n * (n**2 - 1)
    if den == 0:
        return 0.0
    return 1.0 - (num / den)


def _rank(values: list[float]) -> list[float]:
    ordered = sorted((value, idx) for idx, value in enumerate(values))
    ranks = [0.0] * len(values)
    i = 0
    while i < len(ordered):
        j = i
        while j + 1 < len(ordered) and ordered[j + 1][0] == ordered[i][0]:
            j += 1
        rank_value = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[ordered[k][1]] = rank_value
        i = j + 1
    return ranks


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, math.ceil(0.95 * len(ordered)) - 1))
    return round(float(ordered[idx]), 3)


def _to_float_dict(value: Any) -> dict[str, float]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, float] = {}
    for key, raw in value.items():
        try:
            out[str(key)] = float(raw)
        except (TypeError, ValueError):
            continue
    return out


def _to_interventions(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        out.append(dict(item))
    return out


def _write_artifacts(output_root: Path, report: CausalGoldEvalReport) -> None:
    run_dir = output_root / report.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(run_dir / "legacy_pass.json", report.legacy_pass.to_dict())
    _write_json(run_dir / "pywhy_pass.json", report.pywhy_pass.to_dict())
    _write_json(run_dir / "judge_cases_legacy.json", report.judge_summary.get("judge_cases_legacy", []))
    _write_json(run_dir / "judge_cases_pywhy.json", report.judge_summary.get("judge_cases_pywhy", []))
    _write_json(run_dir / "judge_summary.json", report.judge_summary)
    _write_json(run_dir / "report.json", report.to_dict())
    _write_markdown_summary(run_dir / "summary.md", report)
    _append_history(output_root / "history.csv", report)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown_summary(path: Path, report: CausalGoldEvalReport) -> None:
    lines = [
        f"# Causal Gold Eval {report.run_id}",
        "",
        f"- status: `{report.status}`",
        f"- sampled_case_count: `{report.metrics.get('sampled_case_count', 0)}`",
        f"- judge_score_pywhy: `{report.metrics.get('judge_score_pywhy', 0)}`",
        f"- judge_score_legacy: `{report.metrics.get('judge_score_legacy', 0)}`",
        f"- judge_overall_score: `{report.metrics.get('judge_overall_score', 0)}`",
        f"- mae_overall_legacy: `{report.metrics.get('mae_overall_legacy', 0)}`",
        f"- mae_overall_pywhy: `{report.metrics.get('mae_overall_pywhy', 0)}`",
        f"- rpm_actual_avg: `{report.metrics.get('rpm_actual_avg', 0)}`",
        f"- rate_limit_error_count: `{report.metrics.get('rate_limit_error_count', 0)}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_history(path: Path, report: CausalGoldEvalReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    row = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "status": report.status,
        "sampled_case_count": report.metrics.get("sampled_case_count", 0),
        "judge_score_pywhy": report.metrics.get("judge_score_pywhy", 0),
        "judge_score_legacy": report.metrics.get("judge_score_legacy", 0),
        "judge_overall_score": report.metrics.get("judge_overall_score", 0),
        "mae_overall_legacy": report.metrics.get("mae_overall_legacy", 0),
        "mae_overall_pywhy": report.metrics.get("mae_overall_pywhy", 0),
        "rpm_actual_avg": report.metrics.get("rpm_actual_avg", 0),
        "rate_limit_error_count": report.metrics.get("rate_limit_error_count", 0),
    }
    fieldnames = list(row.keys())
    with path.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)
