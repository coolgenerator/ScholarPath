"""Causal gold-set live evaluation runner (manual trigger, report mode)."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import select

from scholarpath.causal_engine import LegacyCausalEngine, PyWhyCausalEngine
from scholarpath.causal_engine.types import CausalEstimateResult, CausalRequestContext
from scholarpath.causal_engine.warning_audit import (
    WarningAudit,
    capture_stage_warnings,
    normalize_warning_mode,
)
from scholarpath.db.models import CausalModelRegistry, TokenUsage
from scholarpath.db.session import async_session_factory
from scholarpath.evals.causal_judge import CausalGoldJudge

DEFAULT_CAUSAL_GOLD_DATASET_PATH = (
    Path(__file__).resolve().parent / "datasets" / "causal_gold_v1.json"
)
DEFAULT_CAUSAL_OUTPUT_DIR = Path(".benchmarks/causal")
logger = logging.getLogger(__name__)

_OUTCOMES = (
    "admission_probability",
    "academic_outcome",
    "career_outcome",
    "life_satisfaction",
    "phd_probability",
)


@dataclass
class CausalGoldCase:
    case_id: str
    cohort: str
    context: str
    student_features: dict[str, float]
    school_features: dict[str, float]
    offer_features: dict[str, float]
    gold_outcomes: dict[str, float]
    gold_tolerance: dict[str, float]
    label_type: str
    intervention_checks: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class CausalGoldDataset:
    dataset_id: str
    version: str
    cases: list[CausalGoldCase]


@dataclass
class CausalGoldPassCase:
    case_id: str
    cohort: str
    context: str
    predicted_outcomes: dict[str, float]
    gold_outcomes: dict[str, float]
    tolerance_by_outcome: dict[str, float]
    abs_errors: dict[str, float]
    within_tolerance: dict[str, bool]
    estimate_confidence: float
    label_type: str
    label_confidence: float
    fallback_used: bool
    fallback_reason: str | None = None
    intervention_checks_total: int = 0
    intervention_checks_passed: int = 0
    elapsed_ms: float = 0.0
    warnings_total: int = 0
    warnings_by_stage: dict[str, int] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class CausalGoldPassReport:
    pass_name: str
    eval_run_id: str
    status: str
    started_at: str
    ended_at: str
    elapsed_seconds: float
    case_count: int = 0
    mae_by_outcome: dict[str, float] = field(default_factory=dict)
    mae_overall: float = 0.0
    brier_admission: float = 0.0
    ece_admission: float = 0.0
    spearman_by_group: dict[str, float] = field(default_factory=dict)
    intervention_direction_pass_rate: float = 0.0
    fallback_rate: float = 0.0
    avg_estimate_confidence: float = 0.0
    engine_case_concurrency: int = 1
    engine_case_p95_ms: float = 0.0
    label_type_counts: dict[str, int] = field(default_factory=dict)
    warnings_total: int = 0
    warnings_by_stage: dict[str, int] = field(default_factory=dict)
    judge_calls: int = 0
    judge_tokens_actual: int = 0
    judge_error_rate: float = 0.0
    judge_case_score_avg: float = 0.0
    judge_field_pass_rate: float = 0.0
    token_usage_by_stage: dict[str, Any] = field(default_factory=dict)
    judge_report: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    cases: list[CausalGoldPassCase] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CausalGoldEvalReport:
    run_id: str
    generated_at: str
    config: dict[str, Any]
    legacy_pass: CausalGoldPassReport
    pywhy_pass: CausalGoldPassReport | None
    metrics: dict[str, Any]
    status: str
    recommendations: list[str]
    judge_summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_causal_gold_dataset(path: str | Path | None = None) -> CausalGoldDataset:
    dataset_path = Path(path) if path is not None else DEFAULT_CAUSAL_GOLD_DATASET_PATH
    payload = json.loads(dataset_path.read_text(encoding="utf-8"))
    cases_raw = payload.get("cases", [])
    if not isinstance(cases_raw, list):
        raise ValueError("Dataset cases must be a list")

    cases: list[CausalGoldCase] = []
    outcome_presence = {name: 0 for name in _OUTCOMES}
    for idx, row in enumerate(cases_raw):
        if not isinstance(row, dict):
            raise ValueError(f"Case[{idx}] must be object")
        case_id = str(row.get("case_id", "")).strip()
        if not case_id:
            raise ValueError(f"Case[{idx}] missing case_id")
        cohort = str(row.get("cohort", "")).strip().lower()
        if cohort not in {"in_db", "out_db"}:
            raise ValueError(f"Case[{idx}] invalid cohort: {cohort}")
        context = str(row.get("context", "")).strip()
        if not context:
            raise ValueError(f"Case[{idx}] missing context")
        label_type = str(row.get("label_type", "")).strip().lower()
        if label_type not in {"true", "proxy"}:
            raise ValueError(f"Case[{idx}] invalid label_type: {label_type}")

        student_features = _coerce_numeric_map(row.get("student_features"), "student_features", idx)
        school_features = _coerce_numeric_map(row.get("school_features"), "school_features", idx)
        offer_features = _coerce_numeric_map(row.get("offer_features", {}), "offer_features", idx)
        gold_outcomes = _coerce_numeric_map(row.get("gold_outcomes"), "gold_outcomes", idx)
        gold_tolerance = _coerce_numeric_map(row.get("gold_tolerance"), "gold_tolerance", idx)

        if "school_selectivity" not in school_features:
            raise ValueError(f"Case[{idx}] requires school_features.school_selectivity")

        for outcome, value in gold_outcomes.items():
            if outcome in outcome_presence:
                outcome_presence[outcome] += 1
            if value < 0 or value > 1:
                raise ValueError(f"Case[{idx}] gold_outcomes[{outcome}] out of [0,1]")
            tol = float(gold_tolerance.get(outcome, 0.1))
            if tol <= 0:
                raise ValueError(f"Case[{idx}] gold_tolerance[{outcome}] must be > 0")

        checks = row.get("intervention_checks", [])
        if not isinstance(checks, list):
            raise ValueError(f"Case[{idx}] intervention_checks must be list")

        cases.append(
            CausalGoldCase(
                case_id=case_id,
                cohort=cohort,
                context=context,
                student_features=student_features,
                school_features=school_features,
                offer_features=offer_features,
                gold_outcomes=gold_outcomes,
                gold_tolerance=gold_tolerance,
                label_type=label_type,
                intervention_checks=[item for item in checks if isinstance(item, dict)],
            )
        )

    if len(cases) != 40:
        raise ValueError(f"Causal gold dataset must contain 40 cases, got {len(cases)}")
    for outcome, count in outcome_presence.items():
        if count < 8:
            raise ValueError(f"Outcome '{outcome}' must appear in >=8 cases, got {count}")

    return CausalGoldDataset(
        dataset_id=str(payload.get("dataset_id", dataset_path.stem)),
        version=str(payload.get("version", "1")),
        cases=cases,
    )


def _select_eval_cases(
    cases: list[CausalGoldCase],
    *,
    sample_size: int,
    sample_strategy: str,
    case_ids: list[str] | None,
) -> list[CausalGoldCase]:
    if not cases:
        raise ValueError("Dataset has no cases")

    case_map = {case.case_id: case for case in cases}
    if case_ids:
        seen: set[str] = set()
        selected: list[CausalGoldCase] = []
        missing: list[str] = []
        for raw_id in case_ids:
            case_id = str(raw_id).strip()
            if not case_id or case_id in seen:
                continue
            seen.add(case_id)
            item = case_map.get(case_id)
            if item is None:
                missing.append(case_id)
            else:
                selected.append(item)
        if missing:
            raise ValueError(f"Unknown case_ids: {', '.join(sorted(missing))}")
        if not selected:
            raise ValueError("case_ids provided but no valid cases selected")
        return selected

    total = len(cases)
    if sample_size <= 0:
        raise ValueError("sample_size must be > 0")
    if sample_size > total:
        raise ValueError(f"sample_size must be <= dataset size ({total})")

    strategy = str(sample_strategy or "full").strip().lower()
    sorted_cases = sorted(cases, key=lambda case: case.case_id)
    if strategy == "full":
        return sorted_cases[:sample_size]

    if strategy == "balanced_fixed":
        if sample_size % 2 != 0:
            raise ValueError("balanced_fixed requires an even sample_size")
        half = sample_size // 2
        in_db = [case for case in sorted_cases if case.cohort == "in_db"]
        out_db = [case for case in sorted_cases if case.cohort == "out_db"]
        if len(in_db) < half or len(out_db) < half:
            raise ValueError(
                "balanced_fixed requires enough in_db/out_db cases for requested sample_size",
            )
        return in_db[:half] + out_db[:half]

    raise ValueError(f"Unknown sample_strategy: {sample_strategy}")


async def run_causal_gold_eval(
    *,
    dataset_path: str | Path | None = None,
    output_dir: str | Path = DEFAULT_CAUSAL_OUTPUT_DIR,
    judge_enabled: bool = True,
    judge_concurrency: int = 2,
    judge_temperature: float = 0.1,
    judge_max_tokens: int = 1200,
    max_rpm_total: int = 180,
    engine_case_concurrency: int = 4,
    warning_mode: str = "count_silent",
    sample_size: int = 40,
    sample_strategy: str = "full",
    case_ids: list[str] | None = None,
    pywhy_model_version_hint: str = "latest_stable",
) -> CausalGoldEvalReport:
    if max_rpm_total > 200:
        raise ValueError("max_rpm_total must be <= 200")
    if max_rpm_total <= 0:
        raise ValueError("max_rpm_total must be > 0")
    if int(engine_case_concurrency) <= 0:
        raise ValueError("engine_case_concurrency must be > 0")
    resolved_warning_mode = normalize_warning_mode(warning_mode)

    dataset = load_causal_gold_dataset(dataset_path)
    sampled_cases = _select_eval_cases(
        dataset.cases,
        sample_size=sample_size,
        sample_strategy=sample_strategy,
        case_ids=case_ids,
    )
    sampled_case_ids = [case.case_id for case in sampled_cases]
    sampled_dataset = CausalGoldDataset(
        dataset_id=dataset.dataset_id,
        version=dataset.version,
        cases=sampled_cases,
    )
    run_id = f"causal-gold-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc).isoformat()
    out_root = Path(output_dir)
    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    async with async_session_factory() as session:
        legacy_engine = LegacyCausalEngine()
        pywhy_engine = PyWhyCausalEngine(
            session=session,
            model_version_hint=pywhy_model_version_hint,
            warning_mode=resolved_warning_mode,
        )
        legacy = await _run_engine_pass(
            session=session,
            run_id=run_id,
            pass_name="legacy",
            dataset=sampled_dataset,
            engine=legacy_engine,
            fallback_engine=legacy_engine,
            case_concurrency=engine_case_concurrency,
            warning_mode=resolved_warning_mode,
        )

        pywhy_precondition = await _has_active_pywhy_model(
            session,
            model_version_hint=pywhy_model_version_hint,
        )
        if not pywhy_precondition:
            pywhy = _build_failed_precondition_pass(
                pass_name="pywhy",
                eval_run_id=f"{run_id}-pywhy",
                reason="No active pywhy model in causal_model_registry",
            )
        else:
            pywhy = await _run_engine_pass(
                session=session,
                run_id=run_id,
                pass_name="pywhy",
                dataset=sampled_dataset,
                engine=pywhy_engine,
                fallback_engine=legacy_engine,
                case_concurrency=engine_case_concurrency,
                warning_mode=resolved_warning_mode,
            )

        judge_summary: dict[str, Any] = {}
        judge_usage_summary = {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0.0, "rate_limit_errors": 0}
        judge_stage_errors: list[dict[str, Any]] = []
        legacy_judge_eval_id = f"{run_id}-legacy-judge"
        pywhy_judge_eval_id = f"{run_id}-pywhy-judge"
        summary_eval_id = f"{run_id}-judge-summary"
        if judge_enabled:
            try:
                judge = CausalGoldJudge(
                    max_rpm_total=max_rpm_total,
                    concurrency=judge_concurrency,
                    temperature=judge_temperature,
                    max_tokens=judge_max_tokens,
                )
            except Exception as exc:
                judge_stage_errors.append({"stage": "judge_init", "error": str(exc)})
                legacy.judge_report = {
                    "pass_name": "legacy",
                    "eval_run_id": legacy_judge_eval_id,
                    "status": "failed",
                    "case_results": [],
                    "case_count": 0,
                    "avg_case_score": 0.0,
                    "field_pass_rate": 0.0,
                    "errors": [{"stage": "judge_init", "error": str(exc)}],
                }
                if pywhy.case_count > 0:
                    pywhy.judge_report = {
                        "pass_name": "pywhy",
                        "eval_run_id": pywhy_judge_eval_id,
                        "status": "failed",
                        "case_results": [],
                        "case_count": 0,
                        "avg_case_score": 0.0,
                        "field_pass_rate": 0.0,
                        "errors": [{"stage": "judge_init", "error": str(exc)}],
                    }
                judge_summary = {
                    "run_id": run_id,
                    "eval_run_id": summary_eval_id,
                    "status": "partial",
                    "overall_score": 0.0,
                    "score_uplift": 0.0,
                    "highlights": [],
                    "risks": ["Judge initialization failed."],
                    "recommendations": ["Check LLM API keys / endpoint connectivity and retry."],
                    "error": str(exc),
                }
            else:
                try:
                    legacy.judge_report = (
                        await judge.evaluate_pass(
                            pass_name="legacy",
                            eval_run_id=legacy_judge_eval_id,
                            case_payloads=_build_judge_case_payloads(legacy.cases),
                            pass_metadata={
                                "mae_overall": legacy.mae_overall,
                                "fallback_rate": legacy.fallback_rate,
                                "intervention_direction_pass_rate": legacy.intervention_direction_pass_rate,
                            },
                        )
                    ).to_dict()
                except Exception as exc:
                    judge_stage_errors.append({"stage": "judge_pass_legacy", "error": str(exc)})
                    legacy.judge_report = {
                        "pass_name": "legacy",
                        "eval_run_id": legacy_judge_eval_id,
                        "status": "failed",
                        "case_results": [],
                        "case_count": 0,
                        "avg_case_score": 0.0,
                        "field_pass_rate": 0.0,
                        "errors": [{"stage": "judge_pass_legacy", "error": str(exc)}],
                    }
                _write_json(run_dir / "judge_cases_legacy.json", legacy.judge_report)

                legacy_judge_usage = await _collect_token_usage(
                    eval_run_id=legacy_judge_eval_id,
                    caller_prefixes=("eval.causal.judge.",),
                )
                _attach_judge_usage(pass_report=legacy, usage=legacy_judge_usage)

                pywhy_summary_for_run: dict[str, Any] | None = None
                if pywhy.case_count > 0:
                    try:
                        pywhy.judge_report = (
                            await judge.evaluate_pass(
                                pass_name="pywhy",
                                eval_run_id=pywhy_judge_eval_id,
                                case_payloads=_build_judge_case_payloads(pywhy.cases),
                                pass_metadata={
                                    "mae_overall": pywhy.mae_overall,
                                    "fallback_rate": pywhy.fallback_rate,
                                    "intervention_direction_pass_rate": pywhy.intervention_direction_pass_rate,
                                },
                            )
                        ).to_dict()
                    except Exception as exc:
                        judge_stage_errors.append({"stage": "judge_pass_pywhy", "error": str(exc)})
                        pywhy.judge_report = {
                            "pass_name": "pywhy",
                            "eval_run_id": pywhy_judge_eval_id,
                            "status": "failed",
                            "case_results": [],
                            "case_count": 0,
                            "avg_case_score": 0.0,
                            "field_pass_rate": 0.0,
                            "errors": [{"stage": "judge_pass_pywhy", "error": str(exc)}],
                        }
                    _write_json(run_dir / "judge_cases_pywhy.json", pywhy.judge_report)
                    pywhy_usage = await _collect_token_usage(
                        eval_run_id=pywhy_judge_eval_id,
                        caller_prefixes=("eval.causal.judge.",),
                    )
                    _attach_judge_usage(pass_report=pywhy, usage=pywhy_usage)
                    pywhy_summary_for_run = {
                        "avg_case_score": pywhy.judge_case_score_avg,
                        "field_pass_rate": pywhy.judge_field_pass_rate,
                        "judge_calls": pywhy.judge_calls,
                        "judge_error_rate": pywhy.judge_error_rate,
                    }
                else:
                    pywhy_summary_for_run = None

                try:
                    run_summary = await judge.evaluate_run(
                        run_id=run_id,
                        eval_run_id=summary_eval_id,
                        legacy_summary={
                            "avg_case_score": legacy.judge_case_score_avg,
                            "field_pass_rate": legacy.judge_field_pass_rate,
                            "judge_calls": legacy.judge_calls,
                            "judge_error_rate": legacy.judge_error_rate,
                        },
                        pywhy_summary=pywhy_summary_for_run,
                        aggregate_metrics={
                            "mae_overall_legacy": legacy.mae_overall,
                            "mae_overall_pywhy": pywhy.mae_overall if pywhy.case_count > 0 else None,
                            "fallback_rate_legacy": legacy.fallback_rate,
                            "fallback_rate_pywhy": pywhy.fallback_rate if pywhy.case_count > 0 else None,
                            "intervention_pass_legacy": legacy.intervention_direction_pass_rate,
                            "intervention_pass_pywhy": pywhy.intervention_direction_pass_rate if pywhy.case_count > 0 else None,
                        },
                    )
                    judge_summary = run_summary.to_dict()
                except Exception as exc:
                    judge_stage_errors.append({"stage": "judge_run", "error": str(exc)})
                    judge_summary = {
                        "run_id": run_id,
                        "eval_run_id": summary_eval_id,
                        "status": "partial",
                        "overall_score": 0.0,
                        "score_uplift": 0.0,
                        "highlights": [],
                        "risks": ["Run-level judge failed."],
                        "recommendations": ["Inspect judge inputs and retry with lower concurrency."],
                        "error": str(exc),
                    }

                _write_json(run_dir / "judge_summary.json", judge_summary)
                judge_usage_summary = await _collect_token_usage(
                    eval_run_id=summary_eval_id,
                    caller_prefixes=("eval.causal.judge.",),
                )

        if judge_stage_errors:
            legacy.errors.extend(judge_stage_errors)
            if legacy.status == "ok":
                legacy.status = "partial"
            if pywhy.case_count > 0:
                pywhy.errors.extend(judge_stage_errors)
                if pywhy.status == "ok":
                    pywhy.status = "partial"

        _write_json(run_dir / "legacy_pass.json", legacy.to_dict())
        _write_json(run_dir / "pywhy_pass.json", pywhy.to_dict())

        metrics = _build_report_metrics(
            legacy=legacy,
            pywhy=pywhy,
            judge_summary=judge_summary,
            judge_usage_summary=judge_usage_summary,
            max_rpm_total=max_rpm_total,
            sampled_case_count=len(sampled_case_ids),
        )
        status = _grade_status(metrics=metrics)
        if judge_stage_errors and status != "failed":
            status = "partial"
        recommendations = _build_recommendations(
            legacy=legacy,
            pywhy=pywhy,
            metrics=metrics,
            status=status,
        )

        report = CausalGoldEvalReport(
            run_id=run_id,
            generated_at=now,
            config={
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.version,
                "dataset_path": str(Path(dataset_path) if dataset_path is not None else DEFAULT_CAUSAL_GOLD_DATASET_PATH),
                "output_dir": str(run_dir),
                "judge_enabled": judge_enabled,
                "judge_concurrency": judge_concurrency,
                "judge_temperature": judge_temperature,
                "judge_max_tokens": judge_max_tokens,
                "max_rpm_total": max_rpm_total,
                "engine_case_concurrency": int(engine_case_concurrency),
                "warning_mode": resolved_warning_mode,
                "sample_size": sample_size,
                "sample_strategy": sample_strategy,
                "sampled_case_ids": sampled_case_ids,
                "pywhy_model_version_hint": str(pywhy_model_version_hint or "latest_stable"),
            },
            legacy_pass=legacy,
            pywhy_pass=pywhy if pywhy.case_count > 0 or pywhy.status == "failed_precondition" else None,
            metrics=metrics,
            status=status,
            recommendations=recommendations,
            judge_summary=judge_summary,
        )
        _write_json(run_dir / "report.json", report.to_dict())
        _write_markdown_summary(run_dir / "summary.md", report)
        _append_history(out_root / "history.csv", report)
        await session.commit()

    return report


async def _run_engine_pass(
    *,
    session,
    run_id: str,
    pass_name: str,
    dataset: CausalGoldDataset,
    engine,
    fallback_engine,
    case_concurrency: int,
    warning_mode: str,
) -> CausalGoldPassReport:
    started = datetime.now(timezone.utc)
    started_at = started.isoformat()
    eval_run_id = f"{run_id}-{pass_name}"
    errors: list[dict[str, Any]] = []
    semaphore = asyncio.Semaphore(max(1, int(case_concurrency)))
    mode = normalize_warning_mode(warning_mode)

    async def evaluate_case(index: int, case: CausalGoldCase) -> tuple[int, CausalGoldPassCase, list[dict[str, Any]]]:
        case_errors: list[dict[str, Any]] = []
        case_warning_audit = WarningAudit()
        case_started_monotonic = time.monotonic()
        async with semaphore:
            ctx = _build_case_context(case=case, run_id=run_id, pass_name=pass_name)
            outcomes = [key for key in case.gold_outcomes.keys() if key]
            fallback_used = False
            fallback_reason = None
            try:
                with capture_stage_warnings(
                    stage=f"{pass_name}.estimate",
                    warning_mode=mode,
                    audit=case_warning_audit,
                ):
                    result: CausalEstimateResult = await engine.estimate(ctx, outcomes)
            except Exception as exc:
                fallback_used = True
                fallback_reason = str(exc)
                case_errors.append(
                    {"stage": "estimate", "case_id": case.case_id, "error": str(exc)}
                )
                try:
                    with capture_stage_warnings(
                        stage=f"{pass_name}.estimate_fallback",
                        warning_mode=mode,
                        audit=case_warning_audit,
                    ):
                        result = await fallback_engine.estimate(ctx, outcomes)
                    result.fallback_used = True
                    result.fallback_reason = fallback_reason
                    result.causal_engine_version = f"{pass_name}_fallback"
                except Exception as fallback_exc:
                    case_errors.append(
                        {
                            "stage": "estimate_fallback",
                            "case_id": case.case_id,
                            "error": str(fallback_exc),
                        },
                    )
                    result = CausalEstimateResult(
                        scores={name: 0.5 for name in outcomes},
                        confidence_by_outcome={name: 0.2 for name in outcomes},
                        estimate_confidence=0.2,
                        label_type=case.label_type,
                        label_confidence=0.2,
                        causal_engine_version=f"{pass_name}_fallback_failed",
                        causal_model_version=None,
                        fallback_used=True,
                        fallback_reason=f"{fallback_reason} | fallback_error={fallback_exc}",
                    )

            estimate_meta = result.metadata if isinstance(result.metadata, dict) else {}
            estimate_warning_stage = estimate_meta.get("warnings_by_stage")
            if isinstance(estimate_warning_stage, dict):
                for stage, count in estimate_warning_stage.items():
                    case_warning_audit.by_stage[str(stage)] = (
                        case_warning_audit.by_stage.get(str(stage), 0) + int(count or 0)
                    )
                case_warning_audit.total += int(estimate_meta.get("warnings_total", 0) or 0)

            abs_errors: dict[str, float] = {}
            within: dict[str, bool] = {}
            tolerance_by_outcome: dict[str, float] = {}
            for outcome, gold_val in case.gold_outcomes.items():
                predicted = float(result.scores.get(outcome, 0.5))
                error = abs(predicted - float(gold_val))
                tol = float(case.gold_tolerance.get(outcome, 0.1))
                tolerance_by_outcome[outcome] = round(tol, 6)
                abs_errors[outcome] = round(error, 6)
                within[outcome] = bool(error <= tol)

            checks_total, checks_passed, check_warning_snapshot = await _run_intervention_checks(
                engine=engine,
                fallback_engine=fallback_engine,
                ctx=ctx,
                checks=case.intervention_checks,
                outcomes=outcomes,
                baseline_scores=result.scores,
                warning_mode=mode,
                warning_stage_prefix=f"{pass_name}.intervention",
            )
            if isinstance(check_warning_snapshot, dict):
                for stage, count in check_warning_snapshot.items():
                    case_warning_audit.by_stage[stage] = (
                        case_warning_audit.by_stage.get(stage, 0) + int(count or 0)
                    )

            case_elapsed_ms = int((time.monotonic() - case_started_monotonic) * 1000)
            case_warning_snapshot = case_warning_audit.snapshot()
            pass_case = CausalGoldPassCase(
                case_id=case.case_id,
                cohort=case.cohort,
                context=case.context,
                predicted_outcomes={k: round(float(v), 6) for k, v in result.scores.items()},
                gold_outcomes={k: round(float(v), 6) for k, v in case.gold_outcomes.items()},
                tolerance_by_outcome=tolerance_by_outcome,
                abs_errors=abs_errors,
                within_tolerance=within,
                estimate_confidence=round(float(result.estimate_confidence), 6),
                label_type=str(result.label_type or case.label_type),
                label_confidence=round(float(result.label_confidence), 6),
                fallback_used=bool(result.fallback_used or fallback_used),
                fallback_reason=result.fallback_reason or fallback_reason,
                intervention_checks_total=checks_total,
                intervention_checks_passed=checks_passed,
                elapsed_ms=float(case_elapsed_ms),
                warnings_total=int(case_warning_snapshot.get("warnings_total", 0) or 0),
                warnings_by_stage=dict(case_warning_snapshot.get("warnings_by_stage", {})),
                errors=[],
            )
            return index, pass_case, case_errors

    coros = [evaluate_case(index, case) for index, case in enumerate(dataset.cases)]
    results = await asyncio.gather(*coros)
    results.sort(key=lambda item: item[0])
    cases: list[CausalGoldPassCase] = []
    all_warning_stage_counts: dict[str, int] = {}
    case_latencies_ms: list[int] = []
    warning_total = 0
    for _, case_report, case_errors in results:
        cases.append(case_report)
        errors.extend(case_errors)
        case_latencies_ms.append(int(case_report.elapsed_ms))
        warning_total += int(case_report.warnings_total)
        for stage, count in case_report.warnings_by_stage.items():
            all_warning_stage_counts[stage] = (
                all_warning_stage_counts.get(stage, 0) + int(count or 0)
            )

    ended = datetime.now(timezone.utc)
    elapsed = max(0.0, (ended - started).total_seconds())
    metrics = _compute_pass_metrics(
        cases,
        warning_mode=mode,
        pass_name=pass_name,
    )
    return CausalGoldPassReport(
        pass_name=pass_name,
        eval_run_id=eval_run_id,
        status="partial" if errors else "ok",
        started_at=started_at,
        ended_at=ended.isoformat(),
        elapsed_seconds=round(elapsed, 4),
        case_count=len(cases),
        mae_by_outcome=metrics["mae_by_outcome"],
        mae_overall=metrics["mae_overall"],
        brier_admission=metrics["brier_admission"],
        ece_admission=metrics["ece_admission"],
        spearman_by_group=metrics["spearman_by_group"],
        intervention_direction_pass_rate=metrics["intervention_direction_pass_rate"],
        fallback_rate=metrics["fallback_rate"],
        avg_estimate_confidence=metrics["avg_estimate_confidence"],
        engine_case_concurrency=max(1, int(case_concurrency)),
        engine_case_p95_ms=round(_percentile(case_latencies_ms, q=0.95), 2) if case_latencies_ms else 0.0,
        label_type_counts=metrics["label_type_counts"],
        warnings_total=int(metrics.get("warnings_total", warning_total) or 0),
        warnings_by_stage=dict(metrics.get("warnings_by_stage", dict(sorted(all_warning_stage_counts.items())))),
        errors=errors,
        cases=cases,
    )


def _build_failed_precondition_pass(
    *,
    pass_name: str,
    eval_run_id: str,
    reason: str,
) -> CausalGoldPassReport:
    now = datetime.now(timezone.utc).isoformat()
    return CausalGoldPassReport(
        pass_name=pass_name,
        eval_run_id=eval_run_id,
        status="failed_precondition",
        started_at=now,
        ended_at=now,
        elapsed_seconds=0.0,
        case_count=0,
        errors=[{"stage": "precondition", "error": reason}],
    )


def _build_case_context(
    *,
    case: CausalGoldCase,
    run_id: str,
    pass_name: str,
) -> CausalRequestContext:
    request_id = f"{run_id}:{pass_name}:{case.case_id}"
    return CausalRequestContext(
        request_id=request_id,
        context=case.context,
        student_id=uuid.uuid5(uuid.NAMESPACE_DNS, f"student:{case.case_id}"),
        school_id=uuid.uuid5(uuid.NAMESPACE_DNS, f"school:{case.case_id}"),
        offer_id=(uuid.uuid5(uuid.NAMESPACE_DNS, f"offer:{case.case_id}") if case.offer_features else None),
        student_features=dict(case.student_features),
        school_features=dict(case.school_features),
        interaction_features=dict(case.offer_features),
        metadata={"cohort": case.cohort, "label_type": case.label_type},
    )


async def _run_intervention_checks(
    *,
    engine,
    fallback_engine,
    ctx: CausalRequestContext,
    checks: list[dict[str, Any]],
    outcomes: list[str],
    baseline_scores: dict[str, float],
    warning_mode: str,
    warning_stage_prefix: str,
) -> tuple[int, int, dict[str, int]]:
    total = 0
    passed = 0
    warning_audit = WarningAudit()
    if not checks:
        return 0, 0, {}
    for item in checks:
        variable = str(item.get("variable_name") or item.get("variable") or "").strip()
        outcome = str(item.get("outcome_name") or "").strip()
        expected = str(item.get("expected_direction") or "non_decrease").strip().lower()
        if not variable or not outcome:
            continue
        total += 1
        base_value = ctx.all_features.get(variable, 0.5)
        if "target_value" in item:
            target_value = _clip01(_safe_float(item.get("target_value"), default=base_value))
        else:
            delta = _safe_float(item.get("delta"), default=0.05)
            target_value = _clip01(base_value + delta)

        overridden_ctx = _context_with_interventions(ctx=ctx, interventions={variable: target_value})
        try:
            with capture_stage_warnings(
                stage=f"{warning_stage_prefix}.estimate",
                warning_mode=warning_mode,
                audit=warning_audit,
            ):
                result = await engine.estimate(overridden_ctx, [outcome])
        except Exception:
            try:
                with capture_stage_warnings(
                    stage=f"{warning_stage_prefix}.estimate_fallback",
                    warning_mode=warning_mode,
                    audit=warning_audit,
                ):
                    result = await fallback_engine.estimate(overridden_ctx, [outcome])
            except Exception:
                continue

        baseline = _safe_float(baseline_scores.get(outcome), default=0.0)
        modified = _safe_float(result.scores.get(outcome), default=baseline)
        effect = modified - baseline
        min_effect = abs(_safe_float(item.get("min_effect"), default=0.0))
        ok = _direction_ok(effect=effect, expected=expected, min_effect=min_effect)
        if ok:
            passed += 1
    return total, passed, dict(sorted(warning_audit.by_stage.items()))


def _context_with_interventions(
    *,
    ctx: CausalRequestContext,
    interventions: dict[str, float],
) -> CausalRequestContext:
    student = dict(ctx.student_features)
    school = dict(ctx.school_features)
    interaction = dict(ctx.interaction_features)
    for key, value in interventions.items():
        if key in student:
            student[key] = float(value)
        elif key in school:
            school[key] = float(value)
        else:
            interaction[key] = float(value)
    return CausalRequestContext(
        request_id=ctx.request_id,
        context=ctx.context,
        student_id=ctx.student_id,
        school_id=ctx.school_id,
        offer_id=ctx.offer_id,
        student_features=student,
        school_features=school,
        interaction_features=interaction,
        metadata=dict(ctx.metadata),
    )


def _direction_ok(*, effect: float, expected: str, min_effect: float) -> bool:
    if expected == "increase":
        return effect >= min_effect
    if expected == "decrease":
        return effect <= -min_effect
    if expected == "non_increase":
        return effect <= min_effect
    # default non_decrease
    return effect >= -min_effect


def _compute_pass_metrics(
    cases: list[CausalGoldPassCase],
    *,
    warning_mode: str,
    pass_name: str,
) -> dict[str, Any]:
    if not cases:
        return {
            "mae_by_outcome": {},
            "mae_overall": 0.0,
            "brier_admission": 0.0,
            "ece_admission": 0.0,
            "spearman_by_group": {},
            "intervention_direction_pass_rate": 0.0,
            "fallback_rate": 0.0,
            "avg_estimate_confidence": 0.0,
            "label_type_counts": {},
        }

    errors_by_outcome: dict[str, list[float]] = {}
    all_errors: list[float] = []
    brier_values: list[float] = []
    ece_pairs: list[tuple[float, float]] = []
    fallback_count = 0
    intervention_total = 0
    intervention_passed = 0
    label_counts: dict[str, int] = {}
    confidence_values: list[float] = []
    warning_total = 0
    warning_by_stage: dict[str, int] = {}

    for case in cases:
        if case.fallback_used:
            fallback_count += 1
        intervention_total += case.intervention_checks_total
        intervention_passed += case.intervention_checks_passed
        label_counts[case.label_type] = label_counts.get(case.label_type, 0) + 1
        confidence_values.append(case.estimate_confidence)
        warning_total += int(case.warnings_total)
        for stage, count in case.warnings_by_stage.items():
            warning_by_stage[stage] = warning_by_stage.get(stage, 0) + int(count or 0)

        for outcome, err in case.abs_errors.items():
            errors_by_outcome.setdefault(outcome, []).append(float(err))
            all_errors.append(float(err))
        pred = case.predicted_outcomes.get("admission_probability")
        gold = case.gold_outcomes.get("admission_probability")
        if pred is not None and gold is not None:
            p = _clip01(float(pred))
            y = _clip01(float(gold))
            brier_values.append((p - y) ** 2)
            ece_pairs.append((p, y))

    mae_by_outcome = {
        outcome: round(sum(vals) / len(vals), 6)
        for outcome, vals in sorted(errors_by_outcome.items())
        if vals
    }
    mae_overall = round(sum(all_errors) / len(all_errors), 6) if all_errors else 0.0
    brier = round(sum(brier_values) / len(brier_values), 6) if brier_values else 0.0
    ece = round(_compute_ece(ece_pairs, bins=10), 6)
    spearman_warning_audit = WarningAudit()
    spearman = _compute_spearman_by_group(
        cases,
        warning_mode=warning_mode,
        warning_audit=spearman_warning_audit,
        warning_stage_prefix=f"{pass_name}.spearman",
    )
    warning_total += int(spearman_warning_audit.total)
    for stage, count in spearman_warning_audit.by_stage.items():
        warning_by_stage[stage] = warning_by_stage.get(stage, 0) + int(count or 0)
    intervention_rate = (
        round(intervention_passed / intervention_total, 6)
        if intervention_total > 0
        else 1.0
    )
    fallback_rate = round(fallback_count / len(cases), 6)
    avg_est_conf = round(sum(confidence_values) / len(confidence_values), 6) if confidence_values else 0.0

    return {
        "mae_by_outcome": mae_by_outcome,
        "mae_overall": mae_overall,
        "brier_admission": brier,
        "ece_admission": ece,
        "spearman_by_group": spearman,
        "intervention_direction_pass_rate": intervention_rate,
        "fallback_rate": fallback_rate,
        "avg_estimate_confidence": avg_est_conf,
        "label_type_counts": label_counts,
        "warnings_total": warning_total,
        "warnings_by_stage": dict(sorted(warning_by_stage.items())),
    }


def _compute_ece(pairs: list[tuple[float, float]], *, bins: int) -> float:
    if not pairs:
        return 0.0
    if bins <= 0:
        bins = 10
    edges = [i / bins for i in range(bins + 1)]
    total = len(pairs)
    acc = 0.0
    for i in range(bins):
        lo = edges[i]
        hi = edges[i + 1]
        if i == bins - 1:
            bucket = [(p, y) for p, y in pairs if lo <= p <= hi]
        else:
            bucket = [(p, y) for p, y in pairs if lo <= p < hi]
        if not bucket:
            continue
        avg_p = sum(p for p, _ in bucket) / len(bucket)
        avg_y = sum(y for _, y in bucket) / len(bucket)
        acc += (len(bucket) / total) * abs(avg_p - avg_y)
    return acc


def _compute_spearman_by_group(
    cases: list[CausalGoldPassCase],
    *,
    warning_mode: str,
    warning_audit: WarningAudit,
    warning_stage_prefix: str,
) -> dict[str, float]:
    rows = []
    for case in cases:
        shared_outcomes = sorted(
            set(case.predicted_outcomes.keys()) & set(case.gold_outcomes.keys())
        )
        pred_vals = [float(case.predicted_outcomes[name]) for name in shared_outcomes]
        gold_vals = [float(case.gold_outcomes[name]) for name in shared_outcomes]
        if not pred_vals or not gold_vals:
            continue
        rows.append(
            {
                "cohort": case.cohort,
                "pred_overall": sum(pred_vals) / len(pred_vals),
                "gold_overall": sum(gold_vals) / len(gold_vals),
            }
        )
    if not rows:
        return {}

    frame = pd.DataFrame(rows)
    out: dict[str, float] = {}
    for cohort, group in frame.groupby("cohort"):
        if len(group) < 2:
            out[str(cohort)] = 0.0
            continue
        with capture_stage_warnings(
            stage=f"{warning_stage_prefix}.{cohort}",
            warning_mode=warning_mode,
            audit=warning_audit,
        ):
            corr = group["pred_overall"].corr(group["gold_overall"], method="spearman")
        out[str(cohort)] = round(float(corr if corr is not None and not math.isnan(corr) else 0.0), 6)
    if len(frame) >= 2:
        with capture_stage_warnings(
            stage=f"{warning_stage_prefix}.all",
            warning_mode=warning_mode,
            audit=warning_audit,
        ):
            corr_all = frame["pred_overall"].corr(frame["gold_overall"], method="spearman")
        out["all"] = round(float(corr_all if corr_all is not None and not math.isnan(corr_all) else 0.0), 6)
    return out


def _build_judge_case_payloads(cases: list[CausalGoldPassCase]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for case in cases:
        fields = []
        for outcome, gold in case.gold_outcomes.items():
            fields.append(
                {
                    "outcome_name": outcome,
                    "predicted_value": case.predicted_outcomes.get(outcome),
                    "gold_value": gold,
                    "abs_error": case.abs_errors.get(outcome),
                    "tolerance": case.tolerance_by_outcome.get(outcome),
                    "within_tolerance": case.within_tolerance.get(outcome, False),
                }
            )

        payloads.append(
            {
                "case_id": case.case_id,
                "cohort": case.cohort,
                "context": case.context,
                "label_type": case.label_type,
                "estimate_confidence": case.estimate_confidence,
                "fallback_used": case.fallback_used,
                "fallback_reason": case.fallback_reason,
                "fields": fields,
            }
        )
    return payloads


def _attach_judge_usage(*, pass_report: CausalGoldPassReport, usage: dict[str, Any]) -> None:
    pass_report.token_usage_by_stage["judge"] = usage
    calls = int(usage.get("calls", 0) or 0)
    errors = int(usage.get("errors", 0) or 0)
    pass_report.judge_calls = calls
    pass_report.judge_tokens_actual = int(usage.get("tokens", 0) or 0)
    pass_report.judge_error_rate = round(errors / calls, 6) if calls > 0 else 0.0
    report = pass_report.judge_report or {}
    pass_report.judge_case_score_avg = round(float(report.get("avg_case_score", 0.0) or 0.0), 6)
    pass_report.judge_field_pass_rate = round(float(report.get("field_pass_rate", 0.0) or 0.0), 6)


async def _collect_token_usage(
    *,
    eval_run_id: str,
    caller_prefixes: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    pattern = f"%#{eval_run_id}"
    async with async_session_factory() as session:
        stmt = select(
            TokenUsage.total_tokens,
            TokenUsage.error,
            TokenUsage.latency_ms,
            TokenUsage.caller,
            TokenUsage.created_at,
        ).where(TokenUsage.caller.like(pattern))
        rows = (await session.execute(stmt)).all()

    prefixes = tuple(
        str(prefix).strip().lower()
        for prefix in (caller_prefixes or ())
        if str(prefix).strip()
    )
    filtered = []
    for total, error, latency, caller, created_at in rows:
        caller_text = str(caller or "").strip().lower()
        if prefixes and not caller_text.startswith(prefixes):
            continue
        filtered.append((total, error, latency, created_at))

    calls = len(filtered)
    errors = sum(1 for _, err, _, _ in filtered if err)
    tokens = int(sum(int(total or 0) for total, _, _, _ in filtered))
    latencies = [int(lat) for _, _, lat, _ in filtered if lat is not None]
    p95_latency = _percentile(latencies, q=0.95) if latencies else 0.0
    rate_limit_errors = sum(
        1
        for _, err, _, _ in filtered
        if err and ("429" in str(err) or "rate limit" in str(err).lower())
    )

    timestamps = [
        item[3].timestamp()
        for item in filtered
        if item[3] is not None
    ]
    rpm_actual = 0.0
    if calls > 1 and timestamps:
        elapsed = max(1e-3, max(timestamps) - min(timestamps))
        rpm_actual = calls / (elapsed / 60.0)
    return {
        "calls": calls,
        "errors": errors,
        "tokens": tokens,
        "p95_latency_ms": round(p95_latency, 2),
        "rate_limit_errors": rate_limit_errors,
        "rpm_actual": round(rpm_actual, 4),
    }


def _build_report_metrics(
    *,
    legacy: CausalGoldPassReport,
    pywhy: CausalGoldPassReport,
    judge_summary: dict[str, Any],
    judge_usage_summary: dict[str, Any],
    max_rpm_total: int,
    sampled_case_count: int,
) -> dict[str, Any]:
    judge_calls_total = legacy.judge_calls + pywhy.judge_calls + int(judge_usage_summary.get("calls", 0) or 0)
    expected_calls = legacy.case_count + (pywhy.case_count if pywhy.case_count > 0 else 0) + (1 if judge_summary else 0)
    retry_estimated = max(0, judge_calls_total - expected_calls)
    rpm_values = [
        float(legacy.token_usage_by_stage.get("judge", {}).get("rpm_actual", 0.0) or 0.0),
        float(pywhy.token_usage_by_stage.get("judge", {}).get("rpm_actual", 0.0) or 0.0),
        float(judge_usage_summary.get("rpm_actual", 0.0) or 0.0),
    ]
    rpm_values = [v for v in rpm_values if v > 0]
    rpm_actual_avg = sum(rpm_values) / len(rpm_values) if rpm_values else 0.0
    rate_limit_error_count = int(
        (legacy.token_usage_by_stage.get("judge", {}) or {}).get("rate_limit_errors", 0)
        + (pywhy.token_usage_by_stage.get("judge", {}) or {}).get("rate_limit_errors", 0)
        + int(judge_usage_summary.get("rate_limit_errors", 0) or 0)
    )
    tokens_actual_judge = int(
        legacy.judge_tokens_actual + pywhy.judge_tokens_actual + int(judge_usage_summary.get("tokens", 0) or 0)
    )
    p95_latency = max(
        float((legacy.token_usage_by_stage.get("judge", {}) or {}).get("p95_latency_ms", 0.0) or 0.0),
        float((pywhy.token_usage_by_stage.get("judge", {}) or {}).get("p95_latency_ms", 0.0) or 0.0),
        float(judge_usage_summary.get("p95_latency_ms", 0.0) or 0.0),
    )
    warnings_total = int((legacy.warnings_total or 0) + (pywhy.warnings_total or 0))
    warnings_by_stage: dict[str, int] = {}
    for bucket in (legacy.warnings_by_stage, pywhy.warnings_by_stage):
        for stage, count in (bucket or {}).items():
            warnings_by_stage[stage] = warnings_by_stage.get(stage, 0) + int(count or 0)
    judge_overall = float(
        judge_summary.get("overall_score", pywhy.judge_case_score_avg if pywhy.case_count > 0 else legacy.judge_case_score_avg)
        or 0.0
    )
    judge_uplift = float(
        judge_summary.get(
            "score_uplift",
            (pywhy.judge_case_score_avg - legacy.judge_case_score_avg) if pywhy.case_count > 0 else 0.0,
        )
        or 0.0
    )

    return {
        "brier_admission_legacy": legacy.brier_admission,
        "brier_admission_pywhy": pywhy.brier_admission if pywhy.case_count > 0 else None,
        "ece_admission_legacy": legacy.ece_admission,
        "ece_admission_pywhy": pywhy.ece_admission if pywhy.case_count > 0 else None,
        "mae_overall_legacy": legacy.mae_overall,
        "mae_overall_pywhy": pywhy.mae_overall if pywhy.case_count > 0 else None,
        "mae_by_outcome_legacy": legacy.mae_by_outcome,
        "mae_by_outcome_pywhy": pywhy.mae_by_outcome if pywhy.case_count > 0 else {},
        "spearman_by_group_legacy": legacy.spearman_by_group,
        "spearman_by_group_pywhy": pywhy.spearman_by_group if pywhy.case_count > 0 else {},
        "intervention_direction_pass_rate_legacy": legacy.intervention_direction_pass_rate,
        "intervention_direction_pass_rate_pywhy": pywhy.intervention_direction_pass_rate if pywhy.case_count > 0 else None,
        "fallback_rate_legacy": legacy.fallback_rate,
        "fallback_rate_pywhy": pywhy.fallback_rate if pywhy.case_count > 0 else None,
        "judge_field_pass_rate_legacy": legacy.judge_field_pass_rate,
        "judge_field_pass_rate_pywhy": pywhy.judge_field_pass_rate if pywhy.case_count > 0 else None,
        "engine_elapsed_seconds_legacy": legacy.elapsed_seconds,
        "engine_elapsed_seconds_pywhy": pywhy.elapsed_seconds if pywhy.case_count > 0 else None,
        "engine_p95_case_ms_legacy": legacy.engine_case_p95_ms,
        "engine_p95_case_ms_pywhy": pywhy.engine_case_p95_ms if pywhy.case_count > 0 else None,
        "engine_case_concurrency": max(legacy.engine_case_concurrency, pywhy.engine_case_concurrency),
        "warnings_total": warnings_total,
        "warnings_by_stage": dict(sorted(warnings_by_stage.items())),
        "judge_overall_score": round(judge_overall, 4),
        "judge_score_uplift_pywhy_vs_legacy": round(judge_uplift, 4),
        "tokens_actual_judge": tokens_actual_judge,
        "p95_latency_ms": round(p95_latency, 2),
        "rpm_actual_avg": round(rpm_actual_avg, 4),
        "rate_limit_error_count": rate_limit_error_count,
        "retry_count_estimated": retry_estimated,
        "max_rpm_total": max_rpm_total,
        "sampled_case_count": int(sampled_case_count),
    }


def _grade_status(*, metrics: dict[str, Any]) -> str:
    pywhy_mae = metrics.get("mae_overall_pywhy")
    legacy_mae = metrics.get("mae_overall_legacy", 1.0)
    judge_score = float(metrics.get("judge_overall_score", 0.0) or 0.0)
    pywhy_field_pass = float(metrics.get("judge_field_pass_rate_pywhy", 0.0) or 0.0)
    fallback_rate = float(metrics.get("fallback_rate_pywhy", metrics.get("fallback_rate_legacy", 1.0)) or 0.0)
    rpm_actual = float(metrics.get("rpm_actual_avg", 0.0) or 0.0)
    rate_limit_errors = int(metrics.get("rate_limit_error_count", 0) or 0)

    pywhy_not_worse = pywhy_mae is None or float(pywhy_mae) <= float(legacy_mae) + 1e-9
    if (
        judge_score >= 80
        and pywhy_not_worse
        and pywhy_field_pass >= 0.6
        and fallback_rate <= 0.2
        and rate_limit_errors == 0
        and rpm_actual <= 200
    ):
        return "good"
    if judge_score >= 60 and pywhy_field_pass >= 0.4 and fallback_rate <= 0.5 and rpm_actual <= 200:
        return "watch"
    return "bad"


def _build_recommendations(
    *,
    legacy: CausalGoldPassReport,
    pywhy: CausalGoldPassReport,
    metrics: dict[str, Any],
    status: str,
) -> list[str]:
    recs: list[str] = []
    if pywhy.case_count == 0 and pywhy.status == "failed_precondition":
        recs.append("Activate one pywhy model before running full dual-pass comparison.")
    if float(metrics.get("rate_limit_error_count", 0) or 0) > 0:
        recs.append("Lower judge concurrency or lower max_rpm_total to reduce rate-limit retries.")
    if int(metrics.get("warnings_total", 0) or 0) > 0:
        recs.append("Review warning-rich stages and keep warning_mode=count_silent for stable runs.")
    if float(metrics.get("fallback_rate_pywhy", 0.0) or 0.0) > 0.2:
        recs.append("Improve pywhy training coverage (feature snapshots + outcome events) to reduce fallbacks.")
    if pywhy.case_count > 0 and pywhy.mae_overall > legacy.mae_overall:
        recs.append("Refine pywhy estimator/feature set because pywhy MAE is currently worse than legacy.")
    if status == "good":
        recs.append("Ready for gradual traffic ramp: 10% -> 50% -> 100%.")
    if not recs:
        recs.append("Keep collecting outcomes and re-run eval to improve confidence intervals.")
    return recs


async def _has_active_pywhy_model(
    session,
    *,
    model_version_hint: str = "latest_stable",
) -> bool:
    hint = str(model_version_hint or "latest_stable").strip().lower()
    if hint != "latest_stable":
        stmt = (
            select(CausalModelRegistry.id)
            .where(CausalModelRegistry.model_version == model_version_hint)
            .limit(1)
        )
        row = await session.execute(stmt)
        return row.first() is not None

    stmt = select(CausalModelRegistry.id).where(CausalModelRegistry.is_active.is_(True)).limit(1)
    row = await session.execute(stmt)
    return row.first() is not None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown_summary(path: Path, report: CausalGoldEvalReport) -> None:
    lines = [
        f"# Causal Gold Eval Summary",
        "",
        f"- Run ID: `{report.run_id}`",
        f"- Generated At: `{report.generated_at}`",
        f"- Status: `{report.status}`",
        f"- Dataset: `{report.config.get('dataset_id')}@{report.config.get('dataset_version')}`",
        f"- MAE Legacy / PyWhy: `{report.metrics.get('mae_overall_legacy')}` / `{report.metrics.get('mae_overall_pywhy')}`",
        f"- Brier(admission) Legacy / PyWhy: `{report.metrics.get('brier_admission_legacy')}` / `{report.metrics.get('brier_admission_pywhy')}`",
        f"- Judge Overall Score: `{report.metrics.get('judge_overall_score')}`",
        f"- Judge Uplift(PyWhy-Legacy): `{report.metrics.get('judge_score_uplift_pywhy_vs_legacy')}`",
        f"- Judge Field Pass Legacy/PyWhy: `{report.metrics.get('judge_field_pass_rate_legacy')}` / `{report.metrics.get('judge_field_pass_rate_pywhy')}`",
        f"- Engine Elapsed Legacy/PyWhy (s): `{report.metrics.get('engine_elapsed_seconds_legacy')}` / `{report.metrics.get('engine_elapsed_seconds_pywhy')}`",
        f"- Engine Case P95 Legacy/PyWhy (ms): `{report.metrics.get('engine_p95_case_ms_legacy')}` / `{report.metrics.get('engine_p95_case_ms_pywhy')}`",
        f"- RPM Actual Avg: `{report.metrics.get('rpm_actual_avg')}` (limit `{report.metrics.get('max_rpm_total')}`)",
        f"- Rate Limit Errors: `{report.metrics.get('rate_limit_error_count')}`",
        f"- Warnings Total: `{report.metrics.get('warnings_total')}`",
        "",
        "## Recommendations",
    ]
    for rec in report.recommendations:
        lines.append(f"- {rec}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_history(path: Path, report: CausalGoldEvalReport) -> None:
    headers = [
        "run_id",
        "generated_at",
        "dataset_id",
        "dataset_version",
        "status",
        "cases_legacy",
        "cases_pywhy",
        "mae_overall_legacy",
        "mae_overall_pywhy",
        "brier_admission_legacy",
        "brier_admission_pywhy",
        "judge_overall_score",
        "judge_score_uplift_pywhy_vs_legacy",
        "judge_field_pass_rate_pywhy",
        "engine_elapsed_seconds_pywhy",
        "warnings_total",
        "rpm_actual_avg",
        "rate_limit_error_count",
    ]
    row = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "dataset_id": report.config.get("dataset_id"),
        "dataset_version": report.config.get("dataset_version"),
        "status": report.status,
        "cases_legacy": report.legacy_pass.case_count,
        "cases_pywhy": report.pywhy_pass.case_count if report.pywhy_pass else 0,
        "mae_overall_legacy": report.metrics.get("mae_overall_legacy"),
        "mae_overall_pywhy": report.metrics.get("mae_overall_pywhy"),
        "brier_admission_legacy": report.metrics.get("brier_admission_legacy"),
        "brier_admission_pywhy": report.metrics.get("brier_admission_pywhy"),
        "judge_overall_score": report.metrics.get("judge_overall_score"),
        "judge_score_uplift_pywhy_vs_legacy": report.metrics.get("judge_score_uplift_pywhy_vs_legacy"),
        "judge_field_pass_rate_pywhy": report.metrics.get("judge_field_pass_rate_pywhy"),
        "engine_elapsed_seconds_pywhy": report.metrics.get("engine_elapsed_seconds_pywhy"),
        "warnings_total": report.metrics.get("warnings_total"),
        "rpm_actual_avg": report.metrics.get("rpm_actual_avg"),
        "rate_limit_error_count": report.metrics.get("rate_limit_error_count"),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=headers)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def _coerce_numeric_map(value: Any, label: str, idx: int) -> dict[str, float]:
    if not isinstance(value, dict):
        raise ValueError(f"Case[{idx}] {label} must be object")
    out: dict[str, float] = {}
    for key, item in value.items():
        name = str(key).strip()
        if not name:
            continue
        try:
            out[name] = float(item)
        except (TypeError, ValueError):
            raise ValueError(f"Case[{idx}] {label}.{name} must be numeric")
    return out


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _percentile(values: list[int], *, q: float) -> float:
    if not values:
        return 0.0
    data = sorted(values)
    if len(data) == 1:
        return float(data[0])
    q = max(0.0, min(1.0, q))
    pos = q * (len(data) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(data[lo])
    frac = pos - lo
    return float(data[lo] + frac * (data[hi] - data[lo]))
