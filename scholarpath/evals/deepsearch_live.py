"""DeepSearch live evaluation runner (manual trigger, report mode)."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import math
import time
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from scholarpath.evals.deepsearch_judge import DeepSearchLiveJudge
from scholarpath.search.canonical_merge import (
    PRD_EXPANDED_CRITICAL_FIELDS,
    coerce_numeric,
    fingerprint_value,
    normalise_numeric,
    normalise_variable_name,
)

DEFAULT_DATASET_PATH = (
    Path(__file__).resolve().parent
    / "datasets"
    / "deepsearch_live_coldmix_v1.json"
)
DEFAULT_OUTPUT_DIR = Path(".benchmarks/deepsearch")
logger = logging.getLogger(__name__)

COHORT_IN_DB = "in_db"
COHORT_OUT_DB = "out_db"
COHORT_UNKNOWN = "unknown"
COHORT_DB_HIT_MIN_RATIO = 0.8


@dataclass
class SchoolEvalCase:
    school_name: str
    aliases: list[str] = field(default_factory=list)
    required_fields: list[str] = field(default_factory=list)
    rules: dict[str, dict[str, Any]] = field(default_factory=dict)
    cohort: str = COHORT_UNKNOWN


@dataclass
class LiveEvalDataset:
    dataset_id: str
    version: str
    schools: list[SchoolEvalCase]


@dataclass
class DeepSearchPassReport:
    pass_name: str
    eval_run_id: str
    execution_mode: str
    status: str
    started_at: str
    ended_at: str
    elapsed_seconds: float
    db_hit_ratio: float = 0.0
    fallback_trigger_rate: float = 0.0
    self_source_calls: int = 0
    internal_websearch_calls: int = 0
    persisted_data_points: int = 0
    tokens_estimated: int = 0
    tokens_actual: int = 0
    token_calls: int = 0
    token_errors: int = 0
    p95_latency_ms: float = 0.0
    error_rate: float = 0.0
    required_slots: int = 0
    covered_required_slots: int = 0
    required_slot_recall: float = 0.0
    critical_slots: int = 0
    covered_critical_slots: int = 0
    critical_slot_recall: float = 0.0
    rules_checked: int = 0
    rules_passed: int = 0
    rule_pass_rate: float = 0.0
    raw_fact_count_before_merge: int = 0
    unique_fact_count_after_merge: int = 0
    dedupe_drop_count: int = 0
    intra_run_dedupe_drop_ratio: float = 0.0
    multi_source_agreement_count: int = 0
    multi_source_conflict_count: int = 0
    judge_calls: int = 0
    judge_tokens_actual: int = 0
    judge_error_rate: float = 0.0
    judge_school_score_avg: float = 0.0
    judge_field_pass_rate: float = 0.0
    token_usage_by_stage: dict[str, Any] = field(default_factory=dict)
    judge_report: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    search_metadata: dict[str, Any] = field(default_factory=dict)
    school_coverage: dict[str, Any] = field(default_factory=dict)


@dataclass
class DeepSearchLiveEvalReport:
    run_id: str
    generated_at: str
    config: dict[str, Any]
    pass1: DeepSearchPassReport
    pass2: DeepSearchPassReport | None
    metrics: dict[str, Any]
    status: str
    recommendations: list[str]
    judge_summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _normalise_cohort(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if value in {COHORT_IN_DB, COHORT_OUT_DB}:
        return value
    return COHORT_UNKNOWN


def load_dataset(path: str | Path | None = None) -> LiveEvalDataset:
    dataset_path = Path(path) if path is not None else DEFAULT_DATASET_PATH
    payload = json.loads(dataset_path.read_text(encoding="utf-8"))
    defaults_required = [
        normalise_variable_name(field)
        for field in payload.get("required_fields_default", [])
    ]
    defaults_rules = {
        normalise_variable_name(field): dict(rule)
        for field, rule in payload.get("rules_default", {}).items()
    }

    schools: list[SchoolEvalCase] = []
    for item in payload.get("schools", []):
        school_name = str(item.get("school_name", "")).strip()
        if not school_name:
            continue
        aliases = [str(v).strip() for v in item.get("aliases", []) if str(v).strip()]
        required_raw = item.get("required_fields", defaults_required)
        required_fields = sorted(
            {normalise_variable_name(field) for field in required_raw if field}
        )
        item_rules = {
            normalise_variable_name(field): dict(rule)
            for field, rule in item.get("rules", {}).items()
        }
        merged_rules = dict(defaults_rules)
        merged_rules.update(item_rules)
        cohort = _normalise_cohort(item.get("cohort"))
        schools.append(
            SchoolEvalCase(
                school_name=school_name,
                aliases=aliases,
                required_fields=required_fields,
                rules=merged_rules,
                cohort=cohort,
            )
        )

    return LiveEvalDataset(
        dataset_id=str(payload.get("dataset_id", dataset_path.stem)),
        version=str(payload.get("version", "1")),
        schools=schools,
    )


def compute_duplicate_ratio_from_rows(
    rows: list[dict[str, Any]],
    *,
    freshness_days: int,
) -> dict[str, Any]:
    ttl_bucket = f"{max(freshness_days, 0)}d"
    fingerprints: list[tuple[str, str, str, str, str]] = []
    for row in rows:
        school_id = str(row.get("school_id") or "")
        variable = normalise_variable_name(str(row.get("variable_name") or ""))
        source_name = str(row.get("source_name") or "").strip().lower()
        value_text = str(row.get("value_text") or "")
        raw_numeric = row.get("value_numeric")
        numeric = raw_numeric
        if numeric is None:
            numeric = coerce_numeric(value_text, variable_name=variable)
        else:
            try:
                numeric = normalise_numeric(
                    float(numeric),
                    variable_name=variable,
                    value_text=value_text,
                )
            except (TypeError, ValueError):
                numeric = coerce_numeric(value_text, variable_name=variable)

        fingerprints.append(
            (
                school_id,
                variable,
                source_name,
                fingerprint_value(value_text=value_text, value_numeric=numeric),
                ttl_bucket,
            )
        )

    total_rows = len(fingerprints)
    unique_rows = len(set(fingerprints))
    duplicate_rows = max(0, total_rows - unique_rows)
    duplicate_ratio = duplicate_rows / total_rows if total_rows > 0 else 0.0
    return {
        "total_rows": total_rows,
        "unique_rows": unique_rows,
        "duplicate_rows": duplicate_rows,
        "duplicate_ratio": round(duplicate_ratio, 4),
    }


def grade_live_eval_status(
    *,
    critical_slot_recall: float,
    db_hit_uplift: float,
    db_duplicate_row_ratio_90d: float,
) -> dict[str, str]:
    def _grade_high(value: float, good: float, watch: float) -> str:
        if value >= good:
            return "good"
        if value >= watch:
            return "watch"
        return "bad"

    def _grade_low(value: float, good: float, watch: float) -> str:
        if value <= good:
            return "good"
        if value <= watch:
            return "watch"
        return "bad"

    critical_grade = _grade_high(critical_slot_recall, good=0.80, watch=0.60)
    uplift_grade = _grade_high(db_hit_uplift, good=0.20, watch=0.10)
    dup_grade = _grade_low(db_duplicate_row_ratio_90d, good=0.02, watch=0.05)

    grades = {
        "critical_slot_recall": critical_grade,
        "db_hit_uplift": uplift_grade,
        "db_duplicate_row_ratio_90d": dup_grade,
    }
    if "bad" in grades.values():
        grades["overall"] = "bad"
    elif "watch" in grades.values():
        grades["overall"] = "watch"
    else:
        grades["overall"] = "good"
    return grades


async def run_deepsearch_live_eval(
    *,
    student_id: str,
    dataset_path: str | Path | None = None,
    required_fields: list[str] | None = None,
    freshness_days: int = 90,
    max_internal_websearch_calls_per_school: int = 1,
    budget_mode: str = "balanced",
    second_pass: bool = True,
    judge_enabled: bool = True,
    judge_concurrency: int = 2,
    judge_temperature: float = 0.1,
    judge_max_tokens: int = 1200,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    celery_timeout_seconds: int = 900,
    celery_poll_interval_seconds: float = 2.0,
    cold_reset_out_group: bool = True,
    cold_reset_window_days: int | None = None,
    validate_cohort: bool = True,
) -> DeepSearchLiveEvalReport:
    dataset = load_dataset(dataset_path)
    if not dataset.schools:
        raise ValueError("DeepSearch live eval dataset is empty")

    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    benchmark_root = Path(output_dir)
    run_dir = benchmark_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    normalized_override = None
    if required_fields:
        normalized_override = sorted(
            {normalise_variable_name(field) for field in required_fields if field}
        )

    effective_required_fields = normalized_override or sorted(
        {
            normalise_variable_name(field)
            for case in dataset.schools
            for field in case.required_fields
        },
    )
    cohort_cases = _group_cases_by_cohort(dataset.schools)
    reset_window = (
        max(int(cold_reset_window_days), 0)
        if cold_reset_window_days is not None
        else max(freshness_days, 0)
    )
    cold_reset_stats = {
        "schools_targeted": 0,
        "schools_matched": 0,
        "deleted_conflicts": 0,
        "deleted_data_points": 0,
        "window_days": reset_window,
        "enabled": bool(cold_reset_out_group),
    }
    if cold_reset_out_group and cohort_cases.get(COHORT_OUT_DB):
        cold_reset_stats = await _cold_reset_out_group_data(
            out_cases=cohort_cases[COHORT_OUT_DB],
            freshness_days=reset_window,
        )
        cold_reset_stats["enabled"] = True
        cold_reset_stats["window_days"] = reset_window

    cohort_db_hit_ratio_pass1 = await _compute_cohort_db_hit_ratio(
        cases=dataset.schools,
        required_fields_override=normalized_override,
        freshness_days=freshness_days,
    )
    cohort_validation = _validate_cohort_state(
        cohort_db_hit_ratio=cohort_db_hit_ratio_pass1,
        has_in_db=bool(cohort_cases.get(COHORT_IN_DB)),
        has_out_db=bool(cohort_cases.get(COHORT_OUT_DB)),
    )
    if validate_cohort and not cohort_validation.get("ok", True):
        report = _build_cohort_validation_failed_report(
            run_id=run_id,
            run_dir=run_dir,
            dataset=dataset,
            student_id=student_id,
            normalized_override=normalized_override,
            freshness_days=freshness_days,
            max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
            budget_mode=budget_mode,
            second_pass=second_pass,
            judge_enabled=judge_enabled,
            judge_concurrency=judge_concurrency,
            judge_temperature=judge_temperature,
            judge_max_tokens=judge_max_tokens,
            cold_reset_out_group=cold_reset_out_group,
            cold_reset_window_days=reset_window,
            validate_cohort=validate_cohort,
            cohort_validation=cohort_validation,
            cohort_db_hit_ratio_pass1=cohort_db_hit_ratio_pass1,
            cold_reset_stats=cold_reset_stats,
        )
        _write_json(run_dir / "pass1.json", asdict(report.pass1))
        _write_json(run_dir / "report.json", report.to_dict())
        _write_markdown_summary(run_dir / "summary.md", report)
        _append_history(benchmark_root / "history.csv", report)
        return report

    school_names = [case.school_name for case in dataset.schools]
    pass1 = await _run_single_pass(
        pass_name="pass1",
        eval_run_id=f"{run_id}-p1",
        student_id=student_id,
        school_names=school_names,
        dataset=dataset,
        required_fields_override=normalized_override,
        freshness_days=freshness_days,
        max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
        budget_mode=budget_mode,
        celery_timeout_seconds=celery_timeout_seconds,
        celery_poll_interval_seconds=celery_poll_interval_seconds,
        judge_enabled=judge_enabled,
        judge_concurrency=judge_concurrency,
        judge_temperature=judge_temperature,
        judge_max_tokens=judge_max_tokens,
    )
    _write_json(run_dir / "pass1.json", asdict(pass1))
    if judge_enabled and pass1.judge_report:
        _write_json(run_dir / "judge_pass1.json", pass1.judge_report)

    pass2: DeepSearchPassReport | None = None
    cohort_db_hit_ratio_pass2: dict[str, float] = {}
    if second_pass:
        cohort_db_hit_ratio_pass2 = await _compute_cohort_db_hit_ratio(
            cases=dataset.schools,
            required_fields_override=normalized_override,
            freshness_days=freshness_days,
        )
        pass2 = await _run_single_pass(
            pass_name="pass2",
            eval_run_id=f"{run_id}-p2",
            student_id=student_id,
            school_names=school_names,
            dataset=dataset,
            required_fields_override=normalized_override,
            freshness_days=freshness_days,
            max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
            budget_mode=budget_mode,
            celery_timeout_seconds=celery_timeout_seconds,
            celery_poll_interval_seconds=celery_poll_interval_seconds,
            judge_enabled=judge_enabled,
            judge_concurrency=judge_concurrency,
            judge_temperature=judge_temperature,
            judge_max_tokens=judge_max_tokens,
        )
        _write_json(run_dir / "pass2.json", asdict(pass2))
        if judge_enabled and pass2.judge_report:
            _write_json(run_dir / "judge_pass2.json", pass2.judge_report)

    dedupe_audit = await _audit_db_duplicate_ratio(
        school_names=school_names,
        required_fields=effective_required_fields,
        freshness_days=freshness_days,
    )

    pass2_effective = pass2 is not None and pass2.status != "failed"
    primary = pass2 if pass2_effective and pass2 is not None else pass1
    total_estimated = pass1.tokens_estimated + (pass2.tokens_estimated if pass2 else 0)

    db_hit_uplift = 0.0
    external_call_reduction = 0.0
    persist_insert_reduction = 0.0
    if pass2_effective and pass2 is not None:
        db_hit_uplift = pass2.db_hit_ratio - pass1.db_hit_ratio
        external_1 = pass1.self_source_calls + pass1.internal_websearch_calls
        external_2 = pass2.self_source_calls + pass2.internal_websearch_calls
        if external_1 > 0:
            external_call_reduction = (external_1 - external_2) / external_1
        if pass1.persisted_data_points > 0:
            persist_insert_reduction = (
                pass1.persisted_data_points - pass2.persisted_data_points
            ) / pass1.persisted_data_points

    judge_summary: dict[str, Any] = {}
    judge_summary_usage = {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0.0}
    if judge_enabled:
        judge_summary_eval_id = f"{run_id}-judge-summary"
        try:
            judge_summary = await _run_eval_judge_summary(
                run_id=run_id,
                eval_run_id=judge_summary_eval_id,
                pass1_summary={
                    "avg_school_score": pass1.judge_school_score_avg,
                    "field_pass_rate": pass1.judge_field_pass_rate,
                    "judge_calls": pass1.judge_calls,
                    "judge_error_rate": pass1.judge_error_rate,
                },
                pass2_summary=(
                    {
                        "avg_school_score": pass2.judge_school_score_avg,
                        "field_pass_rate": pass2.judge_field_pass_rate,
                        "judge_calls": pass2.judge_calls,
                        "judge_error_rate": pass2.judge_error_rate,
                    }
                    if pass2_effective and pass2 is not None
                    else None
                ),
                aggregate_metrics={
                    "db_hit_uplift": db_hit_uplift,
                    "external_call_reduction": external_call_reduction,
                    "persist_insert_reduction": persist_insert_reduction,
                },
                judge_concurrency=judge_concurrency,
                judge_temperature=judge_temperature,
                judge_max_tokens=judge_max_tokens,
            )
        except Exception as exc:
            judge_summary = {
                "run_id": run_id,
                "eval_run_id": judge_summary_eval_id,
                "status": "bad",
                "overall_score": primary.judge_school_score_avg,
                "score_uplift": (
                    (pass2.judge_school_score_avg - pass1.judge_school_score_avg)
                    if pass2_effective and pass2 is not None
                    else 0.0
                ),
                "highlights": [],
                "risks": ["Run-level judge summary failed."],
                "recommendations": ["Re-run judge summary with available pass outputs."],
                "error": str(exc),
            }
        _write_json(run_dir / "judge_summary.json", judge_summary)
        judge_summary_usage = await _collect_token_usage(
            eval_run_id=judge_summary.get("eval_run_id", f"{run_id}-judge-summary"),
            caller_prefixes=("eval.deepsearch.judge.",),
        )

    pass2_search_tokens = int(
        ((pass2.token_usage_by_stage.get("search", {}) if pass2 else {}) or {}).get("tokens", 0)
        or 0,
    )
    pass2_search_calls = int(
        ((pass2.token_usage_by_stage.get("search", {}) if pass2 else {}) or {}).get("calls", 0)
        or 0,
    )
    pass2_search_errors = int(
        ((pass2.token_usage_by_stage.get("search", {}) if pass2 else {}) or {}).get("errors", 0)
        or 0,
    )
    tokens_actual_search = int(
        ((pass1.token_usage_by_stage.get("search", {}) or {}).get("tokens", 0) or 0)
        + pass2_search_tokens,
    )
    search_calls = int(
        ((pass1.token_usage_by_stage.get("search", {}) or {}).get("calls", 0) or 0)
        + pass2_search_calls,
    )
    search_errors = int(
        ((pass1.token_usage_by_stage.get("search", {}) or {}).get("errors", 0) or 0)
        + pass2_search_errors,
    )

    tokens_actual_judge = int(
        pass1.judge_tokens_actual
        + (pass2.judge_tokens_actual if pass2 else 0)
        + int(judge_summary_usage.get("tokens", 0) or 0),
    )
    judge_calls_total = int(
        pass1.judge_calls
        + (pass2.judge_calls if pass2 else 0)
        + int(judge_summary_usage.get("calls", 0) or 0),
    )
    pass1_judge_errors = int(
        ((pass1.token_usage_by_stage.get("judge", {}) or {}).get("errors", 0) or 0),
    )
    pass2_judge_errors = int(
        ((pass2.token_usage_by_stage.get("judge", {}) if pass2 else {}) or {}).get("errors", 0)
        or 0,
    )
    judge_errors_total = int(
        pass1_judge_errors
        + pass2_judge_errors
        + int(judge_summary_usage.get("errors", 0) or 0),
    )

    total_actual = tokens_actual_search + tokens_actual_judge
    total_calls = search_calls + judge_calls_total
    total_errors = search_errors + judge_errors_total
    error_rate = total_errors / total_calls if total_calls > 0 else 0.0

    db_duplicate_ratio = float(dedupe_audit.get("duplicate_ratio", 0.0) or 0.0)
    grade = grade_live_eval_status(
        critical_slot_recall=primary.critical_slot_recall,
        db_hit_uplift=db_hit_uplift,
        db_duplicate_row_ratio_90d=db_duplicate_ratio,
    )

    judge_score_uplift = 0.0
    if pass2_effective and pass2 is not None:
        judge_score_uplift = pass2.judge_school_score_avg - pass1.judge_school_score_avg
    judge_overall_score = primary.judge_school_score_avg
    if judge_summary:
        judge_overall_score = float(judge_summary.get("overall_score", judge_overall_score) or judge_overall_score)
        judge_score_uplift = float(judge_summary.get("score_uplift", judge_score_uplift) or judge_score_uplift)

    cohort_required_slot_recall_pass1 = _cohort_required_slot_recall(
        cases=dataset.schools,
        school_coverage=pass1.school_coverage,
    )
    cohort_required_slot_recall_pass2 = _cohort_required_slot_recall(
        cases=dataset.schools,
        school_coverage=pass2.school_coverage if pass2 is not None else {},
    )
    cohort_db_hit_uplift = _cohort_metric_uplift(
        before=cohort_db_hit_ratio_pass1,
        after=cohort_db_hit_ratio_pass2 if pass2 is not None else {},
    )

    metrics = {
        "required_slot_recall": round(primary.required_slot_recall, 4),
        "critical_slot_recall": round(primary.critical_slot_recall, 4),
        "db_hit_uplift": round(db_hit_uplift, 4),
        "external_call_reduction": round(external_call_reduction, 4),
        "persist_insert_reduction": round(persist_insert_reduction, 4),
        "intra_run_dedupe_drop_ratio": round(primary.intra_run_dedupe_drop_ratio, 4),
        "db_duplicate_row_ratio_90d": round(db_duplicate_ratio, 4),
        "self_source_calls": primary.self_source_calls,
        "internal_websearch_calls": primary.internal_websearch_calls,
        "fallback_trigger_rate": round(primary.fallback_trigger_rate, 4),
        "tokens_estimated": int(total_estimated),
        "tokens_actual": int(total_actual),
        "tokens_actual_search": int(tokens_actual_search),
        "tokens_actual_judge": int(tokens_actual_judge),
        "judge_actual_tokens": int(tokens_actual_judge),
        "judge_overall_score": round(judge_overall_score, 4),
        "judge_score_uplift_pass2_vs_pass1": round(judge_score_uplift, 4),
        "p95_latency_ms": round(
            max(pass1.p95_latency_ms, pass2.p95_latency_ms if pass2 else 0.0),
            2,
        ),
        "error_rate": round(error_rate, 4),
        "grading": grade,
        "dedupe_audit": dedupe_audit,
        "second_pass_effective": bool(pass2_effective),
        "second_pass_status": pass2.status if pass2 is not None else "not_run",
        "cohort_db_hit_ratio_pass1": cohort_db_hit_ratio_pass1,
        "cohort_db_hit_ratio_pass2": cohort_db_hit_ratio_pass2 if pass2 is not None else {},
        "cohort_required_slot_recall_pass1": cohort_required_slot_recall_pass1,
        "cohort_required_slot_recall_pass2": (
            cohort_required_slot_recall_pass2 if pass2 is not None else {}
        ),
        "cohort_db_hit_uplift": cohort_db_hit_uplift if pass2 is not None else {},
        "cold_reset_deleted_rows": int(cold_reset_stats.get("deleted_data_points", 0) or 0),
        "cold_reset_stats": cold_reset_stats,
        "cohort_validation": cohort_validation,
    }

    recommendations: list[str] = []
    if grade["critical_slot_recall"] != "good":
        recommendations.append(
            "Increase cheap-source coverage for critical fields before fallback is triggered.",
        )
    if grade["db_hit_uplift"] != "good":
        recommendations.append(
            "Increase persistence hit reuse by tightening field routing and freshness strategy.",
        )
    if validate_cohort and cohort_validation.get("ok") is False:
        recommendations.append(
            "Cohort validation failed before pass1; verify in_db/out_db split and DB freshness window.",
        )
    if grade["db_duplicate_row_ratio_90d"] != "good":
        recommendations.append(
            "Tighten persistence fingerprint normalization to reduce near-duplicate rows.",
        )
    if pass2 is not None and not pass2_effective:
        recommendations.append(
            "Pass2 failed; uplift metrics use pass1 baseline only. Re-run after fixing worker/task stability.",
        )
    if judge_enabled and judge_overall_score < 80:
        recommendations.append(
            "Improve field-level evidence quality and consistency to raise judge scores.",
        )
    if not recommendations:
        recommendations.append("Current live eval metrics are within target thresholds.")

    report = DeepSearchLiveEvalReport(
        run_id=run_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        config={
            "dataset_id": dataset.dataset_id,
            "dataset_version": dataset.version,
            "student_id": student_id,
            "schools": school_names,
            "required_fields_override": normalized_override or [],
            "freshness_days": freshness_days,
            "max_internal_websearch_calls_per_school": max_internal_websearch_calls_per_school,
            "budget_mode": budget_mode,
            "second_pass": second_pass,
            "judge_enabled": judge_enabled,
            "judge_concurrency": judge_concurrency,
            "judge_temperature": judge_temperature,
            "judge_max_tokens": judge_max_tokens,
            "cold_reset_out_group": cold_reset_out_group,
            "cold_reset_window_days": reset_window,
            "validate_cohort": validate_cohort,
            "output_dir": str(run_dir),
        },
        pass1=pass1,
        pass2=pass2,
        metrics=metrics,
        status=grade["overall"],
        recommendations=recommendations,
        judge_summary=judge_summary,
    )

    _write_json(run_dir / "report.json", report.to_dict())
    _write_markdown_summary(run_dir / "summary.md", report)
    _append_history(benchmark_root / "history.csv", report)
    return report


def _group_cases_by_cohort(cases: list[SchoolEvalCase]) -> dict[str, list[SchoolEvalCase]]:
    grouped: dict[str, list[SchoolEvalCase]] = defaultdict(list)
    for case in cases:
        grouped[_normalise_cohort(case.cohort)].append(case)
    return dict(grouped)


def _cohort_required_slot_recall(
    *,
    cases: list[SchoolEvalCase],
    school_coverage: dict[str, Any],
) -> dict[str, float]:
    slots: dict[str, int] = defaultdict(int)
    covered: dict[str, int] = defaultdict(int)
    for case in cases:
        cohort = _normalise_cohort(case.cohort)
        coverage = school_coverage.get(case.school_name, {}) if school_coverage else {}
        slots[cohort] += int(coverage.get("required_slots", 0) or 0)
        covered[cohort] += int(coverage.get("covered_required_slots", 0) or 0)

    ratios: dict[str, float] = {}
    for cohort, slot_count in slots.items():
        if slot_count <= 0:
            ratios[cohort] = 1.0
        else:
            ratios[cohort] = round(covered[cohort] / slot_count, 4)
    return ratios


def _cohort_metric_uplift(
    *,
    before: dict[str, float],
    after: dict[str, float],
) -> dict[str, float]:
    keys = sorted(set(before.keys()) | set(after.keys()))
    return {
        key: round(float(after.get(key, 0.0) or 0.0) - float(before.get(key, 0.0) or 0.0), 4)
        for key in keys
    }


def _validate_cohort_state(
    *,
    cohort_db_hit_ratio: dict[str, float],
    has_in_db: bool,
    has_out_db: bool,
) -> dict[str, Any]:
    out_ratio = float(cohort_db_hit_ratio.get(COHORT_OUT_DB, 0.0) or 0.0)
    in_ratio = float(cohort_db_hit_ratio.get(COHORT_IN_DB, 0.0) or 0.0)
    errors: list[str] = []
    if has_out_db and out_ratio > 0.0:
        errors.append(
            f"{COHORT_OUT_DB} ratio expected 0.0 before pass1, got {round(out_ratio, 4)}",
        )
    if has_in_db and in_ratio < COHORT_DB_HIT_MIN_RATIO:
        errors.append(
            f"{COHORT_IN_DB} ratio expected >= {COHORT_DB_HIT_MIN_RATIO}, got {round(in_ratio, 4)}",
        )
    return {
        "ok": not errors,
        "errors": errors,
        "thresholds": {"in_db_min_ratio": COHORT_DB_HIT_MIN_RATIO, "out_db_expected_ratio": 0.0},
        "snapshot": {COHORT_IN_DB: round(in_ratio, 4), COHORT_OUT_DB: round(out_ratio, 4)},
    }


def _build_cohort_validation_failed_report(
    *,
    run_id: str,
    run_dir: Path,
    dataset: LiveEvalDataset,
    student_id: str,
    normalized_override: list[str] | None,
    freshness_days: int,
    max_internal_websearch_calls_per_school: int,
    budget_mode: str,
    second_pass: bool,
    judge_enabled: bool,
    judge_concurrency: int,
    judge_temperature: float,
    judge_max_tokens: int,
    cold_reset_out_group: bool,
    cold_reset_window_days: int,
    validate_cohort: bool,
    cohort_validation: dict[str, Any],
    cohort_db_hit_ratio_pass1: dict[str, float],
    cold_reset_stats: dict[str, Any],
) -> DeepSearchLiveEvalReport:
    now = datetime.now(timezone.utc).isoformat()
    pass1 = DeepSearchPassReport(
        pass_name="pass1",
        eval_run_id=f"{run_id}-p1",
        execution_mode="not_started",
        status="failed",
        started_at=now,
        ended_at=now,
        elapsed_seconds=0.0,
        errors=[
            {
                "stage": "cohort_validation",
                "error": "; ".join(cohort_validation.get("errors", []))
                or "cohort validation failed",
            },
        ],
    )
    metrics = {
        "required_slot_recall": 0.0,
        "critical_slot_recall": 0.0,
        "db_hit_uplift": 0.0,
        "external_call_reduction": 0.0,
        "persist_insert_reduction": 0.0,
        "intra_run_dedupe_drop_ratio": 0.0,
        "db_duplicate_row_ratio_90d": 0.0,
        "self_source_calls": 0,
        "internal_websearch_calls": 0,
        "fallback_trigger_rate": 0.0,
        "tokens_estimated": 0,
        "tokens_actual": 0,
        "tokens_actual_search": 0,
        "tokens_actual_judge": 0,
        "judge_actual_tokens": 0,
        "judge_overall_score": 0.0,
        "judge_score_uplift_pass2_vs_pass1": 0.0,
        "p95_latency_ms": 0.0,
        "error_rate": 0.0,
        "grading": {"critical_slot_recall": "bad", "db_hit_uplift": "bad", "db_duplicate_row_ratio_90d": "good", "overall": "bad"},
        "dedupe_audit": {"status": "skipped"},
        "second_pass_effective": False,
        "second_pass_status": "not_run",
        "cohort_db_hit_ratio_pass1": cohort_db_hit_ratio_pass1,
        "cohort_db_hit_ratio_pass2": {},
        "cohort_required_slot_recall_pass1": {},
        "cohort_required_slot_recall_pass2": {},
        "cohort_db_hit_uplift": {},
        "cold_reset_deleted_rows": int(cold_reset_stats.get("deleted_data_points", 0) or 0),
        "cold_reset_stats": cold_reset_stats,
        "cohort_validation": cohort_validation,
    }
    recommendations = [
        "Cohort preflight validation failed. Fix in_db/out_db data state and rerun.",
    ]
    for err in cohort_validation.get("errors", []):
        recommendations.append(f"Validation detail: {err}")

    return DeepSearchLiveEvalReport(
        run_id=run_id,
        generated_at=now,
        config={
            "dataset_id": dataset.dataset_id,
            "dataset_version": dataset.version,
            "student_id": student_id,
            "schools": [case.school_name for case in dataset.schools],
            "required_fields_override": normalized_override or [],
            "freshness_days": freshness_days,
            "max_internal_websearch_calls_per_school": max_internal_websearch_calls_per_school,
            "budget_mode": budget_mode,
            "second_pass": second_pass,
            "judge_enabled": judge_enabled,
            "judge_concurrency": judge_concurrency,
            "judge_temperature": judge_temperature,
            "judge_max_tokens": judge_max_tokens,
            "cold_reset_out_group": cold_reset_out_group,
            "cold_reset_window_days": cold_reset_window_days,
            "validate_cohort": validate_cohort,
            "output_dir": str(run_dir),
        },
        pass1=pass1,
        pass2=None,
        metrics=metrics,
        status="failed",
        recommendations=recommendations,
        judge_summary={},
    )


async def _resolve_school_ids(
    *,
    names: list[str],
) -> dict[str, Any]:
    from sqlalchemy import func, select

    from scholarpath.db.models import School
    from scholarpath.db.session import async_session_factory

    normalized_names = [str(name).strip() for name in names if str(name).strip()]
    if not normalized_names:
        return {}

    lower_names = [name.lower() for name in normalized_names]
    resolved: dict[str, Any] = {}
    async with async_session_factory() as session:
        exact_stmt = select(func.lower(School.name), School.id).where(
            func.lower(School.name).in_(lower_names),
        )
        for lower_name, school_id in (await session.execute(exact_stmt)).all():
            resolved[str(lower_name)] = school_id

        for name in normalized_names:
            key = name.lower()
            if key in resolved:
                continue
            fuzzy_stmt = (
                select(School.id)
                .where(School.name.ilike(f"%{name}%"))
                .order_by(School.us_news_rank.asc().nullslast())
                .limit(1)
            )
            school_id = (await session.execute(fuzzy_stmt)).scalars().first()
            if school_id is not None:
                resolved[key] = school_id

    return resolved


async def _cold_reset_out_group_data(
    *,
    out_cases: list[SchoolEvalCase],
    freshness_days: int,
) -> dict[str, Any]:
    from sqlalchemy import delete, or_, select

    from scholarpath.db.models import Conflict, DataPoint
    from scholarpath.db.session import async_session_factory

    target_names = [case.school_name for case in out_cases]
    school_map = await _resolve_school_ids(names=target_names)
    school_ids = [school_map[name.lower()] for name in target_names if name.lower() in school_map]
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(freshness_days, 0))
    stats = {
        "schools_targeted": len(target_names),
        "schools_matched": len(set(school_ids)),
        "deleted_conflicts": 0,
        "deleted_data_points": 0,
    }
    if not school_ids:
        return stats

    async with async_session_factory() as session:
        datapoint_ids = (
            await session.execute(
                select(DataPoint.id)
                .where(DataPoint.school_id.in_(school_ids))
                .where(DataPoint.crawled_at >= cutoff),
            )
        ).scalars().all()
        if not datapoint_ids:
            return stats

        conflict_delete = await session.execute(
            delete(Conflict).where(
                or_(
                    Conflict.datapoint_a_id.in_(datapoint_ids),
                    Conflict.datapoint_b_id.in_(datapoint_ids),
                ),
            ),
        )
        datapoint_delete = await session.execute(
            delete(DataPoint).where(DataPoint.id.in_(datapoint_ids)),
        )
        await session.commit()

    stats["deleted_conflicts"] = int(conflict_delete.rowcount or 0)
    stats["deleted_data_points"] = int(datapoint_delete.rowcount or 0)
    return stats


async def _compute_cohort_db_hit_ratio(
    *,
    cases: list[SchoolEvalCase],
    required_fields_override: list[str] | None,
    freshness_days: int,
) -> dict[str, float]:
    from sqlalchemy import select

    from scholarpath.db.models import DataPoint
    from scholarpath.db.session import async_session_factory

    school_map = await _resolve_school_ids(names=[case.school_name for case in cases])
    school_ids = list({school_map[case.school_name.lower()] for case in cases if case.school_name.lower() in school_map})
    if not school_ids:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=max(freshness_days, 0))
    fields_by_school: dict[Any, set[str]] = defaultdict(set)
    async with async_session_factory() as session:
        rows = (
            await session.execute(
                select(DataPoint.school_id, DataPoint.variable_name)
                .where(DataPoint.school_id.in_(school_ids))
                .where(DataPoint.crawled_at >= cutoff),
            )
        ).all()

    for school_id, variable_name in rows:
        fields_by_school[school_id].add(normalise_variable_name(str(variable_name)))

    slots: dict[str, int] = defaultdict(int)
    covered: dict[str, int] = defaultdict(int)
    for case in cases:
        cohort = _normalise_cohort(case.cohort)
        required_fields = required_fields_override or case.required_fields
        canonical_required = {
            normalise_variable_name(field)
            for field in required_fields
            if normalise_variable_name(field)
        }
        slots[cohort] += len(canonical_required)
        school_id = school_map.get(case.school_name.lower())
        if school_id is None:
            continue
        covered[cohort] += len(fields_by_school.get(school_id, set()) & canonical_required)

    ratios: dict[str, float] = {}
    for cohort, slot_count in slots.items():
        if slot_count <= 0:
            ratios[cohort] = 1.0
        else:
            ratios[cohort] = round(covered[cohort] / slot_count, 4)
    return ratios


async def _run_single_pass(
    *,
    pass_name: str,
    eval_run_id: str,
    student_id: str,
    school_names: list[str],
    dataset: LiveEvalDataset,
    required_fields_override: list[str] | None,
    freshness_days: int,
    max_internal_websearch_calls_per_school: int,
    budget_mode: str,
    celery_timeout_seconds: int,
    celery_poll_interval_seconds: float,
    judge_enabled: bool,
    judge_concurrency: int,
    judge_temperature: float,
    judge_max_tokens: int,
) -> DeepSearchPassReport:
    started_at = datetime.now(timezone.utc)
    mode, result, stage_errors = await _execute_deepsearch_task(
        student_id=student_id,
        school_names=school_names,
        required_fields=required_fields_override,
        freshness_days=freshness_days,
        max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
        budget_mode=budget_mode,
        eval_run_id=eval_run_id,
        timeout_seconds=celery_timeout_seconds,
        poll_interval_seconds=celery_poll_interval_seconds,
    )
    ended_at = datetime.now(timezone.utc)
    elapsed = max(0.0, (ended_at - started_at).total_seconds())

    search_metadata = dict(result.get("search_metadata", {}))
    task_errors = list(result.get("errors", []))
    all_errors: list[dict[str, Any]] = []
    all_errors.extend(stage_errors)
    all_errors.extend(task_errors)

    schools_payload = list(result.get("schools", []))
    coverage = _compute_slot_coverage(
        cases=dataset.schools,
        schools_payload=schools_payload,
        required_fields_override=required_fields_override,
    )
    search_usage = await _collect_token_usage(
        eval_run_id=eval_run_id,
        caller_prefixes=("search.",),
    )
    judge_report: dict[str, Any] = {}
    if judge_enabled:
        school_cases = [
            {
                "school_name": case.school_name,
                "aliases": list(case.aliases),
                "required_fields": list(case.required_fields),
                "rules": dict(case.rules),
            }
            for case in dataset.schools
        ]
        pass_metadata = {
            "db_hit_ratio": float(search_metadata.get("db_hit_ratio", 0.0) or 0.0),
            "fallback_trigger_rate": float(search_metadata.get("fallback_trigger_rate", 0.0) or 0.0),
            "self_source_calls": int(search_metadata.get("self_source_calls", 0) or 0),
            "internal_websearch_calls": int(search_metadata.get("internal_websearch_calls", 0) or 0),
            "coverage_summary": {
                "required_slot_recall": float(coverage["required_slot_recall"]),
                "critical_slot_recall": float(coverage["critical_slot_recall"]),
            },
        }
        try:
            judge_report = await _run_pass_judge(
                pass_name=pass_name,
                eval_run_id=eval_run_id,
                school_cases=school_cases,
                schools_payload=schools_payload,
                pass_metadata=pass_metadata,
                required_fields_override=required_fields_override,
                judge_concurrency=judge_concurrency,
                judge_temperature=judge_temperature,
                judge_max_tokens=judge_max_tokens,
            )
        except Exception as exc:
            all_errors.append({"stage": "judge_pass", "error": str(exc)})
            judge_report = {
                "pass_name": pass_name,
                "eval_run_id": eval_run_id,
                "status": "failed",
                "school_results": [],
                "school_count": 0,
                "avg_school_score": 0.0,
                "field_pass_rate": 0.0,
                "errors": [{"stage": "judge_pass", "error": str(exc)}],
            }
        judge_errors = judge_report.get("errors", [])
        if isinstance(judge_errors, list):
            all_errors.extend([item for item in judge_errors if isinstance(item, dict)])

    judge_usage = await _collect_token_usage(
        eval_run_id=eval_run_id,
        caller_prefixes=("eval.deepsearch.judge.",),
    )
    usage_stats = await _collect_token_usage(eval_run_id=eval_run_id)
    token_usage_by_stage = {
        "search": search_usage,
        "judge": judge_usage,
        "total": usage_stats,
    }
    search_metadata["token_usage_by_stage"] = token_usage_by_stage
    if judge_enabled:
        search_metadata["judge_report"] = judge_report

    raw_facts = int(search_metadata.get("raw_fact_count_before_merge", 0) or 0)
    dedupe_drop = int(search_metadata.get("dedupe_drop_count", 0) or 0)
    dedupe_ratio = dedupe_drop / raw_facts if raw_facts > 0 else 0.0

    status = "ok"
    if all_errors:
        status = "partial"
    if mode == "failed":
        status = "failed"

    token_calls = int(usage_stats.get("calls", 0) or 0)
    token_errors = int(usage_stats.get("errors", 0) or 0)
    error_rate = token_errors / token_calls if token_calls > 0 else 0.0
    judge_calls = int(judge_usage.get("calls", 0) or 0)
    judge_errors = int(judge_usage.get("errors", 0) or 0)
    judge_error_rate = judge_errors / judge_calls if judge_calls > 0 else 0.0
    judge_school_score_avg = 0.0
    judge_field_pass_rate = 0.0
    if judge_report:
        judge_school_score_avg = float(judge_report.get("avg_school_score", 0.0) or 0.0)
        judge_field_pass_rate = float(judge_report.get("field_pass_rate", 0.0) or 0.0)

    return DeepSearchPassReport(
        pass_name=pass_name,
        eval_run_id=eval_run_id,
        execution_mode=mode,
        status=status,
        started_at=started_at.isoformat(),
        ended_at=ended_at.isoformat(),
        elapsed_seconds=round(elapsed, 2),
        db_hit_ratio=float(search_metadata.get("db_hit_ratio", 0.0) or 0.0),
        fallback_trigger_rate=float(search_metadata.get("fallback_trigger_rate", 0.0) or 0.0),
        self_source_calls=int(search_metadata.get("self_source_calls", 0) or 0),
        internal_websearch_calls=int(search_metadata.get("internal_websearch_calls", 0) or 0),
        persisted_data_points=int(search_metadata.get("persisted_data_points", 0) or 0),
        tokens_estimated=int(
            (search_metadata.get("tokens_by_stage", {}) or {}).get("total_estimated", 0) or 0,
        ),
        tokens_actual=int(usage_stats.get("tokens", 0) or 0),
        token_calls=token_calls,
        token_errors=token_errors,
        p95_latency_ms=float(usage_stats.get("p95_latency_ms", 0.0) or 0.0),
        error_rate=round(error_rate, 4),
        required_slots=int(coverage["required_slots"]),
        covered_required_slots=int(coverage["covered_required_slots"]),
        required_slot_recall=float(coverage["required_slot_recall"]),
        critical_slots=int(coverage["critical_slots"]),
        covered_critical_slots=int(coverage["covered_critical_slots"]),
        critical_slot_recall=float(coverage["critical_slot_recall"]),
        rules_checked=int(coverage["rules_checked"]),
        rules_passed=int(coverage["rules_passed"]),
        rule_pass_rate=float(coverage["rule_pass_rate"]),
        raw_fact_count_before_merge=raw_facts,
        unique_fact_count_after_merge=int(search_metadata.get("unique_fact_count_after_merge", 0) or 0),
        dedupe_drop_count=dedupe_drop,
        intra_run_dedupe_drop_ratio=round(dedupe_ratio, 4),
        multi_source_agreement_count=int(search_metadata.get("multi_source_agreement_count", 0) or 0),
        multi_source_conflict_count=int(search_metadata.get("multi_source_conflict_count", 0) or 0),
        judge_calls=judge_calls,
        judge_tokens_actual=int(judge_usage.get("tokens", 0) or 0),
        judge_error_rate=round(judge_error_rate, 4),
        judge_school_score_avg=round(judge_school_score_avg, 4),
        judge_field_pass_rate=round(judge_field_pass_rate, 4),
        token_usage_by_stage=token_usage_by_stage,
        judge_report=judge_report,
        errors=all_errors,
        search_metadata=search_metadata,
        school_coverage=coverage["by_school"],
    )


async def _run_pass_judge(
    *,
    pass_name: str,
    eval_run_id: str,
    school_cases: list[dict[str, Any]],
    schools_payload: list[dict[str, Any]],
    pass_metadata: dict[str, Any],
    required_fields_override: list[str] | None,
    judge_concurrency: int,
    judge_temperature: float,
    judge_max_tokens: int,
) -> dict[str, Any]:
    judge = DeepSearchLiveJudge(
        concurrency=judge_concurrency,
        temperature=judge_temperature,
        max_tokens=judge_max_tokens,
    )
    result = await judge.evaluate_pass(
        pass_name=pass_name,
        eval_run_id=eval_run_id,
        school_cases=school_cases,
        schools_payload=schools_payload,
        pass_metadata=pass_metadata,
        required_fields_override=required_fields_override,
    )
    return result.to_dict()


async def _run_eval_judge_summary(
    *,
    run_id: str,
    eval_run_id: str,
    pass1_summary: dict[str, Any],
    pass2_summary: dict[str, Any] | None,
    aggregate_metrics: dict[str, Any],
    judge_concurrency: int,
    judge_temperature: float,
    judge_max_tokens: int,
) -> dict[str, Any]:
    judge = DeepSearchLiveJudge(
        concurrency=judge_concurrency,
        temperature=judge_temperature,
        max_tokens=judge_max_tokens,
    )
    result = await judge.evaluate_run(
        run_id=run_id,
        eval_run_id=eval_run_id,
        pass1_summary=pass1_summary,
        pass2_summary=pass2_summary,
        aggregate_metrics=aggregate_metrics,
    )
    return result.to_dict()


async def _execute_deepsearch_task(
    *,
    student_id: str,
    school_names: list[str],
    required_fields: list[str] | None,
    freshness_days: int,
    max_internal_websearch_calls_per_school: int,
    budget_mode: str,
    eval_run_id: str,
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    try:
        result = await _run_via_celery(
            student_id=student_id,
            school_names=school_names,
            required_fields=required_fields,
            freshness_days=freshness_days,
            max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
            budget_mode=budget_mode,
            eval_run_id=eval_run_id,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )
        return "celery", result, errors
    except Exception as exc:
        errors.append({"stage": "celery", "error": str(exc)})

    try:
        result = await _run_via_local(
            student_id=student_id,
            school_names=school_names,
            required_fields=required_fields,
            freshness_days=freshness_days,
            max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
            budget_mode=budget_mode,
            eval_run_id=eval_run_id,
        )
        return "local_fallback", result, errors
    except Exception as exc:
        errors.append({"stage": "local", "error": str(exc)})
        return "failed", {"errors": errors}, errors


async def _run_via_celery(
    *,
    student_id: str,
    school_names: list[str],
    required_fields: list[str] | None,
    freshness_days: int,
    max_internal_websearch_calls_per_school: int,
    budget_mode: str,
    eval_run_id: str,
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    from scholarpath.tasks import run_deep_search

    await _ensure_celery_queue_ready(expected_queue="deep_search")

    async_result = await asyncio.to_thread(
        run_deep_search.delay,
        student_id=student_id,
        school_names=school_names,
        required_fields=required_fields,
        freshness_days=freshness_days,
        max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
        budget_mode=budget_mode,
        eval_run_id=eval_run_id,
    )

    deadline = time.monotonic() + max(timeout_seconds, 1)
    while True:
        status = await asyncio.to_thread(lambda: async_result.status)
        if status == "SUCCESS":
            payload = await asyncio.to_thread(lambda: async_result.result)
            if isinstance(payload, dict):
                return payload
            raise RuntimeError("Celery task completed without dict payload")
        if status in {"FAILURE", "REVOKED"}:
            detail = await asyncio.to_thread(lambda: async_result.result)
            raise RuntimeError(f"Celery task failed status={status}: {detail}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Celery task timed out after {timeout_seconds}s")
        await asyncio.sleep(max(0.1, poll_interval_seconds))


async def _ensure_celery_queue_ready(*, expected_queue: str) -> None:
    from scholarpath.tasks import run_deep_search

    def _probe() -> set[str]:
        inspect = run_deep_search.app.control.inspect(timeout=1.0)
        active_queues = inspect.active_queues() or {}
        if not active_queues:
            raise RuntimeError("No celery workers responded to inspect.active_queues")

        queue_names: set[str] = set()
        for worker_queues in active_queues.values():
            for queue in worker_queues or []:
                name = str((queue or {}).get("name", "")).strip()
                if name:
                    queue_names.add(name)
        return queue_names

    queue_names = await asyncio.to_thread(_probe)
    if expected_queue not in queue_names:
        raise RuntimeError(
            f"Celery workers are online but queue '{expected_queue}' is not active. "
            f"active_queues={sorted(queue_names)}",
        )
    logger.info(
        "Celery preflight queue check passed for '%s' (active=%s)",
        expected_queue,
        sorted(queue_names),
    )


async def _run_via_local(
    *,
    student_id: str,
    school_names: list[str],
    required_fields: list[str] | None,
    freshness_days: int,
    max_internal_websearch_calls_per_school: int,
    budget_mode: str,
    eval_run_id: str,
) -> dict[str, Any]:
    from scholarpath.tasks.deep_search import _run_deep_search_async

    student_uuid = uuid.UUID(student_id)
    return await _run_deep_search_async(
        student_id=student_uuid,
        school_names=school_names,
        required_fields=required_fields,
        freshness_days=freshness_days,
        max_internal_websearch_calls_per_school=max_internal_websearch_calls_per_school,
        budget_mode=budget_mode,
        eval_run_id=eval_run_id,
    )


async def _collect_token_usage(
    eval_run_id: str,
    caller_prefixes: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    try:
        from sqlalchemy import select

        from scholarpath.db.models import TokenUsage
        from scholarpath.db.session import async_session_factory
    except Exception as exc:  # pragma: no cover - depends on runtime env
        return {"calls": 0, "errors": 0, "tokens": 0, "p95_latency_ms": 0.0, "error": str(exc)}

    pattern = f"%#{eval_run_id}"
    async with async_session_factory() as session:
        stmt = select(
            TokenUsage.total_tokens,
            TokenUsage.error,
            TokenUsage.latency_ms,
            TokenUsage.caller,
        ).where(TokenUsage.caller.like(pattern))
        rows = (await session.execute(stmt)).all()

    prefixes = tuple(
        str(prefix).strip().lower()
        for prefix in (caller_prefixes or ())
        if str(prefix).strip()
    )
    filtered = []
    for total, error, latency, caller in rows:
        caller_text = str(caller or "").strip().lower()
        if prefixes and not caller_text.startswith(prefixes):
            continue
        filtered.append((total, error, latency))

    rows_for_stats = filtered if prefixes else [(t, e, l) for t, e, l, _ in rows]

    calls = len(rows_for_stats)
    errors = sum(1 for _, error, _ in rows_for_stats if error)
    tokens = int(sum(int(total or 0) for total, _, _ in rows_for_stats))
    latencies = [int(latency) for _, _, latency in rows_for_stats if latency is not None]
    p95_latency = _percentile(latencies, 0.95) if latencies else 0.0
    return {
        "calls": calls,
        "errors": errors,
        "tokens": tokens,
        "p95_latency_ms": round(p95_latency, 2),
    }


async def _audit_db_duplicate_ratio(
    *,
    school_names: list[str],
    required_fields: list[str],
    freshness_days: int,
) -> dict[str, Any]:
    try:
        from sqlalchemy import func, select

        from scholarpath.db.models import DataPoint, School
        from scholarpath.db.session import async_session_factory
    except Exception as exc:  # pragma: no cover - depends on runtime env
        return {
            "status": "unavailable",
            "duplicate_ratio": 0.0,
            "error": str(exc),
        }

    cutoff = datetime.now(timezone.utc) - timedelta(days=max(freshness_days, 0))
    normalized_required = {normalise_variable_name(field) for field in required_fields}

    async with async_session_factory() as session:
        school_map: dict[str, Any] = {}
        lower_names = [school.lower() for school in school_names]
        exact_stmt = select(func.lower(School.name), School.id).where(
            func.lower(School.name).in_(lower_names),
        )
        for lower_name, school_id in (await session.execute(exact_stmt)).all():
            school_map[lower_name] = school_id

        for school in school_names:
            key = school.lower()
            if key in school_map:
                continue
            fuzzy_stmt = (
                select(School.id)
                .where(School.name.ilike(f"%{school}%"))
                .order_by(School.us_news_rank.asc().nullslast())
                .limit(1)
            )
            school_id = (await session.execute(fuzzy_stmt)).scalars().first()
            if school_id is not None:
                school_map[key] = school_id

        school_ids = list(school_map.values())
        if not school_ids:
            return {
                "status": "ok",
                "duplicate_ratio": 0.0,
                "total_rows": 0,
                "unique_rows": 0,
                "duplicate_rows": 0,
            }

        rows_stmt = (
            select(
                DataPoint.school_id,
                DataPoint.variable_name,
                DataPoint.source_name,
                DataPoint.value_text,
                DataPoint.value_numeric,
            )
            .where(DataPoint.school_id.in_(school_ids))
            .where(DataPoint.crawled_at >= cutoff)
        )
        all_rows = (await session.execute(rows_stmt)).all()

    row_dicts = []
    for school_id, variable_name, source_name, value_text, value_numeric in all_rows:
        canonical_var = normalise_variable_name(str(variable_name))
        if normalized_required and canonical_var not in normalized_required:
            continue
        row_dicts.append(
            {
                "school_id": str(school_id),
                "variable_name": canonical_var,
                "source_name": source_name,
                "value_text": value_text,
                "value_numeric": value_numeric,
            }
        )

    ratio_payload = compute_duplicate_ratio_from_rows(
        row_dicts,
        freshness_days=freshness_days,
    )
    ratio_payload["status"] = "ok"
    return ratio_payload


def _compute_slot_coverage(
    *,
    cases: list[SchoolEvalCase],
    schools_payload: list[dict[str, Any]],
    required_fields_override: list[str] | None,
) -> dict[str, Any]:
    payload_index = _index_school_payload(schools_payload)
    critical_set = {normalise_variable_name(field) for field in PRD_EXPANDED_CRITICAL_FIELDS}

    required_slots = 0
    covered_required_slots = 0
    critical_slots = 0
    covered_critical_slots = 0
    rules_checked = 0
    rules_passed = 0
    by_school: dict[str, Any] = {}

    for case in cases:
        case_required = required_fields_override or case.required_fields
        normalized_required = sorted(
            {normalise_variable_name(field) for field in case_required if field}
        )
        required_slots += len(normalized_required)
        case_critical = {field for field in normalized_required if field in critical_set}
        critical_slots += len(case_critical)

        school_payload = _find_school_payload(case, payload_index)
        school_data = school_payload.get("data", {}) if school_payload else {}
        normalized_data: dict[str, Any] = {
            normalise_variable_name(field): value
            for field, value in school_data.items()
        }
        present_fields = set(normalized_data.keys())

        covered = len(present_fields & set(normalized_required))
        covered_required_slots += covered
        critical_covered = len(present_fields & case_critical)
        covered_critical_slots += critical_covered

        school_rules_checked = 0
        school_rules_passed = 0
        for rule_field, rule in case.rules.items():
            canonical_field = normalise_variable_name(rule_field)
            if canonical_field not in normalized_required:
                continue
            school_rules_checked += 1
            value_node = normalized_data.get(canonical_field)
            value = value_node.get("value") if isinstance(value_node, dict) else value_node
            if _evaluate_rule(value=value, rule=rule, field_name=canonical_field):
                school_rules_passed += 1

        rules_checked += school_rules_checked
        rules_passed += school_rules_passed
        by_school[case.school_name] = {
            "required_slots": len(normalized_required),
            "covered_required_slots": covered,
            "required_slot_recall": round(
                covered / len(normalized_required) if normalized_required else 1.0,
                4,
            ),
            "critical_slots": len(case_critical),
            "covered_critical_slots": critical_covered,
            "critical_slot_recall": round(
                critical_covered / len(case_critical) if case_critical else 1.0,
                4,
            ),
            "rules_checked": school_rules_checked,
            "rules_passed": school_rules_passed,
            "matched_school": school_payload.get("name") if school_payload else None,
        }

    required_slot_recall = covered_required_slots / required_slots if required_slots > 0 else 1.0
    critical_slot_recall = covered_critical_slots / critical_slots if critical_slots > 0 else 1.0
    rule_pass_rate = rules_passed / rules_checked if rules_checked > 0 else 1.0
    return {
        "required_slots": required_slots,
        "covered_required_slots": covered_required_slots,
        "required_slot_recall": round(required_slot_recall, 4),
        "critical_slots": critical_slots,
        "covered_critical_slots": covered_critical_slots,
        "critical_slot_recall": round(critical_slot_recall, 4),
        "rules_checked": rules_checked,
        "rules_passed": rules_passed,
        "rule_pass_rate": round(rule_pass_rate, 4),
        "by_school": by_school,
    }


def _index_school_payload(
    schools_payload: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for school in schools_payload:
        names = []
        raw_name = school.get("name")
        if raw_name:
            names.append(str(raw_name))
        aliases = school.get("aliases", [])
        if isinstance(aliases, list):
            names.extend(str(alias) for alias in aliases if alias)
        for name in names:
            index.setdefault(name.strip().lower(), school)
    return index


def _find_school_payload(
    case: SchoolEvalCase,
    payload_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidates = [case.school_name, *case.aliases]
    for name in candidates:
        hit = payload_index.get(name.strip().lower())
        if hit is not None:
            return hit
    return {}


def _evaluate_rule(
    *,
    value: Any,
    rule: dict[str, Any],
    field_name: str,
) -> bool:
    kind = str(rule.get("kind", "")).strip().lower()
    if kind == "non_empty_text":
        return bool(str(value).strip()) if value is not None else False

    if kind == "enum":
        if value is None:
            return False
        allowed = {
            str(option).strip().lower()
            for option in rule.get("allowed", [])
            if str(option).strip()
        }
        if not allowed:
            return True
        return str(value).strip().lower() in allowed

    if kind == "numeric_range":
        parsed = _coerce_number(value=value, field_name=field_name)
        if parsed is None:
            return False
        min_v = float(rule.get("min", -math.inf))
        max_v = float(rule.get("max", math.inf))
        return min_v <= parsed <= max_v

    # Unknown rule type: keep eval resilient and skip hard-fail.
    return True


def _coerce_number(*, value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    parsed = coerce_numeric(text, variable_name=normalise_variable_name(field_name))
    if parsed is not None:
        return float(parsed)
    try:
        return float(text.replace(",", ""))
    except ValueError:
        return None


def _percentile(values: list[int], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * ratio) - 1))
    return float(ordered[idx])


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown_summary(path: Path, report: DeepSearchLiveEvalReport) -> None:
    p1 = report.pass1
    p2 = report.pass2
    lines = [
        f"# DeepSearch Live Eval {report.run_id}",
        "",
        f"- Status: **{report.status}**",
        f"- Generated At: `{report.generated_at}`",
        f"- Dataset: `{report.config.get('dataset_id')}`",
        f"- Schools: `{len(report.config.get('schools', []))}`",
        "",
        "## Core Metrics",
        f"- Required Slot Recall: `{report.metrics.get('required_slot_recall')}`",
        f"- Critical Slot Recall: `{report.metrics.get('critical_slot_recall')}`",
        f"- DB Hit Uplift: `{report.metrics.get('db_hit_uplift')}`",
        f"- Intra-run Dedupe Drop Ratio: `{report.metrics.get('intra_run_dedupe_drop_ratio')}`",
        f"- DB Duplicate Row Ratio 90d: `{report.metrics.get('db_duplicate_row_ratio_90d')}`",
        f"- Tokens Estimated / Actual: `{report.metrics.get('tokens_estimated')}` / `{report.metrics.get('tokens_actual')}`",
        f"- Tokens Search / Judge: `{report.metrics.get('tokens_actual_search')}` / `{report.metrics.get('tokens_actual_judge')}`",
        f"- Judge Overall Score: `{report.metrics.get('judge_overall_score')}`",
        "",
        "## Pass Summary",
        (
            f"- Pass1 `{p1.execution_mode}`: db_hit={p1.db_hit_ratio}, "
            f"self={p1.self_source_calls}, internal={p1.internal_websearch_calls}, "
            f"persist={p1.persisted_data_points}, judge_score={p1.judge_school_score_avg}"
        ),
    ]
    if p2 is not None:
        lines.append(
            (
                f"- Pass2 `{p2.execution_mode}`: db_hit={p2.db_hit_ratio}, "
                f"self={p2.self_source_calls}, internal={p2.internal_websearch_calls}, "
                f"persist={p2.persisted_data_points}, judge_score={p2.judge_school_score_avg}"
            ),
        )
    lines.extend(
        [
            "",
            "## Recommendations",
            *[f"- {item}" for item in report.recommendations],
            "",
        ],
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _append_history(path: Path, report: DeepSearchLiveEvalReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    header = [
        "run_id",
        "generated_at",
        "status",
        "dataset_id",
        "schools",
        "required_slot_recall",
        "critical_slot_recall",
        "db_hit_uplift",
        "db_duplicate_row_ratio_90d",
        "tokens_estimated",
        "tokens_actual",
        "tokens_actual_search",
        "tokens_actual_judge",
        "judge_overall_score",
        "error_rate",
    ]
    row = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "status": report.status,
        "dataset_id": report.config.get("dataset_id"),
        "schools": len(report.config.get("schools", [])),
        "required_slot_recall": report.metrics.get("required_slot_recall"),
        "critical_slot_recall": report.metrics.get("critical_slot_recall"),
        "db_hit_uplift": report.metrics.get("db_hit_uplift"),
        "db_duplicate_row_ratio_90d": report.metrics.get("db_duplicate_row_ratio_90d"),
        "tokens_estimated": report.metrics.get("tokens_estimated"),
        "tokens_actual": report.metrics.get("tokens_actual"),
        "tokens_actual_search": report.metrics.get("tokens_actual_search"),
        "tokens_actual_judge": report.metrics.get("tokens_actual_judge"),
        "judge_overall_score": report.metrics.get("judge_overall_score"),
        "error_rate": report.metrics.get("error_rate"),
    }

    write_header = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
