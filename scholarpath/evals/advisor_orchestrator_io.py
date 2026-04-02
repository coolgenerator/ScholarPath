"""I/O helpers for advisor orchestrator eval artifacts."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any


def json_default(value: Any) -> Any:
    if isinstance(value, (set, frozenset)):
        try:
            return sorted(value)
        except TypeError:
            return sorted(str(item) for item in value)
    raise TypeError(f"Object of type {value.__class__.__name__} is not JSON serializable")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )


def write_cases_jsonl(path: Path, cases: list[Any]) -> None:
    with path.open("w", encoding="utf-8") as fp:
        for case in cases:
            fp.write(json.dumps(asdict(case), ensure_ascii=False, default=json_default) + "\n")


def write_markdown_summary(path: Path, report: Any) -> None:
    orchestrator = report.orchestrator_metrics
    reedit = report.reedit_metrics
    merged = report.merged_metrics
    lines = [
        "# Advisor Orchestrator Gold Eval v2",
        "",
        f"- Run ID: `{report.run_id}`",
        f"- Status: `{report.status}`",
        f"- Generated At: `{report.generated_at}`",
        f"- Orchestrator Dataset: `{report.config.get('dataset_id')}@{report.config.get('dataset_version')}`",
        f"- Re-edit Dataset: `{report.config.get('reedit_dataset_id')}@{report.config.get('reedit_dataset_version')}`",
        "",
        "## Orchestrator Metrics",
        f"- case_count: `{orchestrator.get('case_count')}`",
        f"- primary_hit_rate: `{orchestrator.get('primary_hit_rate')}`",
        f"- clarify_correct_rate: `{orchestrator.get('clarify_correct_rate')}`",
        f"- execution_limit_violations: `{orchestrator.get('execution_limit_violations')}`",
        f"- contract_valid_rate: `{orchestrator.get('contract_valid_rate')}`",
        f"- task_count_total: `{orchestrator.get('task_count_total')}`",
        f"- task_latency_p90_ms: `{orchestrator.get('task_latency_p90_ms')}`",
        f"- task_latency_p95_ms: `{orchestrator.get('task_latency_p95_ms')}`",
        f"- llm_tokens_per_task: `{orchestrator.get('llm_tokens_per_task')}`",
        f"- llm_latency_avg_ms_per_task: `{orchestrator.get('llm_latency_avg_ms_per_task')}`",
        f"- non_causal_sample_count: `{orchestrator.get('non_causal_sample_count')}`",
        f"- non_causal_p90_ms: `{orchestrator.get('non_causal_p90_ms')}`",
        f"- non_causal_p95_ms: `{orchestrator.get('non_causal_p95_ms')}`",
        f"- non_causal_task_p90_ms: `{orchestrator.get('non_causal_task_p90_ms')}`",
        f"- non_causal_task_p95_ms: `{orchestrator.get('non_causal_task_p95_ms')}`",
        "",
        "## DeepSearch Reuse Metrics",
        f"- deepsearch_expect_total: `{orchestrator.get('deepsearch_expect_total')}`",
        f"- deepsearch_expectation_rate: `{orchestrator.get('deepsearch_expectation_rate')}`",
        f"- deepsearch_trigger_rate: `{orchestrator.get('deepsearch_trigger_rate')}`",
        f"- deepsearch_db_hit_ratio_avg: `{orchestrator.get('deepsearch_db_hit_ratio_avg')}`",
        f"- deepsearch_external_calls_avg: `{orchestrator.get('deepsearch_external_calls_avg')}`",
        f"- deepsearch_pair_total: `{orchestrator.get('deepsearch_pair_total')}`",
        f"- deepsearch_pair_uplift_pass_rate: `{orchestrator.get('deepsearch_pair_uplift_pass_rate')}`",
        f"- deepsearch_pair_external_reduction_rate: `{orchestrator.get('deepsearch_pair_external_reduction_rate')}`",
        f"- deepsearch_db_hit_uplift_avg: `{orchestrator.get('deepsearch_db_hit_uplift_avg')}`",
        "",
        "## Re-edit Metrics",
        f"- case_count: `{reedit.get('case_count')}`",
        f"- reedit_overwrite_success_rate: `{reedit.get('reedit_overwrite_success_rate')}`",
        f"- reedit_truncation_correct_rate: `{reedit.get('reedit_truncation_correct_rate')}`",
        f"- reedit_history_consistency_rate: `{reedit.get('reedit_history_consistency_rate')}`",
        "",
        "## Judge Metrics",
        f"- judge_enabled: `{merged.get('judge_enabled')}`",
        f"- judge_overall_score: `{merged.get('judge_overall_score')}`",
        f"- judge_status: `{merged.get('judge_status')}`",
        f"- tokens_actual_judge: `{merged.get('tokens_actual_judge')}`",
        f"- judge_tokens_per_task: `{merged.get('judge_tokens_per_task')}`",
        f"- judge_latency_avg_ms_per_task: `{merged.get('judge_latency_avg_ms_per_task')}`",
        f"- judge_rate_limit_errors: `{merged.get('judge_rate_limit_errors')}`",
        "",
        "## Complex Output Metrics",
        f"- complex_output_polish_calls: `{merged.get('complex_output_polish_calls')}`",
        f"- complex_output_polish_errors: `{merged.get('complex_output_polish_errors')}`",
        f"- complex_output_render_pass_rate: `{merged.get('complex_output_render_pass_rate')}`",
        "",
        "## Scores",
        f"- deterministic_overall_score: `{merged.get('deterministic_overall_score')}`",
        f"- merged_case_score: `{merged.get('merged_case_score')}`",
        "",
        "## Recommendations",
    ]
    lines.extend(f"- {item}" for item in report.recommendations)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def append_history(path: Path, report: Any) -> None:
    merged = report.merged_metrics
    headers = [
        "run_id",
        "generated_at",
        "status",
        "dataset_id",
        "dataset_version",
        "reedit_dataset_id",
        "reedit_dataset_version",
        "orchestrator_case_count",
        "reedit_case_count",
        "primary_hit_rate",
        "clarify_correct_rate",
        "execution_limit_violations",
        "contract_valid_rate",
        "task_count_total",
        "task_latency_p90_ms",
        "task_latency_p95_ms",
        "llm_tokens_per_task",
        "llm_latency_avg_ms_per_task",
        "non_causal_p90_ms",
        "non_causal_p95_ms",
        "non_causal_task_p90_ms",
        "non_causal_task_p95_ms",
        "reedit_overwrite_success_rate",
        "reedit_truncation_correct_rate",
        "reedit_history_consistency_rate",
        "judge_overall_score",
        "complex_output_polish_calls",
        "complex_output_polish_errors",
        "complex_output_render_pass_rate",
        "deterministic_overall_score",
        "merged_case_score",
    ]
    row = {
        "run_id": report.run_id,
        "generated_at": report.generated_at,
        "status": report.status,
        "dataset_id": report.config.get("dataset_id"),
        "dataset_version": report.config.get("dataset_version"),
        "reedit_dataset_id": report.config.get("reedit_dataset_id"),
        "reedit_dataset_version": report.config.get("reedit_dataset_version"),
        "orchestrator_case_count": merged.get("orchestrator_case_count"),
        "reedit_case_count": merged.get("reedit_case_count"),
        "primary_hit_rate": merged.get("primary_hit_rate"),
        "clarify_correct_rate": merged.get("clarify_correct_rate"),
        "execution_limit_violations": merged.get("execution_limit_violations"),
        "contract_valid_rate": merged.get("contract_valid_rate"),
        "task_count_total": merged.get("task_count_total"),
        "task_latency_p90_ms": merged.get("task_latency_p90_ms"),
        "task_latency_p95_ms": merged.get("task_latency_p95_ms"),
        "llm_tokens_per_task": merged.get("llm_tokens_per_task"),
        "llm_latency_avg_ms_per_task": merged.get("llm_latency_avg_ms_per_task"),
        "non_causal_p90_ms": merged.get("non_causal_p90_ms"),
        "non_causal_p95_ms": merged.get("non_causal_p95_ms"),
        "non_causal_task_p90_ms": merged.get("non_causal_task_p90_ms"),
        "non_causal_task_p95_ms": merged.get("non_causal_task_p95_ms"),
        "reedit_overwrite_success_rate": merged.get("reedit_overwrite_success_rate"),
        "reedit_truncation_correct_rate": merged.get("reedit_truncation_correct_rate"),
        "reedit_history_consistency_rate": merged.get("reedit_history_consistency_rate"),
        "judge_overall_score": merged.get("judge_overall_score"),
        "complex_output_polish_calls": merged.get("complex_output_polish_calls"),
        "complex_output_polish_errors": merged.get("complex_output_polish_errors"),
        "complex_output_render_pass_rate": merged.get("complex_output_render_pass_rate"),
        "deterministic_overall_score": merged.get("deterministic_overall_score"),
        "merged_case_score": merged.get("merged_case_score"),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=headers)
        if not exists:
            writer.writeheader()
        writer.writerow(row)

