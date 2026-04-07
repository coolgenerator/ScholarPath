"""Advisor UX gold-set live evaluation runner (candidate replay + A/B judge)."""

from __future__ import annotations

import asyncio
import json
import math
import re
import statistics
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select

from scholarpath.api.models.chat import ChatBlock, TurnEvent, TurnResult
from scholarpath.chat.agent import ChatAgent
from scholarpath.db.models import Student, TokenUsage
from scholarpath.db.redis import redis_pool
from scholarpath.db.session import async_session_factory
from scholarpath.evals.advisor_orchestrator_io import (
    append_history,
    serialize_json,
    serialize_jsonl,
    write_summary,
)
from scholarpath.evals.advisor_ux_judge import (
    AdvisorUXABJudge,
    AdvisorUXJudgeCaseResult,
    AdvisorUXJudgeRunSummary,
    RUBRIC_DIMENSIONS,
    create_unscored_case_result,
)
from scholarpath.llm.client import LLMClient, get_llm_client

DEFAULT_DATASET_DIR = Path(__file__).resolve().parent / "datasets"
DEFAULT_MINI_DATASET_PATH = DEFAULT_DATASET_DIR / "advisor_ux_gold_mini_v1.json"
DEFAULT_FULL_DATASET_PATH = DEFAULT_DATASET_DIR / "advisor_ux_gold_full_v1.json"
DEFAULT_LOW_SCORE_SMOKE_DATASET_PATH = DEFAULT_DATASET_DIR / "advisor_ux_low_score_smoke_v1.json"
DEFAULT_OUTPUT_DIR = Path(".benchmarks/advisor_orchestrator_ux")
DEFAULT_EXECUTION_CONCURRENCY = 3
DEFAULT_UNSCORED_BUCKETS: tuple[str, ...] = (
    "recommendation",
    "strategy",
    "school_query",
)
FORCED_UNSCORED_BUCKETS: tuple[str, ...] = ("multi_intent",)

_DEGRADATION_INTRUSION_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:CAP_TIMEOUT|CAP_FAILED|CAP_SCHEMA_INVALID|CAP_DEGRADED|STEP_BUDGET_EXCEEDED|LOCK_REJECTED|PROFILE_GATE_BLOCKED)\b", re.IGNORECASE),
    re.compile(r"(降级原因|原因码|reason\s*code)", re.IGNORECASE),
)


@dataclass(slots=True)
class AdvisorUXGoldCase:
    case_id: str
    bucket: str
    tags: list[str]
    student_seed: dict[str, Any] | None
    turns: list[dict[str, Any]]
    hard_checks: list[dict[str, Any]]
    soft_checks: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AdvisorUXDataset:
    dataset_id: str
    version: str
    rubric_dimensions: list[str]
    cases: list[AdvisorUXGoldCase]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AdvisorUXCaseExecution:
    case_id: str
    bucket: str
    tags: list[str]
    status: str
    turns_executed: int
    duration_ms: float
    final_content: str
    final_blocks: list[dict[str, Any]]
    final_usage: dict[str, Any]
    trace_summary: dict[str, Any]
    hard_check_passed: bool
    hard_check_results: list[dict[str, Any]]
    soft_check_mean: float
    soft_check_results: list[dict[str, Any]]
    judge_payload: dict[str, Any]
    error: str | None = None
    degraded_caps: list[str] = field(default_factory=list)
    synthesis_present: bool = False
    primary_angle_covered: bool = False
    fallback_used: bool = False
    skill_id: str = "default"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AdvisorUXEvalReport:
    run_id: str
    generated_at: str
    status: str
    config: dict[str, Any]
    metrics: dict[str, Any]
    judge_summary: dict[str, Any] = field(default_factory=dict)
    mismatches: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AdvisorUXSkillMetrics:
    skill_id: str
    case_count: int
    scored_case_count: int
    candidate_win_rate: float
    mean_score: float
    low_score_rate: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


async def run_advisor_ux_gold_eval(
    *,
    dataset: str = "mini",
    baseline_run_id: str | None = None,
    judge_enabled: bool = True,
    judge_concurrency: int = 2,
    judge_temperature: float = 0.1,
    judge_max_tokens: int = 1200,
    candidate_run_id: str | None = None,
    case_ids: list[str] | None = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    execution_concurrency: int = DEFAULT_EXECUTION_CONCURRENCY,
    unscored_buckets: list[str] | None = None,
) -> AdvisorUXEvalReport:
    if judge_enabled and not baseline_run_id:
        raise ValueError("baseline_run_id is required when judge_enabled=true")

    dataset_obj = load_advisor_ux_dataset(dataset)
    effective_unscored_buckets = _resolve_unscored_buckets(unscored_buckets)
    selected_cases = select_advisor_ux_cases(dataset_obj.cases, case_ids=case_ids)
    run_id = candidate_run_id or f"advisor-ux-{datetime.now(UTC):%Y%m%d-%H%M%S}"
    output_root = Path(output_dir)
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    llm = get_llm_client()
    candidate_suffix = llm.set_caller_suffix(f"{run_id}.candidate")
    try:
        candidate_cases = await _execute_cases(
            llm=llm,
            run_id=run_id,
            cases=selected_cases,
            concurrency=execution_concurrency,
        )
    finally:
        llm.reset_caller_suffix(candidate_suffix)

    baseline_map = _load_baseline_case_map(
        output_root=output_root,
        baseline_run_id=baseline_run_id,
    )
    aligned_pairs, mismatches = _align_ab_cases(
        candidate_cases=candidate_cases,
        baseline_map=baseline_map,
    )

    scored_pairs: list[dict[str, Any]] = []
    unscored_case_results: list[AdvisorUXJudgeCaseResult] = []
    for pair in aligned_pairs:
        case_id = str(pair.get("case_id") or "")
        bucket = str(pair.get("candidate_bucket") or "").strip().lower()
        if not judge_enabled:
            unscored_case_results.append(
                create_unscored_case_result(
                    case_id=case_id,
                    reason="judge_disabled",
                    notes="judge globally disabled by cli flag",
                )
            )
            continue
        if bucket in effective_unscored_buckets:
            unscored_case_results.append(
                create_unscored_case_result(
                    case_id=case_id,
                    reason="unscored_bucket",
                    notes=f"bucket={bucket}",
                )
            )
            continue
        scored_pairs.append(pair)

    scored_judge_case_results: list[AdvisorUXJudgeCaseResult] = []
    judge_run_summary: AdvisorUXJudgeRunSummary | None = None
    if judge_enabled and scored_pairs:
        judge_suffix = llm.set_caller_suffix(f"{run_id}.judge")
        try:
            judge = AdvisorUXABJudge(
                llm=llm,
                temperature=judge_temperature,
                case_max_tokens=judge_max_tokens,
                run_max_tokens=max(600, judge_max_tokens // 2),
            )
            scored_judge_case_results = await _judge_aligned_pairs(
                judge=judge,
                run_id=run_id,
                aligned_pairs=scored_pairs,
                concurrency=judge_concurrency,
            )
            judge_run_summary = await judge.judge_run(
                run_id=run_id,
                case_results=[*scored_judge_case_results, *unscored_case_results],
                metrics={
                    "dataset_id": dataset_obj.dataset_id,
                    "aligned_case_count": len(aligned_pairs),
                    "mismatch_count": len(mismatches),
                    "scored_case_count": len(scored_pairs),
                    "unscored_case_count": len(unscored_case_results),
                },
            )
        finally:
            llm.reset_caller_suffix(judge_suffix)

    judge_case_results = _merge_case_results_in_pair_order(
        aligned_pairs=aligned_pairs,
        scored_results=scored_judge_case_results,
        unscored_results=unscored_case_results,
    )
    scored_case_results = [
        item for item in judge_case_results
        if item.scoring_status == "scored"
    ]
    unscored_case_count = len(judge_case_results) - len(scored_case_results)

    candidate_usage = await _collect_token_usage(
        suffix=f"{run_id}.candidate",
        caller_prefixes=("chat.", "advisor.", "search.", "eval."),
    )
    judge_usage = await _collect_token_usage(
        suffix=f"{run_id}.judge",
        caller_prefixes=("eval.advisor.ux.judge.",),
    )

    mean_delta_by_dim = (
        judge_run_summary.mean_delta_by_dim
        if judge_run_summary is not None
        else _compute_mean_delta_by_dim(scored_case_results)
    )
    overall_user_feel_mean = (
        float(judge_run_summary.overall_user_feel_mean)
        if judge_run_summary is not None
        else _mean(
            item.candidate_mean
            for item in scored_case_results
            if item.candidate_mean is not None
        )
    )
    candidate_win_rate = (
        float(judge_run_summary.candidate_win_rate)
        if judge_run_summary is not None
        else (
            sum(1 for item in scored_case_results if item.winner == "candidate")
            / max(1, len(scored_case_results))
        )
    )
    scoring_coverage_rate = (
        len(scored_case_results) / max(1, len(aligned_pairs))
    )
    generic_refusal_rate = (
        sum(1 for item in candidate_cases if _is_generic_refusal_text(item.final_content))
        / max(1, len(candidate_cases))
    )
    transparency_low_score_rate = (
        sum(
            1
            for item in scored_case_results
            if float(item.candidate_scores.get("execution_chain_transparency", 0.0)) <= 3.0
        )
        / max(1, len(scored_case_results))
        if scored_case_results
        else 0.0
    )
    degradation_intrusion_rate = _compute_degradation_intrusion_rate(candidate_cases)
    transparency_signal_density = _compute_transparency_signal_density(candidate_cases)
    by_skill_metrics = _build_by_skill_metrics(
        candidate_cases=candidate_cases,
        judge_case_results=judge_case_results,
    )

    metrics = {
        "scoring": {
            "scored_case_count": len(scored_case_results),
            "unscored_case_count": unscored_case_count,
            "scoring_coverage_rate": round(scoring_coverage_rate, 4),
        },
        "scored_judge": {
            "candidate_win_rate": round(candidate_win_rate, 4),
            "overall_user_feel_mean": round(overall_user_feel_mean, 4),
            "mean_delta_by_dim": mean_delta_by_dim,
            "by_skill": [item.to_dict() for item in by_skill_metrics],
        },
        "execution": {
            "candidate_case_count": len(candidate_cases),
            "aligned_case_count": len(aligned_pairs),
            "mismatch_count": len(mismatches),
        },
        "experience_watch": {
            "generic_refusal_rate": round(generic_refusal_rate, 4),
            "transparency_low_score_rate": round(transparency_low_score_rate, 4),
            "degradation_intrusion_rate": round(degradation_intrusion_rate, 4),
            "transparency_signal_density": round(transparency_signal_density, 4),
        },
        "token_usage_by_stage": {
            "candidate": candidate_usage,
            "judge": judge_usage,
            "total_tokens": int(candidate_usage.get("tokens", 0))
            + int(judge_usage.get("tokens", 0)),
        },
        "latency_ms_by_stage": {
            "candidate": {
                "median": candidate_usage.get("median_latency_ms", 0.0),
                "p90": candidate_usage.get("p90_latency_ms", 0.0),
                "p95": candidate_usage.get("p95_latency_ms", 0.0),
            },
            "judge": {
                "median": judge_usage.get("median_latency_ms", 0.0),
                "p90": judge_usage.get("p90_latency_ms", 0.0),
                "p95": judge_usage.get("p95_latency_ms", 0.0),
            },
        },
    }
    low_score_case_study = _build_low_score_case_study(
        candidate_cases=candidate_cases,
        judge_case_results=judge_case_results,
    )
    metrics["low_score_case_study"] = {
        "low_score_case_count": low_score_case_study["low_score_case_count"],
        "by_bucket": low_score_case_study["by_bucket"],
        "by_skill": low_score_case_study["by_skill"],
        "by_reason_code": low_score_case_study["by_reason_code"],
        "by_fallback_used": low_score_case_study["by_fallback_used"],
    }

    status = "ok"
    if judge_enabled and scored_pairs and not scored_case_results:
        status = "watch"
    if judge_enabled and judge_run_summary is not None and judge_run_summary.status != "ok":
        status = "watch"
    if any(item.status != "ok" for item in candidate_cases):
        status = "watch"

    report = AdvisorUXEvalReport(
        run_id=run_id,
        generated_at=datetime.now(UTC).isoformat(),
        status=status,
        config={
            "dataset": dataset,
            "dataset_id": dataset_obj.dataset_id,
            "dataset_version": dataset_obj.version,
            "sample_size": len(selected_cases),
            "case_ids": [case.case_id for case in selected_cases],
            "baseline_run_id": baseline_run_id,
            "judge_enabled": judge_enabled,
            "judge_concurrency": judge_concurrency,
            "judge_temperature": judge_temperature,
            "judge_max_tokens": judge_max_tokens,
            "execution_concurrency": execution_concurrency,
            "unscored_buckets": sorted(effective_unscored_buckets),
        },
        metrics=metrics,
        judge_summary=judge_run_summary.to_dict() if judge_run_summary else {},
        mismatches=mismatches,
        errors=[],
    )

    serialize_json(run_dir / "report.json", report.to_dict())
    serialize_jsonl(run_dir / "case_results.jsonl", [item.to_dict() for item in candidate_cases])
    serialize_jsonl(
        run_dir / "judge_case_results.jsonl",
        [item.to_dict() for item in judge_case_results],
    )
    serialize_json(run_dir / "low_score_case_study.json", low_score_case_study)
    _write_summary(
        run_dir=run_dir,
        report=report,
    )
    append_history(
        output_root / "history.csv",
        {
            "run_id": report.run_id,
            "generated_at": report.generated_at,
            "status": report.status,
            "dataset_id": dataset_obj.dataset_id,
            "sample_size": len(selected_cases),
            "scored_case_count": (report.metrics.get("scoring") or {}).get("scored_case_count"),
            "unscored_case_count": (report.metrics.get("scoring") or {}).get("unscored_case_count"),
            "scoring_coverage_rate": (report.metrics.get("scoring") or {}).get("scoring_coverage_rate"),
            "candidate_win_rate": (report.metrics.get("scored_judge") or {}).get("candidate_win_rate"),
            "overall_user_feel_mean": (report.metrics.get("scored_judge") or {}).get("overall_user_feel_mean"),
            "generic_refusal_rate": (report.metrics.get("experience_watch") or {}).get("generic_refusal_rate"),
            "transparency_low_score_rate": (report.metrics.get("experience_watch") or {}).get("transparency_low_score_rate"),
            "degradation_intrusion_rate": (report.metrics.get("experience_watch") or {}).get("degradation_intrusion_rate"),
            "transparency_signal_density": (report.metrics.get("experience_watch") or {}).get("transparency_signal_density"),
            "candidate_tokens": candidate_usage.get("tokens"),
            "judge_tokens": judge_usage.get("tokens"),
            "candidate_latency_median_ms": candidate_usage.get("median_latency_ms"),
            "candidate_latency_p90_ms": candidate_usage.get("p90_latency_ms"),
            "candidate_latency_p95_ms": candidate_usage.get("p95_latency_ms"),
        },
    )
    return report


def load_advisor_ux_dataset(dataset: str | Path) -> AdvisorUXDataset:
    path = _resolve_dataset_path(dataset)
    payload = json.loads(path.read_text(encoding="utf-8"))
    dataset_id = str(payload.get("dataset_id") or path.stem)
    version = str(payload.get("version") or "1")
    rubric_dimensions_raw = payload.get("rubric_dimensions")
    rubric_dimensions = (
        [str(item) for item in rubric_dimensions_raw]
        if isinstance(rubric_dimensions_raw, list)
        else list(RUBRIC_DIMENSIONS)
    )
    cases_payload = payload.get("cases")
    if isinstance(cases_payload, list):
        cases = [_parse_case(item) for item in cases_payload]
    else:
        generator = payload.get("generator")
        if not isinstance(generator, dict):
            raise ValueError(f"Dataset {path} must contain `cases` or `generator`")
        case_count = int(generator.get("case_count") or 0)
        if case_count <= 0:
            raise ValueError(f"Dataset {path} generator.case_count must be positive")
        seed = str(generator.get("seed") or "advisor-ux-v1")
        cases = _generate_cases(case_count=case_count, seed=seed)
    _assert_unique_case_ids(cases, dataset_id=dataset_id)
    return AdvisorUXDataset(
        dataset_id=dataset_id,
        version=version,
        rubric_dimensions=rubric_dimensions,
        cases=cases,
    )


def select_advisor_ux_cases(
    cases: list[AdvisorUXGoldCase],
    *,
    case_ids: list[str] | None,
) -> list[AdvisorUXGoldCase]:
    if not case_ids:
        return list(cases)
    mapping = {case.case_id: case for case in cases}
    wanted = [str(item).strip() for item in case_ids if str(item).strip()]
    missing = [case_id for case_id in wanted if case_id not in mapping]
    if missing:
        raise ValueError(f"Unknown advisor ux case ids: {missing}")
    return [mapping[item] for item in wanted]


def _resolve_dataset_path(dataset: str | Path) -> Path:
    if isinstance(dataset, Path):
        return dataset
    raw = str(dataset).strip().lower()
    if raw == "mini":
        return DEFAULT_MINI_DATASET_PATH
    if raw == "full":
        return DEFAULT_FULL_DATASET_PATH
    if raw in {"low_score_smoke", "low-score-smoke", "low_score"}:
        return DEFAULT_LOW_SCORE_SMOKE_DATASET_PATH
    return Path(dataset)


def _parse_case(payload: dict[str, Any]) -> AdvisorUXGoldCase:
    if not isinstance(payload, dict):
        raise ValueError("Each case must be a JSON object")
    case_id = str(payload.get("case_id") or "").strip()
    if not case_id:
        raise ValueError("Case missing case_id")
    turns = payload.get("turns")
    if not isinstance(turns, list) or not turns:
        raise ValueError(f"Case {case_id} must include non-empty turns")
    hard_checks = payload.get("hard_checks")
    soft_checks = payload.get("soft_checks")
    if not isinstance(hard_checks, list):
        hard_checks = []
    if not isinstance(soft_checks, list):
        soft_checks = []
    tags = payload.get("tags")
    if not isinstance(tags, list):
        tags = []
    return AdvisorUXGoldCase(
        case_id=case_id,
        bucket=str(payload.get("bucket") or "general"),
        tags=[str(item) for item in tags if str(item).strip()],
        student_seed=payload.get("student_seed") if isinstance(payload.get("student_seed"), dict) else None,
        turns=[_parse_turn(item) for item in turns],
        hard_checks=[item for item in hard_checks if isinstance(item, dict)],
        soft_checks=[item for item in soft_checks if isinstance(item, dict)],
    )


def _parse_turn(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"content": ""}
    return {"content": str(payload.get("content") or "").strip()}


def _assert_unique_case_ids(cases: list[AdvisorUXGoldCase], *, dataset_id: str) -> None:
    ids = [case.case_id for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Dataset {dataset_id} has duplicate case_id values")


def _generate_cases(*, case_count: int, seed: str) -> list[AdvisorUXGoldCase]:
    # Keep deterministic generation so full[:30] is exactly mini.
    templates: list[dict[str, Any]] = [
        {
            "bucket": "recommendation",
            "tags": ["multi_turn", "recommend"],
            "first": "我目前GPA{gpa}，SAT {sat}，预算{budget}美元，想申美国CS，请先给我选校建议。",
            "second": "再按冲刺/匹配/保底拆开，并给我下一步行动清单。",
            "keywords": ["建议", "行动", "冲刺", "匹配", "保底"],
        },
        {
            "bucket": "strategy",
            "tags": ["multi_turn", "strategy"],
            "first": "我在纠结ED/EA/RD怎么排，背景是GPA{gpa} SAT {sat}，帮我做申请策略。",
            "second": "请把时间线按月份列出来，并说清风险点。",
            "keywords": ["ED", "EA", "RD", "时间", "风险"],
        },
        {
            "bucket": "offer_compare",
            "tags": ["multi_turn", "compare"],
            "first": "我拿到了两所学校的offer，帮我比较学术、就业、费用和风险。",
            "second": "请给一个推荐结论，并说明你最关键的三条判断依据。",
            "keywords": ["比较", "结论", "依据"],
        },
        {
            "bucket": "what_if",
            "tags": ["multi_turn", "what_if"],
            "first": "如果我把SAT从{sat}提到{sat_high}，录取概率和策略会怎么变化？",
            "second": "请给最值得做的两个投入动作。",
            "keywords": ["变化", "概率", "动作"],
        },
        {
            "bucket": "school_query",
            "tags": ["multi_turn", "query"],
            "first": "帮我查一下CMU和UIUC的CS项目差异，重点看课程和就业。",
            "second": "再结合我的预算{budget}美元，给我建议。",
            "keywords": ["课程", "就业", "预算"],
        },
        {
            "bucket": "profile_update",
            "tags": ["multi_turn", "profile"],
            "first": "把我的档案更新：GPA改成{gpa}，预算改成{budget}，目标专业增加Data Science。",
            "second": "先给我更新提案摘要，不要直接提交。",
            "keywords": ["档案", "更新", "提案"],
        },
        {
            "bucket": "guided_intake",
            "tags": ["multi_turn", "intake"],
            "first": "我还没想好国家和专业方向，你先问我关键问题来完善档案。",
            "second": "继续下一组问题，尽量具体。",
            "keywords": ["问题", "完善", "档案"],
        },
        {
            "bucket": "multi_intent",
            "tags": ["multi_turn", "parallel"],
            "first": "同一轮里我需要：先选校建议，再给申请时间线，最后比较两所冲刺校风险。",
            "second": "请按优先级给我分步骤执行，并总结本轮结果。",
            "keywords": ["优先级", "步骤", "总结"],
        },
        {
            "bucket": "memory_followup",
            "tags": ["multi_turn", "memory"],
            "first": "记住我预算上限是{budget}美元、偏好大城市、CS+AI方向。",
            "second": "基于我刚才的偏好，直接给我下一步推荐。",
            "keywords": ["记住", "偏好", "推荐"],
        },
        {
            "bucket": "robustness",
            "tags": ["multi_turn", "robustness"],
            "first": "我的输入可能不完整，你先指出缺失信息，再给一个可执行的临时方案。",
            "second": "如果我今天只能做一件事，应该先做什么？",
            "keywords": ["缺失", "临时方案", "先做"],
        },
    ]
    out: list[AdvisorUXGoldCase] = []
    for idx in range(case_count):
        template = templates[idx % len(templates)]
        serial = idx + 1
        gpa = round(3.4 + (idx % 6) * 0.1, 2)
        sat = 1320 + (idx % 9) * 20
        sat_high = sat + 80
        budget = 45000 + (idx % 8) * 5000
        student_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}:{serial}:student"))
        case_id = f"ux_{serial:03d}"
        hard_checks = [
            {"id": "status_ok", "kind": "status_equals", "value": "ok"},
            {
                "id": "usage_core",
                "kind": "usage_fields_exist",
                "fields": ["wave_count", "tool_steps_used"],
            },
            {
                "id": "trace_core",
                "kind": "trace_events_exist",
                "events": ["turn_started", "planning_done", "turn_completed"],
            },
            {"id": "content_len", "kind": "content_min_chars", "min_chars": 20},
        ]
        soft_checks = [
            {
                "id": "keyword_hit",
                "kind": "content_keywords_any",
                "keywords": template["keywords"],
            },
            {"id": "block_presence", "kind": "block_count_min", "min_count": 1},
        ]
        out.append(
            AdvisorUXGoldCase(
                case_id=case_id,
                bucket=template["bucket"],
                tags=list(template["tags"]),
                student_seed={
                    "id": student_uuid,
                    "name": f"Eval Student {serial}",
                    "gpa": gpa,
                    "gpa_scale": "4.0",
                    "sat_total": sat,
                    "curriculum_type": "AP",
                    "intended_majors": ["Computer Science", "Data Science"],
                    "budget_usd": budget,
                    "need_financial_aid": budget < 65000,
                    "target_year": 2027 + (idx % 3),
                    "preferences": {"regions": ["US"], "city_tier": "metro"},
                },
                turns=[
                    {
                        "content": template["first"].format(
                            gpa=gpa,
                            sat=sat,
                            sat_high=sat_high,
                            budget=budget,
                        )
                    },
                    {"content": template["second"]},
                ],
                hard_checks=hard_checks,
                soft_checks=soft_checks,
            )
        )
    return out


async def _execute_cases(
    *,
    llm: LLMClient,
    run_id: str,
    cases: list[AdvisorUXGoldCase],
    concurrency: int,
) -> list[AdvisorUXCaseExecution]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _one(case: AdvisorUXGoldCase) -> AdvisorUXCaseExecution:
        async with semaphore:
            return await _execute_single_case(llm=llm, run_id=run_id, case=case)

    return await asyncio.gather(*[_one(case) for case in cases])


async def _execute_single_case(
    *,
    llm: LLMClient,
    run_id: str,
    case: AdvisorUXGoldCase,
) -> AdvisorUXCaseExecution:
    t0 = time.perf_counter()
    events: list[TurnEvent] = []
    turn_result: TurnResult | None = None
    turns_executed = 0
    error_text: str | None = None
    session_id = f"advisor-ux-{run_id}-{case.case_id}"

    try:
        student_id = None
        if isinstance(case.student_seed, dict):
            async with async_session_factory() as seed_session:
                student_id = await _ensure_student_seed(
                    session=seed_session,
                    student_seed=case.student_seed,
                )
                await seed_session.commit()

        async def _emit(event: TurnEvent) -> None:
            events.append(event)

        # Match websocket route semantics: each turn owns its own DB session.
        for turn in case.turns:
            turns_executed += 1
            async with async_session_factory() as session:
                agent = ChatAgent(llm=llm, session=session, redis=redis_pool)
                turn_result = await agent.run_turn(
                    session_id=session_id,
                    student_id=student_id,
                    message=str(turn.get("content") or ""),
                    emit_event=_emit,
                )
                if turn_result.status == "ok":
                    await session.commit()
                else:
                    await session.rollback()
    except Exception as exc:  # pragma: no cover - runtime dependent
        error_text = str(exc)
        turn_result = TurnResult(
            trace_id=str(uuid.uuid4()),
            status="error",
            content=f"case_runtime_error: {exc}",
            blocks=[
                ChatBlock(
                    id=f"{case.case_id}-error",
                    kind="error",
                    capability_id="eval_runtime",
                    order=0,
                    payload={"error": str(exc)},
                    meta={},
                )
            ],
            actions=[],
            usage={},
        )

    result = turn_result or TurnResult(
        trace_id=str(uuid.uuid4()),
        status="error",
        content="case_runtime_error: empty_result",
        blocks=[],
        actions=[],
        usage={},
    )
    hard_results = _evaluate_checks(
        checks=case.hard_checks,
        result=result,
        events=events,
        hard_mode=True,
    )
    soft_results = _evaluate_checks(
        checks=case.soft_checks,
        result=result,
        events=events,
        hard_mode=False,
    )
    hard_passed = all(bool(item.get("passed")) for item in hard_results) if hard_results else True
    soft_mean = (
        statistics.fmean(float(item.get("score", 0.0)) for item in soft_results)
        if soft_results
        else 1.0
    )
    trace_summary = _build_trace_summary(events)
    synthesis_payload = _extract_answer_synthesis_payload(result.blocks)
    synthesis_skill = _extract_answer_synthesis_skill(result.blocks)
    usage_skill = str((result.usage or {}).get("active_skill_id") or "").strip()
    skill_id = usage_skill or synthesis_skill or "default"
    synthesis_present = synthesis_payload is not None
    degraded_section = synthesis_payload.get("degraded", {}) if isinstance(synthesis_payload, dict) else {}
    degraded_caps = [
        str(item).strip()
        for item in degraded_section.get("caps", [])
        if str(item).strip()
    ] if isinstance(degraded_section.get("caps"), list) else []
    perspectives = synthesis_payload.get("perspectives", []) if isinstance(synthesis_payload, dict) else []
    primary_angle_covered = bool(isinstance(perspectives, list) and len(perspectives) > 0)
    fallback_used = bool(degraded_section.get("has_degraded")) if isinstance(degraded_section, dict) else False
    judge_content = _build_judge_content(
        synthesis_payload=synthesis_payload,
        fallback_content=result.content,
    )
    final_blocks = [
        {
            "kind": block.kind,
            "capability_id": block.capability_id,
            "order": block.order,
            "meta": block.meta or {},
            "payload": block.payload if block.kind == "answer_synthesis" else {},
        }
        for block in result.blocks
    ]
    duration_ms = round((time.perf_counter() - t0) * 1000.0, 2)

    return AdvisorUXCaseExecution(
        case_id=case.case_id,
        bucket=case.bucket,
        tags=list(case.tags),
        status=result.status,
        turns_executed=turns_executed,
        duration_ms=duration_ms,
        final_content=result.content,
        final_blocks=final_blocks,
        final_usage=dict(result.usage or {}),
        trace_summary=trace_summary,
        hard_check_passed=hard_passed,
        hard_check_results=hard_results,
        soft_check_mean=round(float(soft_mean), 4),
        soft_check_results=soft_results,
        judge_payload={
            "status": result.status,
            "content": judge_content,
            "block_kinds": [item["kind"] for item in final_blocks],
            "usage": dict(result.usage or {}),
            "execution_digest": (
                dict(result.execution_digest)
                if isinstance(result.execution_digest, dict)
                else {}
            ),
            "trace_summary": trace_summary,
            "hard_check_passed": hard_passed,
            "soft_check_mean": round(float(soft_mean), 4),
            "degraded_caps": degraded_caps,
            "synthesis_present": synthesis_present,
            "primary_angle_covered": primary_angle_covered,
            "fallback_used": fallback_used,
            "skill_id": skill_id,
        },
        error=error_text,
        degraded_caps=degraded_caps,
        synthesis_present=synthesis_present,
        primary_angle_covered=primary_angle_covered,
        fallback_used=fallback_used,
        skill_id=skill_id,
    )


async def _ensure_student_seed(
    *,
    session,
    student_seed: dict[str, Any] | None,
) -> uuid.UUID | None:
    if not isinstance(student_seed, dict):
        return None
    raw_id = str(student_seed.get("id") or "").strip()
    if not raw_id:
        return None
    student_id = uuid.UUID(raw_id)
    existing = await session.get(Student, student_id)
    payload = {
        "id": student_id,
        "name": str(student_seed.get("name") or f"Eval Student {student_id.hex[:6]}"),
        "gpa": float(student_seed.get("gpa") or 3.6),
        "gpa_scale": str(student_seed.get("gpa_scale") or "4.0"),
        "sat_total": int(student_seed.get("sat_total") or 1380),
        "curriculum_type": str(student_seed.get("curriculum_type") or "AP"),
        "intended_majors": list(student_seed.get("intended_majors") or ["Computer Science"]),
        "budget_usd": int(student_seed.get("budget_usd") or 60000),
        "need_financial_aid": bool(student_seed.get("need_financial_aid") or False),
        "target_year": int(student_seed.get("target_year") or 2028),
        "preferences": dict(student_seed.get("preferences") or {"regions": ["US"]}),
    }
    if existing is None:
        session.add(Student(**payload))
    else:
        for key, value in payload.items():
            if key == "id":
                continue
            setattr(existing, key, value)
    await session.flush()
    return student_id


def _extract_answer_synthesis_payload(blocks: list[ChatBlock]) -> dict[str, Any] | None:
    for block in blocks:
        if block.kind != "answer_synthesis":
            continue
        payload = block.payload
        if isinstance(payload, dict):
            return dict(payload)
    return None


def _extract_answer_synthesis_skill(blocks: list[ChatBlock]) -> str:
    for block in blocks:
        if block.kind != "answer_synthesis":
            continue
        meta = block.meta
        if not isinstance(meta, dict):
            continue
        task_skill = str(meta.get("task_skill") or "").strip()
        if task_skill:
            return task_skill
    return ""


def _build_judge_content(
    *,
    synthesis_payload: dict[str, Any] | None,
    fallback_content: str,
) -> str:
    if not isinstance(synthesis_payload, dict):
        return fallback_content
    summary = str(synthesis_payload.get("summary") or "").strip()
    conclusion = str(synthesis_payload.get("conclusion") or "").strip()
    perspectives_raw = synthesis_payload.get("perspectives")
    actions_raw = synthesis_payload.get("actions")
    risks_raw = synthesis_payload.get("risks_missing")

    perspective_lines: list[str] = []
    if isinstance(perspectives_raw, list):
        for item in perspectives_raw[:3]:
            if not isinstance(item, dict):
                continue
            angle = str(item.get("angle") or "").strip()
            claim = str(item.get("claim") or "").strip()
            evidence = str(item.get("evidence") or "").strip()
            if not claim:
                continue
            line = f"- {angle}: {claim}"
            if evidence:
                line += f"（{evidence}）"
            perspective_lines.append(line)

    action_lines: list[str] = []
    if isinstance(actions_raw, list):
        for item in actions_raw[:3]:
            if not isinstance(item, dict):
                continue
            step = str(item.get("step") or "").strip()
            if step:
                action_lines.append(f"- {step}")

    risk_lines: list[str] = []
    if isinstance(risks_raw, list):
        for item in risks_raw[:3]:
            text = str(item).strip()
            if text:
                risk_lines.append(f"- {text}")

    sections: list[str] = []
    if summary:
        sections.append(f"摘要：{summary}")
    if conclusion:
        sections.append(f"结论：{conclusion}")
    if perspective_lines:
        sections.append("依据：\n" + "\n".join(perspective_lines))
    if action_lines:
        sections.append("行动：\n" + "\n".join(action_lines))
    if risk_lines:
        sections.append("风险与缺失：\n" + "\n".join(risk_lines))
    text = "\n\n".join(sections).strip()
    return text or fallback_content


def _evaluate_checks(
    *,
    checks: list[dict[str, Any]],
    result: TurnResult,
    events: list[TurnEvent],
    hard_mode: bool,
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for check in checks:
        check_id = str(check.get("id") or "check")
        kind = str(check.get("kind") or "").strip()
        if kind == "status_equals":
            expected = str(check.get("value") or "ok")
            passed = result.status == expected
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "usage_fields_exist":
            fields = [str(item) for item in check.get("fields", []) if str(item).strip()]
            passed = all(field in (result.usage or {}) for field in fields)
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "trace_events_exist":
            required = {str(item) for item in check.get("events", []) if str(item).strip()}
            existing = {event.event for event in events}
            passed = required.issubset(existing)
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "content_min_chars":
            min_chars = int(check.get("min_chars") or 0)
            passed = len(result.content or "") >= min_chars
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "content_keywords_any":
            keywords = [str(item).lower() for item in check.get("keywords", []) if str(item).strip()]
            content = str(result.content or "").lower()
            hit = any(keyword in content for keyword in keywords) if keywords else True
            output.append({"id": check_id, "kind": kind, "passed": hit, "score": 1.0 if hit else 0.0})
            continue
        if kind == "block_count_min":
            min_count = int(check.get("min_count") or 0)
            passed = len(result.blocks) >= min_count
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        # Unknown check is neutral in soft mode, fail-closed in hard mode.
        score = 0.0 if hard_mode else 1.0
        output.append({"id": check_id, "kind": kind, "passed": not hard_mode, "score": score})
    return output


def _build_trace_summary(events: list[TurnEvent]) -> dict[str, Any]:
    if not events:
        return {"event_count": 0, "events": [], "wave_count": 0, "capability_count": 0}
    event_names = [event.event for event in events]
    wave_indices = [
        int((event.data or {}).get("wave_index"))
        for event in events
        if isinstance(event.data, dict) and (event.data or {}).get("wave_index") is not None
    ]
    capability_finished = sum(1 for event in events if event.event == "capability_finished")
    return {
        "event_count": len(events),
        "events": event_names,
        "wave_count": max(wave_indices) if wave_indices else 0,
        "capability_count": capability_finished,
    }


def _load_baseline_case_map(
    *,
    output_root: Path,
    baseline_run_id: str | None,
) -> dict[str, dict[str, Any]]:
    if not baseline_run_id:
        return {}
    path = output_root / baseline_run_id / "case_results.jsonl"
    if not path.exists():
        return {}
    out: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                continue
            case_id = str(row.get("case_id") or "").strip()
            if case_id:
                out[case_id] = row
    return out


def _align_ab_cases(
    *,
    candidate_cases: list[AdvisorUXCaseExecution],
    baseline_map: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    aligned: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    for candidate in candidate_cases:
        base = baseline_map.get(candidate.case_id)
        if base is None:
            mismatches.append(
                {
                    "case_id": candidate.case_id,
                    "reason": "baseline_missing_case",
                }
            )
            continue
        baseline_payload = base.get("judge_payload")
        if not isinstance(baseline_payload, dict):
            mismatches.append(
                {
                    "case_id": candidate.case_id,
                    "reason": "baseline_missing_judge_payload",
                }
            )
            continue
        aligned.append(
            {
                "case_id": candidate.case_id,
                "baseline": baseline_payload,
                "candidate": candidate.judge_payload,
                "candidate_bucket": candidate.bucket,
            }
        )
    return aligned, mismatches


def _merge_case_results_in_pair_order(
    *,
    aligned_pairs: list[dict[str, Any]],
    scored_results: list[AdvisorUXJudgeCaseResult],
    unscored_results: list[AdvisorUXJudgeCaseResult],
) -> list[AdvisorUXJudgeCaseResult]:
    indexed: dict[str, AdvisorUXJudgeCaseResult] = {}
    for item in scored_results:
        indexed[item.case_id] = item
    for item in unscored_results:
        indexed[item.case_id] = item
    ordered: list[AdvisorUXJudgeCaseResult] = []
    for pair in aligned_pairs:
        case_id = str(pair.get("case_id") or "")
        entry = indexed.get(case_id)
        if entry is not None:
            ordered.append(entry)
    return ordered


def _resolve_unscored_buckets(raw: list[str] | None) -> set[str]:
    base = {
        item.strip().lower()
        for item in (raw or list(DEFAULT_UNSCORED_BUCKETS))
        if item and str(item).strip()
    }
    base.update(FORCED_UNSCORED_BUCKETS)
    return base


def _compute_mean_delta_by_dim(
    scored_case_results: list[AdvisorUXJudgeCaseResult],
) -> dict[str, float]:
    if not scored_case_results:
        return {dim: 0.0 for dim in RUBRIC_DIMENSIONS}
    return {
        dim: round(
            _mean(
                item.candidate_scores.get(dim, 0.0) - item.baseline_scores.get(dim, 0.0)
                for item in scored_case_results
            ),
            4,
        )
        for dim in RUBRIC_DIMENSIONS
    }


def _build_low_score_case_study(
    *,
    candidate_cases: list[AdvisorUXCaseExecution],
    judge_case_results: list[AdvisorUXJudgeCaseResult],
) -> dict[str, Any]:
    candidate_map = {item.case_id: item for item in candidate_cases}
    low_cases: list[dict[str, Any]] = []
    for judged in judge_case_results:
        if judged.scoring_status != "scored":
            continue
        if (judged.candidate_mean or 0.0) > 3.0:
            continue
        case = candidate_map.get(judged.case_id)
        if case is None:
            continue
        low_cases.append(
            {
                "case_id": judged.case_id,
                "bucket": case.bucket,
                "skill_id": case.skill_id,
                "candidate_mean": judged.candidate_mean,
                "reason_codes": list(judged.reason_codes),
                "degraded_caps": list(case.degraded_caps),
                "synthesis_present": bool(case.synthesis_present),
                "primary_angle_covered": bool(case.primary_angle_covered),
                "fallback_used": bool(case.fallback_used),
            }
        )

    by_bucket: dict[str, int] = {}
    by_skill: dict[str, int] = {}
    by_reason_code: dict[str, int] = {}
    by_fallback_used = {"true": 0, "false": 0}
    for row in low_cases:
        bucket = str(row.get("bucket") or "unknown")
        by_bucket[bucket] = by_bucket.get(bucket, 0) + 1
        skill_id = str(row.get("skill_id") or "default")
        by_skill[skill_id] = by_skill.get(skill_id, 0) + 1
        reason_codes = row.get("reason_codes")
        if isinstance(reason_codes, list):
            for code in reason_codes:
                key = str(code or "").strip() or "unknown"
                by_reason_code[key] = by_reason_code.get(key, 0) + 1
        by_fallback_used["true" if bool(row.get("fallback_used")) else "false"] += 1

    return {
        "low_score_case_count": len(low_cases),
        "by_bucket": dict(sorted(by_bucket.items(), key=lambda item: (-item[1], item[0]))),
        "by_skill": dict(sorted(by_skill.items(), key=lambda item: (-item[1], item[0]))),
        "by_reason_code": dict(sorted(by_reason_code.items(), key=lambda item: (-item[1], item[0]))),
        "by_fallback_used": by_fallback_used,
        "cases": low_cases,
    }


def _build_by_skill_metrics(
    *,
    candidate_cases: list[AdvisorUXCaseExecution],
    judge_case_results: list[AdvisorUXJudgeCaseResult],
) -> list[AdvisorUXSkillMetrics]:
    candidate_map = {item.case_id: item for item in candidate_cases}
    buckets: dict[str, dict[str, Any]] = {}
    for case in candidate_cases:
        key = case.skill_id or "default"
        bucket = buckets.setdefault(
            key,
            {
                "case_count": 0,
                "scored_case_count": 0,
                "wins": 0,
                "scores": [],
                "low_scores": 0,
            },
        )
        bucket["case_count"] += 1

    for judged in judge_case_results:
        case = candidate_map.get(judged.case_id)
        if case is None:
            continue
        key = case.skill_id or "default"
        bucket = buckets.setdefault(
            key,
            {
                "case_count": 0,
                "scored_case_count": 0,
                "wins": 0,
                "scores": [],
                "low_scores": 0,
            },
        )
        if judged.scoring_status != "scored":
            continue
        score = float(judged.candidate_mean or 0.0)
        bucket["scored_case_count"] += 1
        bucket["scores"].append(score)
        if judged.winner == "candidate":
            bucket["wins"] += 1
        if score <= 3.0:
            bucket["low_scores"] += 1

    out: list[AdvisorUXSkillMetrics] = []
    for skill_id, raw in buckets.items():
        scored_count = int(raw.get("scored_case_count", 0))
        scores = [float(item) for item in raw.get("scores", [])]
        out.append(
            AdvisorUXSkillMetrics(
                skill_id=skill_id,
                case_count=int(raw.get("case_count", 0)),
                scored_case_count=scored_count,
                candidate_win_rate=round(
                    (float(raw.get("wins", 0)) / scored_count) if scored_count else 0.0,
                    4,
                ),
                mean_score=round(_mean(scores) if scores else 0.0, 4),
                low_score_rate=round(
                    (float(raw.get("low_scores", 0)) / scored_count) if scored_count else 0.0,
                    4,
                ),
            )
        )
    return sorted(out, key=lambda item: (-item.case_count, item.skill_id))


def _is_generic_refusal_text(content: str) -> bool:
    text = (content or "").strip().lower()
    if not text:
        return True
    if len(text) > 360:
        return False
    patterns = [
        "please be more specific",
        "i couldn't build",
        "i did not detect",
        "unable to",
        "无法处理",
        "请更具体",
        "没识别到",
    ]
    return any(pattern in text for pattern in patterns)


def _has_degradation_intrusion_text(content: str) -> bool:
    text = str(content or "").strip()
    if not text:
        return False
    return any(pattern.search(text) for pattern in _DEGRADATION_INTRUSION_PATTERNS)


def _compute_degradation_intrusion_rate(candidate_cases: list[AdvisorUXCaseExecution]) -> float:
    if not candidate_cases:
        return 0.0
    intrusion_count = 0
    for case in candidate_cases:
        payload = case.judge_payload if isinstance(case.judge_payload, dict) else {}
        content = str(payload.get("content") or case.final_content or "")
        if _has_degradation_intrusion_text(content):
            intrusion_count += 1
    return intrusion_count / len(candidate_cases)


def _compute_transparency_signal_density(candidate_cases: list[AdvisorUXCaseExecution]) -> float:
    if not candidate_cases:
        return 0.0
    hit = 0
    total = len(candidate_cases) * 3
    for case in candidate_cases:
        payload = case.judge_payload if isinstance(case.judge_payload, dict) else {}
        digest = payload.get("execution_digest") if isinstance(payload.get("execution_digest"), dict) else {}
        what_done = str(digest.get("what_done") or "").strip()
        why_next = str(digest.get("why_next") or "").strip()
        needs_input = digest.get("needs_input")
        has_needs_input = isinstance(needs_input, list) and any(str(item).strip() for item in needs_input)
        if what_done:
            hit += 1
        if why_next:
            hit += 1
        if has_needs_input:
            hit += 1
    return hit / max(1, total)


def _mean(values) -> float:
    vals = [float(item) for item in values]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


async def _judge_aligned_pairs(
    *,
    judge: AdvisorUXABJudge,
    run_id: str,
    aligned_pairs: list[dict[str, Any]],
    concurrency: int,
) -> list[AdvisorUXJudgeCaseResult]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _one(item: dict[str, Any]) -> AdvisorUXJudgeCaseResult:
        async with semaphore:
            return await judge.judge_case(
                run_id=run_id,
                case_id=str(item.get("case_id") or ""),
                baseline_payload=dict(item.get("baseline") or {}),
                candidate_payload=dict(item.get("candidate") or {}),
            )

    return await asyncio.gather(*[_one(item) for item in aligned_pairs])


async def _collect_token_usage(
    *,
    suffix: str,
    caller_prefixes: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    try:
        async with async_session_factory() as session:
            stmt = select(
                TokenUsage.total_tokens,
                TokenUsage.error,
                TokenUsage.latency_ms,
                TokenUsage.caller,
            ).where(TokenUsage.caller.like(f"%#{suffix}"))
            rows = (await session.execute(stmt)).all()
    except Exception as exc:
        return {
            "calls": 0,
            "errors": 0,
            "tokens": 0,
            "median_latency_ms": 0.0,
            "p90_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
            "error": str(exc),
        }

    prefixes = tuple(
        str(prefix).strip().lower()
        for prefix in (caller_prefixes or ())
        if str(prefix).strip()
    )
    filtered: list[tuple[int, str | None, int | None]] = []
    for total_tokens, error, latency_ms, caller in rows:
        caller_text = str(caller or "").strip().lower()
        if prefixes and not caller_text.startswith(prefixes):
            continue
        filtered.append((int(total_tokens or 0), error, latency_ms))

    calls = len(filtered)
    errors = sum(1 for _, error, _ in filtered if error)
    tokens = sum(total for total, _, _ in filtered)
    latencies = [int(latency) for _, _, latency in filtered if latency is not None]
    return {
        "calls": calls,
        "errors": errors,
        "tokens": int(tokens),
        "median_latency_ms": round(_percentile(latencies, 0.5), 2),
        "p90_latency_ms": round(_percentile(latencies, 0.9), 2),
        "p95_latency_ms": round(_percentile(latencies, 0.95), 2),
    }


def _percentile(values: list[int], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil(len(ordered) * ratio) - 1))
    return float(ordered[idx])


def _write_summary(*, run_dir: Path, report: AdvisorUXEvalReport) -> None:
    token_stats = report.metrics.get("token_usage_by_stage", {})
    latency_stats = report.metrics.get("latency_ms_by_stage", {})
    scoring_metrics = report.metrics.get("scoring", {})
    scored_judge_metrics = report.metrics.get("scored_judge", {})
    execution_metrics = report.metrics.get("execution", {})
    experience_watch = report.metrics.get("experience_watch", {})
    low_score_case_study = report.metrics.get("low_score_case_study", {})
    by_skill_rows = scored_judge_metrics.get("by_skill", [])
    judge_summary = report.judge_summary or {}
    lines = [
        f"# Advisor UX Gold Eval {report.run_id}",
        "",
        f"- status: `{report.status}`",
        f"- dataset_id: `{report.config.get('dataset_id')}`",
        f"- candidate_case_count: `{execution_metrics.get('candidate_case_count', 0)}`",
        f"- aligned_case_count: `{execution_metrics.get('aligned_case_count', 0)}`",
        f"- mismatch_count: `{execution_metrics.get('mismatch_count', 0)}`",
        f"- scored_case_count: `{scoring_metrics.get('scored_case_count', 0)}`",
        f"- unscored_case_count: `{scoring_metrics.get('unscored_case_count', 0)}`",
        f"- scoring_coverage_rate: `{scoring_metrics.get('scoring_coverage_rate', 0)}`",
        f"- scored_candidate_win_rate: `{scored_judge_metrics.get('candidate_win_rate', 0)}`",
        f"- scored_overall_user_feel_mean: `{scored_judge_metrics.get('overall_user_feel_mean', 0)}`",
        f"- generic_refusal_rate: `{experience_watch.get('generic_refusal_rate', 0)}`",
        f"- transparency_low_score_rate: `{experience_watch.get('transparency_low_score_rate', 0)}`",
        f"- degradation_intrusion_rate: `{experience_watch.get('degradation_intrusion_rate', 0)}`",
        f"- transparency_signal_density: `{experience_watch.get('transparency_signal_density', 0)}`",
        f"- low_score_case_count: `{low_score_case_study.get('low_score_case_count', 0)}`",
        "",
        "## Token Usage",
        f"- candidate_tokens: `{(token_stats.get('candidate') or {}).get('tokens', 0)}`",
        f"- judge_tokens: `{(token_stats.get('judge') or {}).get('tokens', 0)}`",
        f"- total_tokens: `{token_stats.get('total_tokens', 0)}`",
        "",
        "## Latency (ms)",
        f"- candidate median/p90/p95: `{(latency_stats.get('candidate') or {}).get('median', 0)}` / "
        f"`{(latency_stats.get('candidate') or {}).get('p90', 0)}` / "
        f"`{(latency_stats.get('candidate') or {}).get('p95', 0)}`",
        f"- judge median/p90/p95: `{(latency_stats.get('judge') or {}).get('median', 0)}` / "
        f"`{(latency_stats.get('judge') or {}).get('p90', 0)}` / "
        f"`{(latency_stats.get('judge') or {}).get('p95', 0)}`",
    ]
    if judge_summary:
        lines.extend(
            [
                "",
                "## Judge Summary",
                f"- scored_case_count: `{judge_summary.get('scored_case_count', 0)}`",
                f"- unscored_case_count: `{judge_summary.get('unscored_case_count', 0)}`",
                f"- overall_user_feel_mean: `{judge_summary.get('overall_user_feel_mean', 0)}`",
                f"- recommendations: `{len(judge_summary.get('recommendations') or [])}` items",
            ]
        )
    if isinstance(by_skill_rows, list) and by_skill_rows:
        lines.extend(["", "## Skill Breakdown"])
        for item in by_skill_rows[:8]:
            if not isinstance(item, dict):
                continue
            lines.append(
                "- "
                + f"{item.get('skill_id', 'default')}: "
                + f"cases={item.get('case_count', 0)}, "
                + f"scored={item.get('scored_case_count', 0)}, "
                + f"mean={item.get('mean_score', 0)}, "
                + f"win_rate={item.get('candidate_win_rate', 0)}, "
                + f"low_score_rate={item.get('low_score_rate', 0)}"
            )
    write_summary(run_dir / "summary.md", lines)
