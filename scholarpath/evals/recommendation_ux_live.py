"""Recommendation UX gold-set live evaluation (persona replay + A/B judge)."""

from __future__ import annotations

import asyncio
import json
import math
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
from scholarpath.evals.recommendation_judge import (
    RECOMMENDATION_RUBRIC_DIMENSIONS,
    RecommendationABJudge,
    RecommendationJudgeCaseResult,
    RecommendationJudgeRunSummary,
    create_unscored_recommendation_case_result,
)
from scholarpath.llm.client import LLMClient, get_llm_client

DEFAULT_DATASET_DIR = Path(__file__).resolve().parent / "datasets"
DEFAULT_MINI_DATASET_PATH = DEFAULT_DATASET_DIR / "recommendation_persona_gold_mini_v1.json"
DEFAULT_OUTPUT_DIR = Path(".benchmarks/recommendation_ux")
DEFAULT_EXECUTION_CONCURRENCY = 3
DEFAULT_STRETCH_QUOTA = 3
_SCENARIO_IDS = [
    "budget_first",
    "risk_first",
    "major_first",
    "geo_first",
    "roi_first",
]


@dataclass(slots=True)
class RecommendationPersonaCase:
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
class RecommendationPersonaDataset:
    dataset_id: str
    version: str
    rubric_dimensions: list[str]
    cases: list[RecommendationPersonaCase]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RecommendationUXCaseExecution:
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
    recommendation_payload: dict[str, Any] | None = None
    prefilter_meta: dict[str, Any] = field(default_factory=dict)
    scenario_pack: dict[str, Any] = field(default_factory=dict)
    judge_payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RecommendationUXEvalReport:
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


async def run_recommendation_ux_gold_eval(
    *,
    dataset: str = "mini",
    baseline_run_id: str | None = None,
    judge_enabled: bool = True,
    judge_concurrency: int = 2,
    judge_temperature: float = 0.1,
    judge_max_tokens: int = 1200,
    max_rpm_total: int = 180,
    candidate_run_id: str | None = None,
    case_ids: list[str] | None = None,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    execution_concurrency: int = DEFAULT_EXECUTION_CONCURRENCY,
) -> RecommendationUXEvalReport:
    if max_rpm_total > 200:
        raise ValueError("max_rpm_total must be <= 200")
    if judge_enabled and not baseline_run_id:
        raise ValueError("baseline_run_id is required when judge_enabled=true")

    dataset_obj = load_recommendation_persona_dataset(dataset)
    selected_cases = select_recommendation_persona_cases(dataset_obj.cases, case_ids=case_ids)
    run_id = candidate_run_id or f"recommendation-ux-{datetime.now(UTC):%Y%m%d-%H%M%S}"
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

    if judge_enabled:
        baseline_map = _load_baseline_case_map(
            output_root=output_root,
            baseline_run_id=baseline_run_id,
        )
        aligned_pairs, mismatches = _align_ab_cases(
            candidate_cases=candidate_cases,
            baseline_map=baseline_map,
        )
    else:
        aligned_pairs, mismatches = [], []

    scored_pairs: list[dict[str, Any]] = []
    unscored_case_results: list[RecommendationJudgeCaseResult] = []
    for pair in aligned_pairs:
        case_id = str(pair.get("case_id") or "")
        if not judge_enabled:
            unscored_case_results.append(
                create_unscored_recommendation_case_result(
                    case_id=case_id,
                    reason="judge_disabled",
                    notes="judge disabled by cli flag",
                )
            )
            continue
        scored_pairs.append(pair)

    scored_judge_case_results: list[RecommendationJudgeCaseResult] = []
    judge_run_summary: RecommendationJudgeRunSummary | None = None
    if judge_enabled and scored_pairs:
        judge_suffix = llm.set_caller_suffix(f"{run_id}.judge")
        try:
            judge = RecommendationABJudge(
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

    hard_check_pass_rate = (
        sum(1 for item in candidate_cases if item.hard_check_passed) / max(1, len(candidate_cases))
    )
    recommendation_route_hit_rate = (
        sum(
            1
            for item in candidate_cases
            if str((item.final_usage or {}).get("active_skill_id") or "").strip() == "recommendation"
        )
        / max(1, len(candidate_cases))
    )
    recommendation_payload_exists_rate = (
        sum(1 for item in candidate_cases if isinstance(item.recommendation_payload, dict))
        / max(1, len(candidate_cases))
    )
    soft_check_mean = _mean(item.soft_check_mean for item in candidate_cases)
    scoring_coverage_rate = len(scored_case_results) / max(1, len(aligned_pairs))
    candidate_win_rate = (
        float(judge_run_summary.candidate_win_rate)
        if judge_run_summary is not None
        else (
            sum(1 for item in scored_case_results if item.winner == "candidate")
            / max(1, len(scored_case_results))
        )
    )
    overall_user_feel_mean = (
        float(judge_run_summary.overall_user_feel_mean)
        if judge_run_summary is not None
        else _mean(item.candidate_mean for item in scored_case_results if item.candidate_mean is not None)
    )
    mean_delta_by_dim = (
        dict(judge_run_summary.mean_delta_by_dim)
        if judge_run_summary is not None
        else _compute_mean_delta_by_dim(scored_case_results)
    )
    generic_refusal_rate = (
        sum(1 for item in candidate_cases if _is_generic_refusal_text(item.final_content))
        / max(1, len(candidate_cases))
    )

    candidate_usage = await _collect_token_usage(
        suffix=f"{run_id}.candidate",
        caller_prefixes=("chat.", "advisor.", "search.", "eval."),
    )
    judge_usage = await _collect_token_usage(
        suffix=f"{run_id}.judge",
        caller_prefixes=("eval.recommendation.ux.judge.",),
    )

    status_reasons: list[str] = []
    if hard_check_pass_rate < 0.98:
        status_reasons.append("hard_check_pass_rate<0.98")
    if recommendation_route_hit_rate < 0.95:
        status_reasons.append("recommendation_route_hit_rate<0.95")
    if recommendation_payload_exists_rate < 0.95:
        status_reasons.append("recommendation_payload_exists_rate<0.95")
    if judge_enabled and scoring_coverage_rate < 0.95:
        status_reasons.append("scoring_coverage_rate<0.95")
    if judge_enabled and scored_case_results:
        if candidate_win_rate < 0.55:
            status_reasons.append("candidate_win_rate<0.55")
        if overall_user_feel_mean < 3.8:
            status_reasons.append("overall_user_feel_mean<3.8")
    if judge_enabled and scored_pairs and not scored_case_results:
        status_reasons.append("no_scored_case_results")
    if any(item.status != "ok" for item in candidate_cases):
        status_reasons.append("candidate_case_error_present")

    metrics = {
        "scoring": {
            "scored_case_count": len(scored_case_results),
            "unscored_case_count": unscored_case_count,
            "scoring_coverage_rate": round(scoring_coverage_rate, 4),
            "hard_check_pass_rate": round(hard_check_pass_rate, 4),
            "recommendation_route_hit_rate": round(recommendation_route_hit_rate, 4),
            "recommendation_payload_exists_rate": round(recommendation_payload_exists_rate, 4),
            "soft_check_mean": round(soft_check_mean, 4),
        },
        "scored_judge": {
            "candidate_win_rate": round(candidate_win_rate, 4),
            "overall_user_feel_mean": round(overall_user_feel_mean, 4),
            "mean_delta_by_dim": mean_delta_by_dim,
        },
        "execution": {
            "candidate_case_count": len(candidate_cases),
            "aligned_case_count": len(aligned_pairs),
            "mismatch_count": len(mismatches),
        },
        "experience_watch": {
            "generic_refusal_rate": round(generic_refusal_rate, 4),
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
        "status_reasons": status_reasons,
    }

    status = "ok" if not status_reasons else "watch"
    report = RecommendationUXEvalReport(
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
            "max_rpm_total": max_rpm_total,
            "rubric_dimensions": list(RECOMMENDATION_RUBRIC_DIMENSIONS),
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
    _write_summary(run_dir=run_dir, report=report)
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
            "hard_check_pass_rate": (report.metrics.get("scoring") or {}).get("hard_check_pass_rate"),
            "recommendation_route_hit_rate": (report.metrics.get("scoring") or {}).get("recommendation_route_hit_rate"),
            "recommendation_payload_exists_rate": (report.metrics.get("scoring") or {}).get("recommendation_payload_exists_rate"),
            "candidate_win_rate": (report.metrics.get("scored_judge") or {}).get("candidate_win_rate"),
            "overall_user_feel_mean": (report.metrics.get("scored_judge") or {}).get("overall_user_feel_mean"),
            "generic_refusal_rate": (report.metrics.get("experience_watch") or {}).get("generic_refusal_rate"),
            "candidate_tokens": candidate_usage.get("tokens"),
            "judge_tokens": judge_usage.get("tokens"),
            "candidate_latency_median_ms": candidate_usage.get("median_latency_ms"),
            "candidate_latency_p90_ms": candidate_usage.get("p90_latency_ms"),
            "candidate_latency_p95_ms": candidate_usage.get("p95_latency_ms"),
        },
    )
    return report


def load_recommendation_persona_dataset(dataset: str | Path) -> RecommendationPersonaDataset:
    path = _resolve_dataset_path(dataset)
    payload = json.loads(path.read_text(encoding="utf-8"))
    dataset_id = str(payload.get("dataset_id") or path.stem)
    version = str(payload.get("version") or "1")
    rubric_dimensions_raw = payload.get("rubric_dimensions")
    rubric_dimensions = (
        [str(item) for item in rubric_dimensions_raw]
        if isinstance(rubric_dimensions_raw, list)
        else list(RECOMMENDATION_RUBRIC_DIMENSIONS)
    )
    cases_payload = payload.get("cases")
    if not isinstance(cases_payload, list):
        raise ValueError(f"Dataset {path} must contain a non-empty `cases` list")
    cases = [_parse_case(item) for item in cases_payload]
    _assert_unique_case_ids(cases, dataset_id=dataset_id)
    return RecommendationPersonaDataset(
        dataset_id=dataset_id,
        version=version,
        rubric_dimensions=rubric_dimensions,
        cases=cases,
    )


def select_recommendation_persona_cases(
    cases: list[RecommendationPersonaCase],
    *,
    case_ids: list[str] | None,
) -> list[RecommendationPersonaCase]:
    if not case_ids:
        return list(cases)
    mapping = {case.case_id: case for case in cases}
    wanted = [str(item).strip() for item in case_ids if str(item).strip()]
    missing = [case_id for case_id in wanted if case_id not in mapping]
    if missing:
        raise ValueError(f"Unknown recommendation persona case ids: {missing}")
    return [mapping[item] for item in wanted]


def _resolve_dataset_path(dataset: str | Path) -> Path:
    if isinstance(dataset, Path):
        return dataset
    raw = str(dataset).strip().lower()
    if raw == "mini":
        return DEFAULT_MINI_DATASET_PATH
    return Path(dataset)


def _parse_case(payload: dict[str, Any]) -> RecommendationPersonaCase:
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
    return RecommendationPersonaCase(
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


def _assert_unique_case_ids(cases: list[RecommendationPersonaCase], *, dataset_id: str) -> None:
    ids = [case.case_id for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError(f"Dataset {dataset_id} has duplicate case_id values")


async def _execute_cases(
    *,
    llm: LLMClient,
    run_id: str,
    cases: list[RecommendationPersonaCase],
    concurrency: int,
) -> list[RecommendationUXCaseExecution]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _one(case: RecommendationPersonaCase) -> RecommendationUXCaseExecution:
        async with semaphore:
            return await _execute_single_case(llm=llm, run_id=run_id, case=case)

    return await asyncio.gather(*[_one(case) for case in cases])


async def _execute_single_case(
    *,
    llm: LLMClient,
    run_id: str,
    case: RecommendationPersonaCase,
) -> RecommendationUXCaseExecution:
    t0 = time.perf_counter()
    events: list[TurnEvent] = []
    turn_result: TurnResult | None = None
    turns_executed = 0
    error_text: str | None = None
    session_id = f"recommendation-ux-{run_id}-{case.case_id}"

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
                ),
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
    recommendation_payload = _extract_recommendation_payload(result.blocks)
    hard_results = _evaluate_checks(
        checks=case.hard_checks,
        result=result,
        events=events,
        recommendation_payload=recommendation_payload,
        hard_mode=True,
    )
    soft_results = _evaluate_checks(
        checks=case.soft_checks,
        result=result,
        events=events,
        recommendation_payload=recommendation_payload,
        hard_mode=False,
    )
    hard_passed = all(bool(item.get("passed")) for item in hard_results)
    soft_mean = _mean(float(item.get("score") or 0.0) for item in soft_results)

    prefilter_meta = (
        dict(recommendation_payload.get("prefilter_meta") or {})
        if isinstance(recommendation_payload, dict)
        else {}
    )
    scenario_pack = (
        dict(recommendation_payload.get("scenario_pack") or {})
        if isinstance(recommendation_payload, dict)
        else {}
    )
    judge_payload = _build_recommendation_judge_payload(
        case=case,
        result=result,
        recommendation_payload=recommendation_payload,
    )
    return RecommendationUXCaseExecution(
        case_id=case.case_id,
        bucket=case.bucket,
        tags=case.tags,
        status=result.status,
        turns_executed=turns_executed,
        duration_ms=round((time.perf_counter() - t0) * 1000, 2),
        final_content=str(result.content or ""),
        final_blocks=[item.model_dump(mode="json") for item in result.blocks],
        final_usage=dict(result.usage or {}),
        trace_summary=_build_trace_summary(events),
        hard_check_passed=hard_passed,
        hard_check_results=hard_results,
        soft_check_mean=round(soft_mean, 4),
        soft_check_results=soft_results,
        recommendation_payload=recommendation_payload,
        prefilter_meta=prefilter_meta,
        scenario_pack=scenario_pack,
        judge_payload=judge_payload,
        error=error_text,
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


def _extract_recommendation_payload(blocks: list[ChatBlock]) -> dict[str, Any] | None:
    for block in blocks:
        if block.kind != "recommendation":
            continue
        payload = block.payload
        if isinstance(payload, dict):
            return dict(payload)
    return None


def _evaluate_checks(
    *,
    checks: list[dict[str, Any]],
    result: TurnResult,
    events: list[TurnEvent],
    recommendation_payload: dict[str, Any] | None,
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
        if kind == "recommendation_payload_exists":
            passed = isinstance(recommendation_payload, dict)
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "prefilter_meta_fields_exist":
            fields = [str(item) for item in check.get("fields", []) if str(item).strip()]
            prefilter_meta = (
                dict(recommendation_payload.get("prefilter_meta") or {})
                if isinstance(recommendation_payload, dict)
                else {}
            )
            passed = all(field in prefilter_meta for field in fields)
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "stretch_quota_max":
            max_stretch = int(check.get("max_stretch") or DEFAULT_STRETCH_QUOTA)
            prefilter_meta = (
                dict(recommendation_payload.get("prefilter_meta") or {})
                if isinstance(recommendation_payload, dict)
                else {}
            )
            stretch_count = int(prefilter_meta.get("stretch_count") or 0)
            passed = stretch_count <= max_stretch
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "budget_hard_gate":
            passed = _budget_hard_gate_passed(recommendation_payload)
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "scenario_ids_present":
            expected = [str(item) for item in check.get("ids", []) if str(item).strip()]
            scenario_pack = (
                dict(recommendation_payload.get("scenario_pack") or {})
                if isinstance(recommendation_payload, dict)
                else {}
            )
            scenario_ids = [
                str(item.get("id"))
                for item in scenario_pack.get("scenarios", [])
                if isinstance(item, dict)
            ]
            passed = all(item in scenario_ids for item in expected)
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        if kind == "scenario_shape_complete":
            passed = _scenario_shape_complete(recommendation_payload)
            output.append({"id": check_id, "kind": kind, "passed": passed, "score": 1.0 if passed else 0.0})
            continue
        # Unknown check: neutral in soft mode, fail-closed in hard mode.
        score = 0.0 if hard_mode else 1.0
        output.append({"id": check_id, "kind": kind, "passed": not hard_mode, "score": score})
    return output


def _budget_hard_gate_passed(recommendation_payload: dict[str, Any] | None) -> bool:
    if not isinstance(recommendation_payload, dict):
        return False
    prefilter_meta = dict(recommendation_payload.get("prefilter_meta") or {})
    schools = recommendation_payload.get("schools")
    if not isinstance(schools, list):
        return False
    budget_cap = prefilter_meta.get("budget_cap_used")
    if not isinstance(budget_cap, (int, float)) or float(budget_cap) <= 0:
        return True
    cap = float(budget_cap)
    for school in schools:
        if not isinstance(school, dict):
            continue
        tag = str(school.get("prefilter_tag") or "")
        net_price = school.get("net_price")
        if not isinstance(net_price, (int, float)):
            if tag == "eligible":
                return False
            continue
        if tag == "eligible" and float(net_price) > cap:
            return False
        if tag == "stretch" and float(net_price) <= cap:
            return False
    return True


def _scenario_shape_complete(recommendation_payload: dict[str, Any] | None) -> bool:
    if not isinstance(recommendation_payload, dict):
        return False
    scenario_pack = recommendation_payload.get("scenario_pack")
    if not isinstance(scenario_pack, dict):
        return False
    baseline = scenario_pack.get("baseline")
    scenarios = scenario_pack.get("scenarios")
    if not isinstance(baseline, list) or not isinstance(scenarios, list):
        return False
    if not baseline:
        return False
    required_fields = ("rank", "baseline_rank", "rank_delta", "prefilter_tag", "scenario_reason", "outcome_breakdown")
    for row in baseline:
        if not isinstance(row, dict):
            return False
        if all(field in row for field in required_fields):
            pass
        else:
            # Compact mode fallback: keep minimal integrity checks.
            if "school_name" not in row:
                return False
            if "admission_probability" not in row:
                return False
            if "sub_scores" not in row:
                return False
    scenario_ids = [str(item.get("id")) for item in scenarios if isinstance(item, dict)]
    if any(expected not in scenario_ids for expected in _SCENARIO_IDS):
        return False
    for item in scenarios:
        if not isinstance(item, dict):
            return False
        schools = item.get("schools")
        if not isinstance(schools, list) or not schools:
            return False
        for row in schools:
            if not isinstance(row, dict):
                return False
            if all(field in row for field in required_fields):
                continue
            if "school_name" not in row:
                return False
            if "admission_probability" not in row:
                return False
            if "sub_scores" not in row:
                return False
    return True


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


def _build_recommendation_judge_payload(
    *,
    case: RecommendationPersonaCase,
    result: TurnResult,
    recommendation_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    persona = dict(case.student_seed or {})
    rec_payload = dict(recommendation_payload or {})
    schools_raw = rec_payload.get("schools")
    scenario_pack = rec_payload.get("scenario_pack")
    prefilter_meta = rec_payload.get("prefilter_meta")

    top_schools: list[dict[str, Any]] = []
    if isinstance(schools_raw, list):
        for row in schools_raw[:8]:
            if not isinstance(row, dict):
                continue
            top_schools.append(
                {
                    "school_name": row.get("school_name"),
                    "tier": row.get("tier"),
                    "overall_score": row.get("overall_score"),
                    "admission_probability": row.get("admission_probability"),
                    "net_price": row.get("net_price"),
                    "prefilter_tag": row.get("prefilter_tag"),
                    "is_stretch": row.get("is_stretch"),
                    "rank_delta": row.get("rank_delta"),
                }
            )

    scenario_summary: list[dict[str, Any]] = []
    if isinstance(scenario_pack, dict):
        for item in scenario_pack.get("scenarios", []):
            if not isinstance(item, dict):
                continue
            schools = item.get("schools")
            top_names: list[str] = []
            if isinstance(schools, list):
                for row in schools[:3]:
                    if isinstance(row, dict):
                        name = str(row.get("school_name") or "").strip()
                        if name:
                            top_names.append(name)
            scenario_summary.append(
                {
                    "id": item.get("id"),
                    "label": item.get("label"),
                    "top3": top_names,
                }
            )

    return {
        "case_id": case.case_id,
        "bucket": case.bucket,
        "persona": {
            "gpa": persona.get("gpa"),
            "sat_total": persona.get("sat_total"),
            "budget_usd": persona.get("budget_usd"),
            "intended_majors": persona.get("intended_majors"),
            "preferences": persona.get("preferences"),
        },
        "user_turns": [str(item.get("content") or "") for item in case.turns],
        "assistant_final_content": str(result.content or ""),
        "prefilter_meta": dict(prefilter_meta or {}),
        "top_schools": top_schools,
        "scenario_summary": scenario_summary,
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
    candidate_cases: list[RecommendationUXCaseExecution],
    baseline_map: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    aligned: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    for candidate in candidate_cases:
        base = baseline_map.get(candidate.case_id)
        if base is None:
            mismatches.append({"case_id": candidate.case_id, "reason": "baseline_missing_case"})
            continue
        baseline_payload = base.get("judge_payload")
        if not isinstance(baseline_payload, dict):
            mismatches.append({"case_id": candidate.case_id, "reason": "baseline_missing_judge_payload"})
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
    scored_results: list[RecommendationJudgeCaseResult],
    unscored_results: list[RecommendationJudgeCaseResult],
) -> list[RecommendationJudgeCaseResult]:
    indexed: dict[str, RecommendationJudgeCaseResult] = {}
    for item in scored_results:
        indexed[item.case_id] = item
    for item in unscored_results:
        indexed[item.case_id] = item
    ordered: list[RecommendationJudgeCaseResult] = []
    for pair in aligned_pairs:
        case_id = str(pair.get("case_id") or "")
        entry = indexed.get(case_id)
        if entry is not None:
            ordered.append(entry)
    return ordered


async def _judge_aligned_pairs(
    *,
    judge: RecommendationABJudge,
    run_id: str,
    aligned_pairs: list[dict[str, Any]],
    concurrency: int,
) -> list[RecommendationJudgeCaseResult]:
    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _one(item: dict[str, Any]) -> RecommendationJudgeCaseResult:
        async with semaphore:
            return await judge.judge_case(
                run_id=run_id,
                case_id=str(item.get("case_id") or ""),
                baseline_payload=dict(item.get("baseline") or {}),
                candidate_payload=dict(item.get("candidate") or {}),
            )

    return await asyncio.gather(*[_one(item) for item in aligned_pairs])


def _compute_mean_delta_by_dim(
    results: list[RecommendationJudgeCaseResult],
) -> dict[str, float]:
    if not results:
        return {dim: 0.0 for dim in RECOMMENDATION_RUBRIC_DIMENSIONS}
    return {
        dim: round(
            _mean(
                item.candidate_scores.get(dim, 0.0) - item.baseline_scores.get(dim, 0.0)
                for item in results
            ),
            4,
        )
        for dim in RECOMMENDATION_RUBRIC_DIMENSIONS
    }


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


def _write_summary(*, run_dir: Path, report: RecommendationUXEvalReport) -> None:
    token_stats = report.metrics.get("token_usage_by_stage", {})
    latency_stats = report.metrics.get("latency_ms_by_stage", {})
    scoring_metrics = report.metrics.get("scoring", {})
    scored_judge_metrics = report.metrics.get("scored_judge", {})
    execution_metrics = report.metrics.get("execution", {})
    status_reasons = report.metrics.get("status_reasons", [])
    judge_summary = report.judge_summary or {}
    lines = [
        f"# Recommendation UX Gold Eval {report.run_id}",
        "",
        f"- status: `{report.status}`",
        f"- dataset_id: `{report.config.get('dataset_id')}`",
        f"- sample_size: `{report.config.get('sample_size')}`",
        f"- scoring_coverage_rate: `{scoring_metrics.get('scoring_coverage_rate', 0.0)}`",
        f"- hard_check_pass_rate: `{scoring_metrics.get('hard_check_pass_rate', 0.0)}`",
        f"- soft_check_mean: `{scoring_metrics.get('soft_check_mean', 0.0)}`",
        f"- candidate_win_rate: `{scored_judge_metrics.get('candidate_win_rate', 0.0)}`",
        f"- overall_user_feel_mean: `{scored_judge_metrics.get('overall_user_feel_mean', 0.0)}`",
        f"- mismatch_count: `{execution_metrics.get('mismatch_count', 0)}`",
        f"- candidate_tokens: `{token_stats.get('candidate', {}).get('tokens', 0)}`",
        f"- judge_tokens: `{token_stats.get('judge', {}).get('tokens', 0)}`",
        f"- candidate_p95_latency_ms: `{latency_stats.get('candidate', {}).get('p95', 0.0)}`",
        f"- judge_p95_latency_ms: `{latency_stats.get('judge', {}).get('p95', 0.0)}`",
    ]
    if status_reasons:
        lines.append(f"- status_reasons: `{status_reasons}`")
    if judge_summary:
        lines.append(f"- judge_status: `{judge_summary.get('status', 'n/a')}`")
        if judge_summary.get("errors"):
            lines.append(f"- judge_errors: `{judge_summary.get('errors')}`")
    write_summary(run_dir / "summary.md", lines)


def _is_generic_refusal_text(text: str) -> bool:
    content = (text or "").strip().lower()
    if not content:
        return True
    patterns = (
        "i can't help",
        "cannot help",
        "sorry",
        "i'm unable",
        "无法",
        "抱歉",
    )
    return any(item in content for item in patterns)


def _mean(values: Any) -> float:
    seq = [float(item) for item in values]
    return statistics.fmean(seq) if seq else 0.0
