"""Recommendation gold-set live evaluation (persona-driven + LLM-as-judge)."""

from __future__ import annotations

import asyncio
import csv
import json
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select

from scholarpath.chat.agent import ChatAgent
from scholarpath.db.models import Student, TokenUsage
from scholarpath.db.redis import redis_pool
from scholarpath.db.session import async_session_factory
from scholarpath.llm import LLMClient, get_llm_client
from scholarpath.services.recommendation_skills import map_bucket_to_skill

DEFAULT_OUTPUT_DIR = Path(".benchmarks/recommendation_gold")
_TOP_N_PATTERNS = (
    re.compile(r"\btop\s*[- ]?(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"\bshortlist(?:\s+of)?\s+(\d{1,2})\b", re.IGNORECASE),
    re.compile(r"前\s*(\d{1,2})\s*(?:所|个)?", re.IGNORECASE),
    re.compile(r"(\d{1,2})\s*所", re.IGNORECASE),
)


@dataclass(slots=True)
class RecommendationGoldCase:
    case_id: str
    bucket: str
    tags: list[str]
    student_seed: dict[str, Any]
    turns: list[str]
    hard_checks: list[str]


@dataclass(slots=True)
class RecommendationCaseResult:
    case_id: str
    bucket: str
    status: str
    student_id: str | None
    session_id: str
    turn_count: int
    payload_source: str
    payload_present: bool
    route_plan_applied: bool
    route_hit: bool
    skill_id_used: str | None
    required_output_missing: bool
    forced_retry_count: int
    hard_checks: dict[str, bool]
    hard_pass_count: int
    hard_total: int
    hard_passed: bool
    payload: dict[str, Any] | None
    turn_payload_presence: list[bool]
    turn_outputs: list[dict[str, Any]]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RecommendationJudgeCaseResult:
    case_id: str
    case_score: float
    relevance: float
    personalization_fit: float
    actionability: float
    constraint_awareness: float
    trustworthiness: float
    confidence: float
    notes: str
    issues: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RecommendationGoldEvalReport:
    run_id: str
    generated_at: str
    status: str
    config: dict[str, Any]
    gate: dict[str, Any]
    metrics: dict[str, Any]
    case_results: list[dict[str, Any]]
    judge_case_results: list[dict[str, Any]]
    errors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RecommendationLiveJudge:
    """Single-arm recommendation judge (LLM-as-judge)."""

    def __init__(
        self,
        llm: LLMClient | None = None,
        *,
        temperature: float = 0.1,
        case_max_tokens: int = 900,
    ) -> None:
        self._llm = llm or get_llm_client()
        self._temperature = temperature
        self._case_max_tokens = case_max_tokens

    async def judge_case(
        self,
        *,
        case: RecommendationGoldCase,
        result: RecommendationCaseResult,
    ) -> RecommendationJudgeCaseResult:
        payload = result.payload or {}
        schools = payload.get("schools")
        if not isinstance(schools, list):
            schools = []
        school_excerpt = []
        for item in schools[:8]:
            if not isinstance(item, dict):
                continue
            school_excerpt.append(
                {
                    "school_name": item.get("school_name"),
                    "tier": item.get("tier"),
                    "overall_score": item.get("overall_score"),
                    "admission_probability": item.get("admission_probability"),
                    "net_price": item.get("net_price"),
                    "key_reasons": item.get("key_reasons"),
                }
            )

        prompt_payload = {
            "task": "Judge recommendation quality for a college advising case.",
            "rubric": {
                "relevance": "0-5",
                "personalization_fit": "0-5",
                "actionability": "0-5",
                "constraint_awareness": "0-5",
                "trustworthiness": "0-5",
                "case_score": "0-100 overall",
                "confidence": "0-1",
                "notes": "short string",
                "issues": "list of short strings",
            },
            "case": {
                "case_id": case.case_id,
                "bucket": case.bucket,
                "tags": case.tags,
                "student_seed": case.student_seed,
                "turns": case.turns,
                "hard_checks": case.hard_checks,
            },
            "assistant_output": {
                "payload_present": result.payload_present,
                "hard_checks": result.hard_checks,
                "school_excerpt": school_excerpt,
                "strategy_summary": payload.get("strategy_summary"),
                "ed_recommendation": payload.get("ed_recommendation"),
                "ea_recommendations": payload.get("ea_recommendations"),
                "scenario_validation": payload.get("scenario_validation"),
                "constraint_status": payload.get("constraint_status"),
                "constraint_fail_reasons": payload.get("constraint_fail_reasons"),
                "deepsearch_pending": payload.get("deepsearch_pending"),
                "deepsearch_fallback_triggered": payload.get("deepsearch_fallback_triggered"),
            },
        }
        schema = {
            "type": "object",
            "properties": {
                "case_score": {"type": "number"},
                "relevance": {"type": "number"},
                "personalization_fit": {"type": "number"},
                "actionability": {"type": "number"},
                "constraint_awareness": {"type": "number"},
                "trustworthiness": {"type": "number"},
                "confidence": {"type": "number"},
                "notes": {"type": "string"},
                "issues": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
            "required": [
                "case_score",
                "relevance",
                "personalization_fit",
                "actionability",
                "constraint_awareness",
                "trustworthiness",
                "confidence",
                "notes",
                "issues",
            ],
            "additionalProperties": False,
        }
        try:
            judged = await self._llm.complete_json(
                [
                    {
                        "role": "system",
                        "content": (
                            "You are a strict quality judge for college recommendation outputs. "
                            "Return only JSON."
                        ),
                    },
                    {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False)},
                ],
                schema=schema,
                temperature=self._temperature,
                max_tokens=self._case_max_tokens,
                caller="eval.recommendation_judge",
            )
        except Exception as exc:
            return RecommendationJudgeCaseResult(
                case_id=case.case_id,
                case_score=0.0,
                relevance=0.0,
                personalization_fit=0.0,
                actionability=0.0,
                constraint_awareness=0.0,
                trustworthiness=0.0,
                confidence=0.0,
                notes="judge_call_failed",
                issues=[],
                error=str(exc),
            )

        issues = judged.get("issues")
        if not isinstance(issues, list):
            issues = []
        return RecommendationJudgeCaseResult(
            case_id=case.case_id,
            case_score=_to_float(judged.get("case_score")),
            relevance=_to_float(judged.get("relevance")),
            personalization_fit=_to_float(judged.get("personalization_fit")),
            actionability=_to_float(judged.get("actionability")),
            constraint_awareness=_to_float(judged.get("constraint_awareness")),
            trustworthiness=_to_float(judged.get("trustworthiness")),
            confidence=_to_float(judged.get("confidence")),
            notes=str(judged.get("notes") or ""),
            issues=[str(item) for item in issues],
            error=None,
        )


def generate_default_recommendation_cases() -> list[RecommendationGoldCase]:
    """Build deterministic Mini30 persona cases."""
    majors = [
        "Computer Science",
        "Economics",
        "Mechanical Engineering",
        "Biology",
        "Psychology",
        "Data Science",
    ]
    regions = ["West Coast", "Northeast", "Midwest", "South", "Urban", "Suburban"]
    budgets = [12000, 18000, 25000, 32000, 42000, 55000]
    gpas = [3.4, 3.55, 3.7, 3.8, 3.9, 3.95]
    sats = [1240, 1320, 1400, 1460, 1510, 1560]

    buckets = [
        "budget_first",
        "risk_first",
        "major_first",
        "geo_first",
        "roi_first",
    ]
    cases: list[RecommendationGoldCase] = []
    case_counter = 1
    for bucket in buckets:
        for idx in range(6):
            major = majors[idx]
            region = regions[idx]
            budget = budgets[idx]
            gpa = gpas[idx]
            sat = sats[idx]
            turns = [_build_turn(bucket=bucket, major=major, region=region, budget=budget, gpa=gpa, sat=sat)]
            # Add a second-turn follow-up on two fixed positions per bucket.
            if idx in {2, 5}:
                turns.append(
                    "Please give me a concise top-5 shortlist with one-line rationale per school."
                )
            case_id = f"rec_{case_counter:03d}"
            case_counter += 1
            cases.append(
                RecommendationGoldCase(
                    case_id=case_id,
                    bucket=bucket,
                    tags=[bucket, major.lower().replace(" ", "_"), region.lower().replace(" ", "_")],
                    student_seed={
                        "gpa": gpa,
                        "sat_total": sat,
                        "budget_usd": budget,
                        "intended_majors": [major],
                        "preferred_region": region,
                        "risk_preference": "balanced" if bucket != "risk_first" else "safer",
                    },
                    turns=turns,
                    hard_checks=[
                        "payload_exists",
                        "schools_non_empty",
                        "required_school_keys",
                        "budget_signal_present",
                    ],
                )
            )
    return cases


def select_cases(
    cases: list[RecommendationGoldCase],
    *,
    sample_size: int | None = 30,
    case_ids: list[str] | None = None,
) -> list[RecommendationGoldCase]:
    if case_ids:
        wanted = [item.strip() for item in case_ids if item.strip()]
        mapping = {case.case_id: case for case in cases}
        unknown = [item for item in wanted if item not in mapping]
        if unknown:
            raise ValueError(f"Unknown case_ids: {unknown}")
        return [mapping[item] for item in wanted]

    ordered = sorted(cases, key=lambda item: item.case_id)
    if sample_size is None or sample_size >= len(ordered):
        return ordered
    return ordered[: max(1, sample_size)]


async def run_recommendation_gold_eval(
    *,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    sample_size: int | None = 30,
    case_ids: list[str] | None = None,
    judge_enabled: bool = True,
    judge_concurrency: int = 2,
    judge_temperature: float = 0.1,
    judge_max_tokens: int = 900,
    max_rpm_total: int = 180,
    eval_run_id: str | None = None,
) -> RecommendationGoldEvalReport:
    if max_rpm_total > 200:
        raise ValueError("max_rpm_total must be <= 200")

    started_at = datetime.now(timezone.utc)
    run_id = eval_run_id or f"recommendation-gold-{started_at:%Y%m%d-%H%M%S}"
    all_cases = generate_default_recommendation_cases()
    selected = select_cases(all_cases, sample_size=sample_size, case_ids=case_ids)

    llm = get_llm_client()
    case_results: list[RecommendationCaseResult] = []
    errors: list[dict[str, Any]] = []

    for case in selected:
        try:
            case_result = await _run_single_case(
                run_id=run_id,
                llm=llm,
                case=case,
            )
        except Exception as exc:  # pragma: no cover - runtime defensive branch
            case_result = RecommendationCaseResult(
                case_id=case.case_id,
                bucket=case.bucket,
                status="error",
                student_id=None,
                session_id=f"gold-reco-{run_id}-{case.case_id}",
                turn_count=len(case.turns),
                payload_source="none",
                payload_present=False,
                route_plan_applied=False,
                route_hit=False,
                skill_id_used=None,
                required_output_missing=False,
                forced_retry_count=0,
                hard_checks={},
                hard_pass_count=0,
                hard_total=0,
                hard_passed=False,
                payload=None,
                turn_payload_presence=[],
                turn_outputs=[],
                error=str(exc),
            )
            errors.append({"case_id": case.case_id, "error": str(exc)})
        case_results.append(case_result)

    judge_case_results: list[RecommendationJudgeCaseResult] = []
    if judge_enabled and case_results:
        judge = RecommendationLiveJudge(
            llm=llm,
            temperature=judge_temperature,
            case_max_tokens=judge_max_tokens,
        )
        judge_case_results = await _run_judge_cases(
            judge=judge,
            cases=selected,
            results=case_results,
            concurrency=judge_concurrency,
        )

    metrics = await _build_metrics(
        started_at=started_at,
        run_id=run_id,
        case_results=case_results,
        judge_case_results=judge_case_results,
    )
    gate = _build_gate(metrics=metrics)
    status = "ok" if gate["passed"] else "watch"

    report = RecommendationGoldEvalReport(
        run_id=run_id,
        generated_at=datetime.now(timezone.utc).isoformat(),
        status=status,
        config={
            "sample_size": sample_size,
            "case_ids": list(case_ids or []),
            "selected_case_ids": [item.case_id for item in selected],
            "judge_enabled": judge_enabled,
            "judge_concurrency": judge_concurrency,
            "judge_temperature": judge_temperature,
            "judge_max_tokens": judge_max_tokens,
            "max_rpm_total": max_rpm_total,
        },
        gate=gate,
        metrics=metrics,
        case_results=[item.to_dict() for item in case_results],
        judge_case_results=[item.to_dict() for item in judge_case_results],
        errors=errors,
    )
    _write_artifacts(
        output_root=Path(output_dir),
        report=report,
    )
    return report


async def _run_single_case(
    *,
    run_id: str,
    llm: LLMClient,
    case: RecommendationGoldCase,
) -> RecommendationCaseResult:
    session_id = f"gold-reco-{run_id}-{case.case_id}"
    route_plan = {
        "primary_task": "recommendation",
        "modifiers": [case.bucket],
        "required_capabilities": ["recommendation"],
        "required_outputs": ["recommendation_payload"],
        "route_lock": True,
    }
    resolved_skill_id = map_bucket_to_skill(case.bucket)
    turn_outputs: list[dict[str, Any]] = []
    turn_payload_presence: list[bool] = []
    last_payload: dict[str, Any] | None = None
    last_payload_turn_idx: int | None = None
    route_hit = False
    required_output_missing = False
    forced_retry_count = 0

    async with async_session_factory() as session:
        student = Student(
            name=f"GoldEval {case.case_id}",
            email=f"gold-{run_id}-{case.case_id}@example.com",
            gpa=float(case.student_seed.get("gpa") or 3.7),
            gpa_scale="4.0",
            sat_total=int(case.student_seed.get("sat_total") or 1400),
            sat_rw=None,
            sat_math=None,
            act_composite=None,
            toefl_total=None,
            curriculum_type="AP",
            ap_courses=None,
            extracurriculars={"activities": ["Robotics", "Volunteering"]},
            awards=None,
            intended_majors=list(case.student_seed.get("intended_majors") or ["Computer Science"]),
            budget_usd=int(case.student_seed.get("budget_usd") or 40000),
            need_financial_aid=bool(int(case.student_seed.get("budget_usd") or 40000) <= 30000),
            preferences={
                "preferred_region": case.student_seed.get("preferred_region"),
                "risk_preference": case.student_seed.get("risk_preference"),
            },
            ed_preference=None,
            target_year=2027,
            profile_completed=True,
            profile_embedding=None,
        )
        session.add(student)
        await session.flush()
        # Ensure async fallback workers can read this student record immediately.
        await session.commit()

        agent = ChatAgent(llm=llm, session=session, redis=redis_pool)
        for turn_idx, turn in enumerate(case.turns):
            turn_result = await agent.process_turn(
                session_id=session_id,
                student_id=student.id,
                message=turn,
                route_plan=route_plan,
                skill_id=resolved_skill_id,
            )
            response_text = str(turn_result.get("response_text") or "")
            route_meta = turn_result.get("route_meta") or {}
            execution_digest = turn_result.get("execution_digest") or {}
            route_hit = route_hit or bool(route_meta.get("primary_task") == "recommendation")
            required_output_missing = required_output_missing or bool(
                execution_digest.get("required_output_missing"),
            )
            forced_retry_count += int(execution_digest.get("forced_retry_count") or 0)
            summary_text, payload, parse_error = _extract_recommendation_payload(response_text)
            if payload is not None:
                last_payload = payload
                last_payload_turn_idx = turn_idx
            turn_payload_presence.append(payload is not None)
            turn_outputs.append(
                {
                    "turn_index": turn_idx,
                    "user_turn": turn,
                    "assistant_text": summary_text,
                    "payload_present": payload is not None,
                    "payload_parse_error": parse_error,
                    "route_meta": route_meta,
                    "execution_digest": execution_digest,
                }
            )

        await session.commit()
        student_id = str(student.id)

    payload_source = "none"
    if last_payload_turn_idx is not None:
        if last_payload_turn_idx == len(case.turns) - 1:
            payload_source = "final_turn"
        else:
            payload_source = f"cached_turn_{last_payload_turn_idx + 1}"

    checks = _run_hard_checks(case=case, payload=last_payload)
    hard_total = len(checks)
    hard_pass_count = sum(1 for value in checks.values() if value)
    hard_passed = hard_pass_count == hard_total and hard_total > 0
    return RecommendationCaseResult(
        case_id=case.case_id,
        bucket=case.bucket,
        status="ok" if hard_passed else "watch",
        student_id=student_id,
        session_id=session_id,
        turn_count=len(case.turns),
        payload_source=payload_source,
        payload_present=last_payload is not None,
        route_plan_applied=True,
        route_hit=route_hit,
        skill_id_used=resolved_skill_id,
        required_output_missing=required_output_missing,
        forced_retry_count=forced_retry_count,
        hard_checks=checks,
        hard_pass_count=hard_pass_count,
        hard_total=hard_total,
        hard_passed=hard_passed,
        payload=last_payload,
        turn_payload_presence=turn_payload_presence,
        turn_outputs=turn_outputs,
        error=None,
    )


def _extract_recommendation_payload(response_text: str) -> tuple[str, dict[str, Any] | None, str | None]:
    marker = "[RECOMMENDATION]"
    if marker not in response_text:
        return response_text.strip(), None, None
    text_part, json_part = response_text.split(marker, 1)
    payload_raw = (json_part or "").strip()
    if not payload_raw:
        return text_part.strip(), None, "empty_payload"
    try:
        parsed = json.loads(payload_raw)
    except Exception:
        return text_part.strip(), None, "invalid_json"
    if not isinstance(parsed, dict):
        return text_part.strip(), None, "non_object_payload"
    return text_part.strip(), parsed, None


def _run_hard_checks(
    *,
    case: RecommendationGoldCase,
    payload: dict[str, Any] | None,
) -> dict[str, bool]:
    checks: dict[str, bool] = {}
    schools: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        raw = payload.get("schools")
        if isinstance(raw, list):
            schools = [item for item in raw if isinstance(item, dict)]

    checks["payload_exists"] = payload is not None
    checks["schools_non_empty"] = len(schools) > 0
    checks["required_school_keys"] = bool(schools) and all(
        {"school_name", "tier", "overall_score", "admission_probability"}.issubset(set(item.keys()))
        for item in schools
    )
    if "budget_signal_present" in case.hard_checks:
        numeric_prices = [
            float(item.get("net_price"))
            for item in schools
            if isinstance(item.get("net_price"), (int, float))
        ]
        checks["budget_signal_present"] = bool(numeric_prices)
    return checks


async def _run_judge_cases(
    *,
    judge: RecommendationLiveJudge,
    cases: list[RecommendationGoldCase],
    results: list[RecommendationCaseResult],
    concurrency: int,
) -> list[RecommendationJudgeCaseResult]:
    semaphore = asyncio.Semaphore(max(1, concurrency))
    result_map = {item.case_id: item for item in results}

    async def _one(case: RecommendationGoldCase) -> RecommendationJudgeCaseResult:
        result = result_map[case.case_id]
        async with semaphore:
            return await judge.judge_case(case=case, result=result)

    judged = await asyncio.gather(*[_one(case) for case in cases], return_exceptions=True)
    out: list[RecommendationJudgeCaseResult] = []
    for case, item in zip(cases, judged, strict=False):
        if isinstance(item, Exception):
            out.append(
                RecommendationJudgeCaseResult(
                    case_id=case.case_id,
                    case_score=0.0,
                    relevance=0.0,
                    personalization_fit=0.0,
                    actionability=0.0,
                    constraint_awareness=0.0,
                    trustworthiness=0.0,
                    confidence=0.0,
                    notes="judge_execution_error",
                    issues=[],
                    error=str(item),
                )
            )
            continue
        out.append(item)
    return out


async def _build_metrics(
    *,
    started_at: datetime,
    run_id: str,
    case_results: list[RecommendationCaseResult],
    judge_case_results: list[RecommendationJudgeCaseResult],
) -> dict[str, Any]:
    total = len(case_results)
    payload_exists = sum(1 for item in case_results if item.payload_present)
    hard_pass = sum(1 for item in case_results if item.hard_passed)
    required_school_keys_pass = sum(
        1 for item in case_results if item.hard_checks.get("required_school_keys") is True
    )
    budget_signal_pass = sum(
        1
        for item in case_results
        if item.hard_checks.get("budget_signal_present") is True
    )
    budget_signal_total = sum(
        1
        for item in case_results
        if "budget_signal_present" in item.hard_checks
    )
    route_hit = sum(1 for item in case_results if item.route_hit)
    required_output_missing_count = sum(1 for item in case_results if item.required_output_missing)
    forced_retry_total = sum(int(item.forced_retry_count) for item in case_results)
    tier_sanity_pass = sum(
        1 for item in case_results if _tier_sanity_pass(payload=item.payload)
    )
    major_alignment_total = sum(1 for item in case_results if item.bucket == "major_first")
    major_alignment_pass = sum(
        1
        for item in case_results
        if item.bucket == "major_first" and _major_alignment_pass(payload=item.payload)
    )
    geo_alignment_total = sum(1 for item in case_results if item.bucket == "geo_first")
    geo_alignment_pass = sum(
        1
        for item in case_results
        if item.bucket == "geo_first" and _geo_alignment_pass(payload=item.payload)
    )
    budget_compliance_total = sum(1 for item in case_results if _budget_eval_eligible(payload=item.payload))
    budget_compliance_pass = sum(
        1 for item in case_results if _budget_eval_eligible(payload=item.payload) and _budget_compliance_pass(payload=item.payload)
    )
    deepsearch_fallback_triggered = sum(
        1
        for item in case_results
        if bool((item.payload or {}).get("deepsearch_fallback_triggered"))
    )
    deepsearch_task_id_present = sum(
        1
        for item in case_results
        if bool((item.payload or {}).get("deepsearch_fallback_triggered"))
        and bool((item.payload or {}).get("deepsearch_fallback_task_id"))
    )
    deepsearch_task_id_total = max(1, deepsearch_fallback_triggered)
    bucket_degraded_rate: dict[str, float] = {}
    grouped: dict[str, list[RecommendationCaseResult]] = {}
    for item in case_results:
        grouped.setdefault(item.bucket, []).append(item)
    for bucket, rows in grouped.items():
        degraded = 0
        for row in rows:
            status = ((row.payload or {}).get("constraint_status") or "none")
            if status == "degraded":
                degraded += 1
        bucket_degraded_rate[bucket] = round(degraded / max(1, len(rows)), 4)

    risk_quota_shortfall_count = 0
    geo_backfill_count = 0
    tier_cap_trigger_count = 0
    major_evidence_total = 0
    major_evidence_missing = 0
    geo_false_positive_count = 0
    scenario_constraint_effective_total = 0
    scenario_constraint_effective_pass = 0
    topn_requested_cases = 0
    topn_compliant_cases = 0
    sat_scale_mode_distribution: dict[str, int] = {"section": 0, "total": 0, "neutral": 0}
    for item in case_results:
        payload = item.payload or {}
        scenario_validation = payload.get("scenario_validation") or {}
        constraints = scenario_validation.get("constraints") or {}
        risk_quota_shortfall_count += int(
            (constraints.get("risk_tier_mix") or {}).get("shortfall_total") or 0
        )
        geo_backfill_count += int(
            (constraints.get("geo_alignment") or {}).get("backfill_count") or 0
        )
        scenario_constraint_effective_total += 1
        if _is_constraint_effective(payload):
            scenario_constraint_effective_pass += 1
        requested_top_n = _extract_requested_top_n_from_turn_outputs(item.turn_outputs)
        if requested_top_n is not None:
            topn_requested_cases += 1
            schools = _extract_schools(payload)
            if schools and len(schools) <= requested_top_n:
                topn_compliant_cases += 1
        schools = payload.get("schools") or []
        if isinstance(schools, list):
            geo_constraint = (
                (scenario_validation.get("constraints") or {}).get("geo_alignment")
                if isinstance(scenario_validation, dict)
                else {}
            )
            geo_alignment_failed = isinstance(geo_constraint, dict) and geo_constraint.get("passed") is False
            if item.bucket == "geo_first" and geo_alignment_failed:
                high_geo_scores = sum(
                    1
                    for school in schools
                    if isinstance(school, dict) and _to_float(school.get("geo_match")) >= 0.9
                )
                if high_geo_scores:
                    geo_false_positive_count += high_geo_scores
            for school in schools:
                if not isinstance(school, dict):
                    continue
                mode = str(school.get("sat_scale_mode") or "neutral").lower()
                if mode not in sat_scale_mode_distribution:
                    mode = "neutral"
                sat_scale_mode_distribution[mode] += 1
                if bool(school.get("tier_cap_triggered")):
                    tier_cap_trigger_count += 1
                if "major_match_evidence" in school:
                    major_evidence_total += 1
                    if not bool(school.get("major_match_evidence")):
                        major_evidence_missing += 1
    judge_scored = [item for item in judge_case_results if not item.error]
    judge_score_avg = (
        sum(item.case_score for item in judge_scored) / len(judge_scored)
        if judge_scored
        else 0.0
    )
    judge_dims = {
        "relevance": _mean([item.relevance for item in judge_scored]),
        "personalization_fit": _mean([item.personalization_fit for item in judge_scored]),
        "actionability": _mean([item.actionability for item in judge_scored]),
        "constraint_awareness": _mean([item.constraint_awareness for item in judge_scored]),
        "trustworthiness": _mean([item.trustworthiness for item in judge_scored]),
    }
    usage = await _collect_token_usage(
        started_at=started_at,
        caller_prefixes=(
            "chat.",
            "recommendation.",
            "eval.recommendation_judge",
        ),
    )
    return {
        "run_id": run_id,
        "case_count": total,
        "recommendation_route_hit_rate": round(route_hit / max(1, total), 4),
        "recommendation_payload_exists_rate": round(payload_exists / max(1, total), 4),
        "required_output_missing_count": required_output_missing_count,
        "forced_retry_total": forced_retry_total,
        "hard_check_pass_rate": round(hard_pass / max(1, total), 4),
        "required_school_keys_pass_rate": round(required_school_keys_pass / max(1, total), 4),
        "budget_signal_pass_rate": round(budget_signal_pass / max(1, budget_signal_total), 4)
        if budget_signal_total
        else None,
        "tier_sanity_pass_rate": round(tier_sanity_pass / max(1, total), 4),
        "major_alignment_pass_rate": round(major_alignment_pass / max(1, major_alignment_total), 4)
        if major_alignment_total
        else None,
        "geo_alignment_pass_rate": round(geo_alignment_pass / max(1, geo_alignment_total), 4)
        if geo_alignment_total
        else None,
        "budget_compliance_pass_rate": round(budget_compliance_pass / max(1, budget_compliance_total), 4)
        if budget_compliance_total
        else None,
        "deepsearch_fallback_trigger_rate": round(deepsearch_fallback_triggered / max(1, total), 4),
        "deepsearch_task_id_presence_rate": round(
            deepsearch_task_id_present / deepsearch_task_id_total,
            4,
        ),
        "bucket_degraded_rate": bucket_degraded_rate,
        "scenario_constraint_effective_rate": round(
            scenario_constraint_effective_pass / max(1, scenario_constraint_effective_total),
            4,
        ),
        "topn_compliance_rate": round(topn_compliant_cases / max(1, topn_requested_cases), 4)
        if topn_requested_cases
        else None,
        "risk_quota_shortfall_count": risk_quota_shortfall_count,
        "geo_backfill_count": geo_backfill_count,
        "geo_false_positive_count": geo_false_positive_count,
        "tier_cap_trigger_count": tier_cap_trigger_count,
        "major_evidence_missing_rate": round(major_evidence_missing / max(1, major_evidence_total), 4)
        if major_evidence_total
        else None,
        "sat_scale_mode_distribution": sat_scale_mode_distribution,
        "scoring_coverage_rate": round(len(judge_scored) / max(1, len(judge_case_results)), 4)
        if judge_case_results
        else 0.0,
        "overall_user_feel_mean": round(judge_score_avg / 20.0, 4) if judge_case_results else 0.0,
        "judge_case_score_avg": round(judge_score_avg, 4),
        "judge_dim_means": {key: round(value, 4) for key, value in judge_dims.items()},
        "tokens_by_stage": usage,
    }


def _build_gate(*, metrics: dict[str, Any]) -> dict[str, Any]:
    scoring_coverage_rate = float(metrics.get("scoring_coverage_rate") or 0.0)
    route_hit_rate = float(metrics.get("recommendation_route_hit_rate") or 0.0)
    payload_exists_rate = float(metrics.get("recommendation_payload_exists_rate") or 0.0)
    hard_check_pass_rate = float(metrics.get("hard_check_pass_rate") or 0.0)
    overall_user_feel_mean = float(metrics.get("overall_user_feel_mean") or 0.0)
    judge_case_score_avg = float(metrics.get("judge_case_score_avg") or 0.0)
    passed = (
        scoring_coverage_rate >= 0.95
        and route_hit_rate >= 0.95
        and payload_exists_rate >= 0.95
        and hard_check_pass_rate >= 0.98
        and overall_user_feel_mean >= 3.0
        and judge_case_score_avg >= 60.0
    )
    return {
        "scoring_coverage_rate": scoring_coverage_rate,
        "recommendation_route_hit_rate": route_hit_rate,
        "recommendation_payload_exists_rate": payload_exists_rate,
        "hard_check_pass_rate": hard_check_pass_rate,
        "overall_user_feel_mean": overall_user_feel_mean,
        "judge_case_score_avg": judge_case_score_avg,
        "passed": passed,
    }


async def _collect_token_usage(
    *,
    started_at: datetime,
    caller_prefixes: tuple[str, ...] | None,
) -> dict[str, Any]:
    try:
        async with async_session_factory() as session:
            stmt = select(TokenUsage).where(TokenUsage.created_at >= started_at)
            if caller_prefixes:
                clauses = [TokenUsage.caller.like(f"{prefix}%") for prefix in caller_prefixes]
                stmt = stmt.where(or_(*clauses))
            rows = (await session.execute(stmt)).scalars().all()
    except Exception:
        return {
            "calls": 0,
            "tokens": 0,
            "errors": 0,
            "p95_latency_ms": 0.0,
            "rpm_actual_avg": 0.0,
        }
    calls = len(rows)
    tokens = sum(int(row.total_tokens or 0) for row in rows)
    errors = sum(1 for row in rows if row.error)
    latencies = sorted([int(row.latency_ms) for row in rows if row.latency_ms is not None])
    p95_latency = 0.0
    if latencies:
        p95_latency = float(latencies[min(len(latencies) - 1, max(0, int(len(latencies) * 0.95) - 1))])
    duration_min = max((datetime.now(timezone.utc) - started_at).total_seconds() / 60.0, 1e-6)
    return {
        "calls": calls,
        "tokens": tokens,
        "errors": errors,
        "p95_latency_ms": p95_latency,
        "rpm_actual_avg": round(calls / duration_min, 4),
    }


def _tier_sanity_pass(*, payload: dict[str, Any] | None) -> bool:
    schools = _extract_schools(payload)
    if not schools:
        return False
    for item in schools:
        acceptance_raw = item.get("acceptance_rate")
        admission_prob = _to_float(item.get("admission_probability"))
        tier = str(item.get("tier") or "")
        acceptance_pct = _normalize_acceptance_pct(acceptance_raw)
        if acceptance_pct is not None and acceptance_pct < 8.0:
            if tier in {"safety", "likely"} and admission_prob < 0.85:
                return False
        if acceptance_pct is not None and acceptance_pct < 12.0:
            if tier == "likely" and admission_prob < 0.78:
                return False
    return True


def _major_alignment_pass(*, payload: dict[str, Any] | None) -> bool:
    scenario = (payload or {}).get("scenario_validation")
    if isinstance(scenario, dict):
        major_constraint = (scenario.get("constraints") or {}).get("major_alignment")
        if isinstance(major_constraint, dict) and "passed" in major_constraint:
            return bool(major_constraint.get("passed"))
    schools = _extract_schools(payload)
    if not schools:
        return False
    scores = [
        _to_float(item.get("major_match"))
        for item in schools
        if item.get("major_match") is not None
    ]
    if not scores:
        return False
    hits = sum(1 for score in scores if score >= 0.65)
    return (hits / max(1, len(scores))) >= 0.45


def _geo_alignment_pass(*, payload: dict[str, Any] | None) -> bool:
    scenario = (payload or {}).get("scenario_validation")
    if isinstance(scenario, dict):
        geo_constraint = (scenario.get("constraints") or {}).get("geo_alignment")
        if isinstance(geo_constraint, dict) and "passed" in geo_constraint:
            return bool(geo_constraint.get("passed"))
    schools = _extract_schools(payload)
    if not schools:
        return False
    scores = [
        _to_float(item.get("geo_match"))
        for item in schools
        if item.get("geo_match") is not None
    ]
    if not scores:
        return False
    hits = sum(1 for score in scores if score >= 0.75)
    return (hits / max(1, len(scores))) >= 0.5


def _budget_eval_eligible(*, payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    meta = payload.get("prefilter_meta")
    if not isinstance(meta, dict):
        return False
    return isinstance(meta.get("budget_cap_used"), (int, float))


def _budget_compliance_pass(*, payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    schools = _extract_schools(payload)
    if not schools:
        return False
    meta = payload.get("prefilter_meta") or {}
    if not isinstance(meta, dict):
        return False
    budget_cap = meta.get("budget_cap_used")
    if not isinstance(budget_cap, (int, float)):
        return False
    stretch_count = 0
    for item in schools:
        tag = str(item.get("prefilter_tag") or "")
        net_price = item.get("net_price")
        if tag == "eligible":
            if not isinstance(net_price, (int, float)) or float(net_price) > float(budget_cap):
                return False
        elif tag == "stretch":
            stretch_count += 1
            if isinstance(net_price, (int, float)) and float(net_price) <= float(budget_cap):
                return False
        else:
            return False
    return stretch_count <= 3


def _extract_schools(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    raw = payload.get("schools")
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def _normalize_acceptance_pct(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 1.0:
        parsed = parsed * 100.0
    return max(0.0, min(100.0, parsed))


def _extract_requested_top_n_from_turn_outputs(turn_outputs: list[dict[str, Any]]) -> int | None:
    if not isinstance(turn_outputs, list):
        return None
    requested: int | None = None
    for turn in turn_outputs:
        if not isinstance(turn, dict):
            continue
        text = str(turn.get("user_turn") or "")
        for pattern in _TOP_N_PATTERNS:
            match = pattern.search(text)
            if not match:
                continue
            try:
                value = int(match.group(1))
            except (TypeError, ValueError):
                continue
            requested = value if requested is None else min(requested, value)
    return requested


def _is_constraint_effective(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    scenario = payload.get("scenario_validation")
    if not isinstance(scenario, dict):
        return False
    status = str(scenario.get("constraint_status") or "")
    constraints = scenario.get("constraints")
    if not isinstance(constraints, dict):
        return False
    if status == "pass":
        return True

    minimum_results = constraints.get("minimum_results")
    if isinstance(minimum_results, dict) and minimum_results.get("passed") is False:
        return False

    skill_id = str(payload.get("skill_id_used") or scenario.get("scenario") or "")
    primary_constraint_key = None
    if skill_id.endswith("budget_first"):
        primary_constraint_key = "budget_hard_gate"
    elif skill_id.endswith("risk_first"):
        primary_constraint_key = "risk_tier_mix"
    elif skill_id.endswith("major_first"):
        primary_constraint_key = "major_alignment"
    elif skill_id.endswith("geo_first"):
        primary_constraint_key = "geo_alignment"
    elif skill_id.endswith("roi_first"):
        primary_constraint_key = "roi_career_floor"

    if primary_constraint_key is None:
        return False
    primary = constraints.get(primary_constraint_key)
    if not isinstance(primary, dict) or "passed" not in primary:
        return False
    return bool(primary.get("passed"))


def _build_turn(
    *,
    bucket: str,
    major: str,
    region: str,
    budget: int,
    gpa: float,
    sat: int,
) -> str:
    if bucket == "budget_first":
        return (
            f"My annual budget is ${budget}. I want to study {major}. "
            "Please recommend colleges in tiers and keep affordability in mind."
        )
    if bucket == "risk_first":
        return (
            f"I have GPA {gpa:.2f} and SAT {sat}. I prefer safer admission outcomes for {major}. "
            "Please recommend schools by reach/target/safety."
        )
    if bucket == "major_first":
        return (
            f"Please prioritize major fit for {major}. My GPA is {gpa:.2f}, SAT {sat}, "
            f"and budget ${budget}. Recommend schools with clear rationale."
        )
    if bucket == "geo_first":
        return (
            f"I prefer colleges in {region}. Major: {major}. Budget: ${budget}. "
            "Please recommend schools by tier."
        )
    return (
        f"I care most about career ROI. Major: {major}. GPA {gpa:.2f}, SAT {sat}, budget ${budget}. "
        "Recommend schools with practical outcomes."
    )


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _serialize_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )


def _serialize_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, default=_json_default))
            fp.write("\n")


def _write_summary(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_history(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def _json_default(value: Any) -> Any:
    if isinstance(value, (set, frozenset)):
        return sorted(list(value))
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    return str(value)


def _write_artifacts(
    *,
    output_root: Path,
    report: RecommendationGoldEvalReport,
) -> None:
    run_dir = output_root / report.run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    _serialize_json(run_dir / "report.json", report.to_dict())
    _serialize_jsonl(run_dir / "case_results.jsonl", report.case_results)
    _serialize_jsonl(run_dir / "judge_case_results.jsonl", report.judge_case_results)
    _write_summary(
        run_dir / "summary.md",
        [
            f"# Recommendation Gold Eval {report.run_id}",
            "",
            f"- status: `{report.status}`",
            f"- gate_passed: `{report.gate.get('passed')}`",
            f"- route_hit_rate: `{report.metrics.get('recommendation_route_hit_rate')}`",
            f"- payload_exists_rate: `{report.metrics.get('recommendation_payload_exists_rate')}`",
            f"- hard_check_pass_rate: `{report.metrics.get('hard_check_pass_rate')}`",
            f"- tier_sanity_pass_rate: `{report.metrics.get('tier_sanity_pass_rate')}`",
            f"- major_alignment_pass_rate: `{report.metrics.get('major_alignment_pass_rate')}`",
            f"- geo_alignment_pass_rate: `{report.metrics.get('geo_alignment_pass_rate')}`",
            f"- budget_compliance_pass_rate: `{report.metrics.get('budget_compliance_pass_rate')}`",
            f"- deepsearch_fallback_trigger_rate: `{report.metrics.get('deepsearch_fallback_trigger_rate')}`",
            f"- deepsearch_task_id_presence_rate: `{report.metrics.get('deepsearch_task_id_presence_rate')}`",
            f"- bucket_degraded_rate: `{report.metrics.get('bucket_degraded_rate')}`",
            f"- scenario_constraint_effective_rate: `{report.metrics.get('scenario_constraint_effective_rate')}`",
            f"- topn_compliance_rate: `{report.metrics.get('topn_compliance_rate')}`",
            f"- risk_quota_shortfall_count: `{report.metrics.get('risk_quota_shortfall_count')}`",
            f"- geo_backfill_count: `{report.metrics.get('geo_backfill_count')}`",
            f"- geo_false_positive_count: `{report.metrics.get('geo_false_positive_count')}`",
            f"- tier_cap_trigger_count: `{report.metrics.get('tier_cap_trigger_count')}`",
            f"- major_evidence_missing_rate: `{report.metrics.get('major_evidence_missing_rate')}`",
            f"- sat_scale_mode_distribution: `{report.metrics.get('sat_scale_mode_distribution')}`",
            f"- judge_case_score_avg: `{report.metrics.get('judge_case_score_avg')}`",
            f"- overall_user_feel_mean: `{report.metrics.get('overall_user_feel_mean')}`",
        ],
    )
    _append_history(
        output_root / "history.csv",
        {
            "run_id": report.run_id,
            "generated_at": report.generated_at,
            "status": report.status,
            "gate_passed": report.gate.get("passed"),
            "route_hit_rate": report.metrics.get("recommendation_route_hit_rate"),
            "payload_exists_rate": report.metrics.get("recommendation_payload_exists_rate"),
            "hard_check_pass_rate": report.metrics.get("hard_check_pass_rate"),
            "tier_sanity_pass_rate": report.metrics.get("tier_sanity_pass_rate"),
            "major_alignment_pass_rate": report.metrics.get("major_alignment_pass_rate"),
            "geo_alignment_pass_rate": report.metrics.get("geo_alignment_pass_rate"),
            "budget_compliance_pass_rate": report.metrics.get("budget_compliance_pass_rate"),
            "deepsearch_fallback_trigger_rate": report.metrics.get("deepsearch_fallback_trigger_rate"),
            "deepsearch_task_id_presence_rate": report.metrics.get("deepsearch_task_id_presence_rate"),
            "scenario_constraint_effective_rate": report.metrics.get("scenario_constraint_effective_rate"),
            "topn_compliance_rate": report.metrics.get("topn_compliance_rate"),
            "geo_false_positive_count": report.metrics.get("geo_false_positive_count"),
            "tier_cap_trigger_count": report.metrics.get("tier_cap_trigger_count"),
            "major_evidence_missing_rate": report.metrics.get("major_evidence_missing_rate"),
            "judge_case_score_avg": report.metrics.get("judge_case_score_avg"),
            "overall_user_feel_mean": report.metrics.get("overall_user_feel_mean"),
        },
    )
