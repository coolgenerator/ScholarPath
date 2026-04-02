"""LLM-as-judge utilities for Advisor orchestrator gold evaluation."""

from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from scholarpath.config import settings
from scholarpath.llm import LLMClient

_BASE_DIMENSIONS: tuple[str, ...] = (
    "route_correctness",
    "coordination_discipline",
    "clarify_safety",
    "recoverability",
)
_REEDIT_DIMENSIONS: tuple[str, ...] = (
    "route_correctness",
    "coordination_discipline",
    "clarify_safety",
    "recoverability",
    "timeline_integrity",
)

_RUN_JUDGE_SYSTEM_PROMPT = """\
You are a strict judge for advisor orchestrator gold-eval runs.
Use ONLY the provided JSON evidence. Do NOT use external knowledge.

Return JSON only:
{
  "overall_score": 0-100,
  "status": "good" | "watch" | "bad",
  "highlights": ["..."],
  "risks": ["..."],
  "recommendations": ["..."]
}
"""


class SmoothRPMThrottler:
    """Simple global smoother to avoid bursty spikes."""

    def __init__(self, max_rpm_total: int) -> None:
        rpm = max(1, int(max_rpm_total))
        self._min_interval_seconds = 60.0 / float(rpm)
        self._next_ready = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_ready - now)
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_ready = now + self._min_interval_seconds


def build_eval_llm_client(*, max_rpm_total: int) -> LLMClient:
    """Build an evaluation-dedicated LLM client capped by total RPM."""
    capped_total = int(max_rpm_total)
    if capped_total > 200:
        raise ValueError("max_rpm_total must be <= 200")
    if capped_total <= 0:
        raise ValueError("max_rpm_total must be > 0")

    keys = settings.zai_api_keys
    if not keys and settings.ZAI_API_KEY:
        keys = [settings.ZAI_API_KEY]
    if not keys:
        raise ValueError("No LLM API key configured for judge")

    per_endpoint_rpm = max(1, math.floor(capped_total / len(keys)))
    return LLMClient(
        api_key=keys[0],
        api_keys=keys,
        base_url=settings.ZAI_BASE_URL,
        model=settings.ZAI_MODEL,
        max_rpm=per_endpoint_rpm,
    )


@dataclass
class AdvisorJudgeCaseResult:
    case_id: str
    case_score: float
    dimension_scores: dict[str, float] = field(default_factory=dict)
    strengths: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    recommendation: str = ""
    error: str | None = None


@dataclass
class AdvisorJudgePassResult:
    pass_name: str
    eval_run_id: str
    status: str
    case_results: list[AdvisorJudgeCaseResult] = field(default_factory=list)
    case_count: int = 0
    avg_case_score: float = 0.0
    errors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AdvisorJudgeRunSummary:
    run_id: str
    eval_run_id: str
    status: str
    overall_score: float
    score_uplift: float
    highlights: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AdvisorOrchestratorJudge:
    """Case-level and run-level judge for advisor orchestrator eval."""

    def __init__(
        self,
        llm: LLMClient | None = None,
        *,
        max_rpm_total: int = 180,
        concurrency: int = 2,
        temperature: float = 0.1,
        max_tokens: int = 1200,
    ) -> None:
        self._llm = llm or build_eval_llm_client(max_rpm_total=max_rpm_total)
        self._concurrency = max(1, int(concurrency))
        self._temperature = float(temperature)
        self._max_tokens = max(128, int(max_tokens))
        if int(max_rpm_total) <= 0 or int(max_rpm_total) > 200:
            raise ValueError("max_rpm_total must be > 0 and <= 200")
        self._throttler = SmoothRPMThrottler(max_rpm_total=max_rpm_total)

    async def evaluate_cases(
        self,
        *,
        pass_name: str,
        eval_run_id: str,
        case_payloads: list[dict[str, Any]],
        run_metadata: dict[str, Any] | None = None,
    ) -> AdvisorJudgePassResult:
        semaphore = asyncio.Semaphore(self._concurrency)
        errors: list[dict[str, Any]] = []

        async def judge_one(payload: dict[str, Any]) -> AdvisorJudgeCaseResult:
            async with semaphore:
                case_id = str(payload.get("case_id", "unknown"))
                case_type = _resolve_case_type(payload)
                request_payload = {
                    "pass_name": pass_name,
                    "run_metadata": run_metadata or {},
                    "case": payload,
                }
                default = _deterministic_case_result(payload, case_type=case_type, error=None)
                try:
                    await self._throttler.acquire()
                    response = await self._llm.complete_json(
                        [
                            {
                                "role": "system",
                                "content": _build_case_system_prompt(case_type=case_type),
                            },
                            {
                                "role": "user",
                                "content": json.dumps(request_payload, ensure_ascii=False),
                            },
                        ],
                        temperature=self._temperature,
                        max_tokens=self._max_tokens,
                        caller="eval.advisor.orchestrator.judge.case",
                    )
                    return _normalise_case_result(
                        case_id=case_id,
                        response=response,
                        default=default,
                    )
                except Exception as exc:
                    errors.append({"stage": "judge_case", "case_id": case_id, "error": str(exc)})
                    return _deterministic_case_result(payload, case_type=case_type, error=str(exc))

        suffix = self._llm.set_caller_suffix(eval_run_id)
        try:
            results = await asyncio.gather(*[judge_one(item) for item in case_payloads])
        finally:
            self._llm.reset_caller_suffix(suffix)

        case_count = len(results)
        avg_case_score = (
            sum(item.case_score for item in results) / case_count
            if case_count > 0
            else 0.0
        )
        return AdvisorJudgePassResult(
            pass_name=pass_name,
            eval_run_id=eval_run_id,
            status="partial" if errors else "ok",
            case_results=results,
            case_count=case_count,
            avg_case_score=round(avg_case_score, 4),
            errors=errors,
        )

    async def evaluate_run(
        self,
        *,
        run_id: str,
        eval_run_id: str,
        pass_summary: dict[str, Any],
        aggregate_metrics: dict[str, Any],
    ) -> AdvisorJudgeRunSummary:
        payload = {
            "run_id": run_id,
            "pass_summary": pass_summary,
            "aggregate_metrics": aggregate_metrics,
        }
        suffix = self._llm.set_caller_suffix(eval_run_id)
        try:
            await self._throttler.acquire()
            response = await self._llm.complete_json(
                [
                    {"role": "system", "content": _RUN_JUDGE_SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=self._temperature,
                max_tokens=self._max_tokens,
                caller="eval.advisor.orchestrator.judge.run",
            )
        except Exception as exc:
            self._llm.reset_caller_suffix(suffix)
            return _fallback_run_summary(
                run_id=run_id,
                eval_run_id=eval_run_id,
                pass_summary=pass_summary,
                aggregate_metrics=aggregate_metrics,
                error=str(exc),
            )
        self._llm.reset_caller_suffix(suffix)
        return _normalise_run_summary(
            run_id=run_id,
            eval_run_id=eval_run_id,
            response=response,
            pass_summary=pass_summary,
            aggregate_metrics=aggregate_metrics,
        )


def _build_case_system_prompt(*, case_type: str) -> str:
    dims = _REEDIT_DIMENSIONS if case_type == "reedit" else _BASE_DIMENSIONS
    dims_rows = "\n".join(f"- {d}" for d in dims)
    dims_json_rows = ",\n".join(f'    "{d}": 0-100' for d in dims)
    return (
        "You are a strict judge for advisor orchestrator quality.\n"
        "Use ONLY the provided JSON evidence. Do NOT use external knowledge.\n\n"
        f"Score these dimensions from 0 to 100:\n{dims_rows}\n\n"
        "Then produce an overall case_score (0-100), concise strengths, concise risks,\n"
        "and one recommendation.\n\n"
        "Return JSON only:\n"
        "{\n"
        '  "case_score": 0-100,\n'
        '  "dimension_scores": {\n'
        f"{dims_json_rows}\n"
        "  },\n"
        '  "strengths": ["..."],\n'
        '  "risks": ["..."],\n'
        '  "recommendation": "..."\n'
        "}"
    )


def _resolve_case_type(payload: dict[str, Any]) -> str:
    raw = str(payload.get("case_type", "")).strip().lower()
    if raw == "reedit":
        return "reedit"
    return "orchestrator"


def _deterministic_case_result(
    payload: dict[str, Any],
    *,
    case_type: str,
    error: str | None,
) -> AdvisorJudgeCaseResult:
    case_id = str(payload.get("case_id", "unknown"))
    checks = payload.get("deterministic_checks")
    if not isinstance(checks, dict):
        checks = {}

    if case_type == "reedit":
        dimensions = {
            "route_correctness": _score_from_optional_bools(
                [checks.get("overwrite_success"), checks.get("capability_match")]
            ),
            "coordination_discipline": _score_from_optional_bools(
                [checks.get("truncation_correct")]
            ),
            "clarify_safety": _score_from_optional_bools(
                [checks.get("contract_ok")]
            ),
            "recoverability": _score_from_optional_bools(
                [checks.get("history_consistent"), checks.get("contract_ok")]
            ),
            "timeline_integrity": _score_from_optional_bools(
                [checks.get("truncation_correct"), checks.get("history_consistent")]
            ),
        }
    else:
        route_checks = [checks.get("primary_hit"), checks.get("clarify_correct")]
        coordination_checks = [checks.get("max_execution_ok"), checks.get("pending_reason_ok")]
        clarify_checks = [checks.get("clarify_correct"), checks.get("must_clarify_alignment")]
        recoverability_checks = [checks.get("recoverable_ok"), checks.get("error_contract_ok")]
        dimensions = {
            "route_correctness": _score_from_optional_bools(route_checks),
            "coordination_discipline": _score_from_optional_bools(coordination_checks),
            "clarify_safety": _score_from_optional_bools(clarify_checks),
            "recoverability": _score_from_optional_bools(recoverability_checks),
        }

    case_score = round(sum(dimensions.values()) / len(dimensions), 2)
    return AdvisorJudgeCaseResult(
        case_id=case_id,
        case_score=case_score,
        dimension_scores=dimensions,
        strengths=["Deterministic fallback judge applied."],
        risks=(["LLM judge unavailable."] if error else []),
        recommendation="Review failed deterministic checks before changing prompts.",
        error=error,
    )


def _normalise_case_result(
    *,
    case_id: str,
    response: dict[str, Any],
    default: AdvisorJudgeCaseResult,
) -> AdvisorJudgeCaseResult:
    if not isinstance(response, dict):
        return default

    raw_dimensions = response.get("dimension_scores")
    dimension_scores: dict[str, float] = dict(default.dimension_scores)
    if isinstance(raw_dimensions, dict):
        for key in default.dimension_scores.keys():
            if key in raw_dimensions:
                dimension_scores[key] = _bound_score(raw_dimensions.get(key))

    case_score = _bound_score(response.get("case_score", default.case_score))
    if case_score <= 0 and any(dimension_scores.values()):
        case_score = round(sum(dimension_scores.values()) / len(dimension_scores), 2)

    strengths = _as_str_list(response.get("strengths")) or default.strengths
    risks = _as_str_list(response.get("risks")) or default.risks
    recommendation = str(response.get("recommendation") or default.recommendation).strip()
    return AdvisorJudgeCaseResult(
        case_id=case_id,
        case_score=case_score,
        dimension_scores=dimension_scores,
        strengths=strengths,
        risks=risks,
        recommendation=recommendation,
        error=None,
    )


def _normalise_run_summary(
    *,
    run_id: str,
    eval_run_id: str,
    response: dict[str, Any],
    pass_summary: dict[str, Any],
    aggregate_metrics: dict[str, Any],
) -> AdvisorJudgeRunSummary:
    if not isinstance(response, dict):
        return _fallback_run_summary(
            run_id=run_id,
            eval_run_id=eval_run_id,
            pass_summary=pass_summary,
            aggregate_metrics=aggregate_metrics,
            error="Non-dict run judge response",
        )

    overall = _bound_score(response.get("overall_score", pass_summary.get("avg_case_score", 0.0)))
    status = str(response.get("status", "")).strip().lower()
    if status not in {"good", "watch", "bad"}:
        status = _status_from_score(overall)

    merged_metrics = aggregate_metrics.get("merged_metrics")
    baseline_source = merged_metrics if isinstance(merged_metrics, dict) else aggregate_metrics
    baseline = _safe_float(baseline_source.get("deterministic_overall_score"), default=0.0)
    uplift = round(overall - baseline, 4)
    return AdvisorJudgeRunSummary(
        run_id=run_id,
        eval_run_id=eval_run_id,
        status=status,
        overall_score=overall,
        score_uplift=uplift,
        highlights=_as_str_list(response.get("highlights")),
        risks=_as_str_list(response.get("risks")),
        recommendations=_as_str_list(response.get("recommendations")),
        error=None,
    )


def _fallback_run_summary(
    *,
    run_id: str,
    eval_run_id: str,
    pass_summary: dict[str, Any],
    aggregate_metrics: dict[str, Any],
    error: str,
) -> AdvisorJudgeRunSummary:
    baseline = _safe_float(pass_summary.get("avg_case_score"), default=0.0)
    merged_metrics = aggregate_metrics.get("merged_metrics")
    deterministic_source = merged_metrics if isinstance(merged_metrics, dict) else aggregate_metrics
    deterministic = _safe_float(deterministic_source.get("deterministic_overall_score"), default=0.0)
    overall = round(baseline if baseline > 0 else deterministic, 2)
    return AdvisorJudgeRunSummary(
        run_id=run_id,
        eval_run_id=eval_run_id,
        status=_status_from_score(overall),
        overall_score=overall,
        score_uplift=round(overall - deterministic, 4),
        highlights=["Run summary derived from deterministic fallback."],
        risks=["LLM run-level judge unavailable."],
        recommendations=["Retry with healthy LLM connectivity for full semantic judging."],
        error=error,
    )


def _score_from_optional_bools(values: list[Any]) -> float:
    normalized = [v for v in values if isinstance(v, bool)]
    if not normalized:
        return 100.0
    passed = sum(1 for item in normalized if item)
    return round((passed / len(normalized)) * 100.0, 2)


def _bound_score(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return 0.0
    score = max(0.0, min(100.0, score))
    return round(score, 2)


def _status_from_score(score: float) -> str:
    if score >= 85:
        return "good"
    if score >= 70:
        return "watch"
    return "bad"


def _as_str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def _safe_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
