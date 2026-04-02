"""LLM-as-judge utilities for causal gold evaluation."""

from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from scholarpath.config import settings
from scholarpath.llm import LLMClient

_CASE_JUDGE_SYSTEM_PROMPT = """\
You are a strict judge for causal prediction quality.
Use ONLY the provided JSON evidence. Do NOT use external knowledge.

Task:
1) Judge each outcome prediction against its gold value and tolerance.
2) Return a case score from 0 to 100.
3) Return strengths, weaknesses, and one recommendation.

Return JSON only:
{
  "case_score": 0-100,
  "field_judgements": [
    {
      "outcome_name": "field_name",
      "pass": true,
      "score": 0-100,
      "reason": "brief reason"
    }
  ],
  "strengths": ["..."],
  "weaknesses": ["..."],
  "recommendation": "..."
}
"""

_RUN_JUDGE_SYSTEM_PROMPT = """\
You are a strict judge for causal gold-eval run quality.
Use ONLY the provided JSON evidence. Do NOT use external knowledge.

Return JSON only:
{
  "overall_score": 0-100,
  "status": "good" | "watch" | "bad",
  "score_uplift": number,
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
class CausalJudgeCaseResult:
    case_id: str
    case_score: float
    field_pass_rate: float
    field_judgements: list[dict[str, Any]] = field(default_factory=list)
    strengths: list[str] = field(default_factory=list)
    weaknesses: list[str] = field(default_factory=list)
    recommendation: str = ""
    error: str | None = None


@dataclass
class CausalJudgePassResult:
    pass_name: str
    eval_run_id: str
    status: str
    case_results: list[CausalJudgeCaseResult] = field(default_factory=list)
    case_count: int = 0
    avg_case_score: float = 0.0
    field_pass_rate: float = 0.0
    errors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CausalJudgeRunSummary:
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


class CausalGoldJudge:
    """Case-level and run-level judge for causal gold evaluation."""

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
        self._throttler = SmoothRPMThrottler(max_rpm_total=max_rpm_total)

    async def evaluate_pass(
        self,
        *,
        pass_name: str,
        eval_run_id: str,
        case_payloads: list[dict[str, Any]],
        pass_metadata: dict[str, Any],
    ) -> CausalJudgePassResult:
        semaphore = asyncio.Semaphore(self._concurrency)
        errors: list[dict[str, Any]] = []

        async def judge_one(payload: dict[str, Any]) -> CausalJudgeCaseResult:
            async with semaphore:
                case_id = str(payload.get("case_id") or "unknown")
                default = _deterministic_case_result(payload, error=None)
                request_payload = {
                    "pass_name": pass_name,
                    "pass_metadata": pass_metadata,
                    "case": payload,
                }
                try:
                    await self._throttler.acquire()
                    response = await self._llm.complete_json(
                        [
                            {"role": "system", "content": _CASE_JUDGE_SYSTEM_PROMPT},
                            {"role": "user", "content": json.dumps(request_payload, ensure_ascii=False)},
                        ],
                        temperature=self._temperature,
                        max_tokens=self._max_tokens,
                        caller="eval.causal.judge.case",
                    )
                    return _normalise_case_result(default=default, response=response)
                except Exception as exc:
                    errors.append(
                        {"stage": "judge_case", "case_id": case_id, "error": str(exc)}
                    )
                    return _deterministic_case_result(payload, error=str(exc))

        suffix = self._llm.set_caller_suffix(eval_run_id)
        try:
            results = await asyncio.gather(*[judge_one(case) for case in case_payloads])
        finally:
            self._llm.reset_caller_suffix(suffix)

        case_count = len(results)
        avg_score = (
            sum(item.case_score for item in results) / case_count
            if case_count > 0
            else 0.0
        )
        total_fields = sum(len(item.field_judgements) for item in results)
        passed_fields = sum(
            1
            for item in results
            for field in item.field_judgements
            if bool(field.get("pass", False))
        )
        field_pass_rate = passed_fields / total_fields if total_fields > 0 else 0.0

        return CausalJudgePassResult(
            pass_name=pass_name,
            eval_run_id=eval_run_id,
            status="partial" if errors else "ok",
            case_results=results,
            case_count=case_count,
            avg_case_score=round(avg_score, 4),
            field_pass_rate=round(field_pass_rate, 4),
            errors=errors,
        )

    async def evaluate_run(
        self,
        *,
        run_id: str,
        eval_run_id: str,
        legacy_summary: dict[str, Any],
        pywhy_summary: dict[str, Any] | None,
        aggregate_metrics: dict[str, Any],
    ) -> CausalJudgeRunSummary:
        payload = {
            "run_id": run_id,
            "legacy_summary": legacy_summary,
            "pywhy_summary": pywhy_summary,
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
                caller="eval.causal.judge.run",
            )
        except Exception as exc:
            self._llm.reset_caller_suffix(suffix)
            return _fallback_run_summary(
                run_id=run_id,
                eval_run_id=eval_run_id,
                legacy_summary=legacy_summary,
                pywhy_summary=pywhy_summary,
                error=str(exc),
            )
        self._llm.reset_caller_suffix(suffix)
        return _normalise_run_summary(
            run_id=run_id,
            eval_run_id=eval_run_id,
            response=response,
            legacy_summary=legacy_summary,
            pywhy_summary=pywhy_summary,
        )


def _deterministic_case_result(
    payload: dict[str, Any],
    *,
    error: str | None,
) -> CausalJudgeCaseResult:
    case_id = str(payload.get("case_id") or "unknown")
    fields = payload.get("fields")
    if not isinstance(fields, list):
        fields = []
    judgements: list[dict[str, Any]] = []
    for item in fields:
        if not isinstance(item, dict):
            continue
        outcome = str(item.get("outcome_name") or "").strip()
        if not outcome:
            continue
        passed = bool(item.get("within_tolerance", False))
        abs_error = _safe_float(item.get("abs_error"), default=1.0)
        tol = max(1e-6, _safe_float(item.get("tolerance"), default=0.1))
        ratio = min(abs_error / tol, 4.0)
        score = max(0.0, min(100.0, 100.0 - ratio * 25.0))
        judgements.append(
            {
                "outcome_name": outcome,
                "pass": passed,
                "score": round(score, 2),
                "reason": "deterministic_fallback",
            }
        )

    total = len(judgements)
    passed = sum(1 for item in judgements if bool(item.get("pass", False)))
    pass_rate = passed / total if total > 0 else 0.0
    return CausalJudgeCaseResult(
        case_id=case_id,
        case_score=round(pass_rate * 100.0, 2),
        field_pass_rate=round(pass_rate, 4),
        field_judgements=judgements,
        strengths=["Deterministic fallback applied."],
        weaknesses=(["LLM judge unavailable."] if error else []),
        recommendation="Focus on outcomes outside tolerance first.",
        error=error,
    )


def _normalise_case_result(
    *,
    default: CausalJudgeCaseResult,
    response: dict[str, Any],
) -> CausalJudgeCaseResult:
    if not isinstance(response, dict):
        return default

    defaults = {
        str(item.get("outcome_name")): dict(item)
        for item in default.field_judgements
        if str(item.get("outcome_name") or "").strip()
    }
    bucket = response.get("field_judgements")
    if not isinstance(bucket, list):
        bucket = response.get("fields")
    if isinstance(bucket, list):
        for item in bucket:
            if not isinstance(item, dict):
                continue
            outcome = str(item.get("outcome_name") or item.get("variable_name") or "").strip()
            if not outcome or outcome not in defaults:
                continue
            passed = bool(item.get("pass", defaults[outcome]["pass"]))
            score = _clamp_score(item.get("score"), default=defaults[outcome]["score"])
            reason = str(item.get("reason") or defaults[outcome]["reason"]).strip()
            defaults[outcome] = {
                "outcome_name": outcome,
                "pass": passed,
                "score": score,
                "reason": reason,
            }

    field_judgements = list(defaults.values())
    pass_rate = (
        sum(1 for item in field_judgements if bool(item.get("pass"))) / len(field_judgements)
        if field_judgements
        else 0.0
    )
    case_score = _clamp_score(response.get("case_score"), default=pass_rate * 100.0)
    strengths = _string_list(response.get("strengths"))
    weaknesses = _string_list(response.get("weaknesses"))
    recommendation = str(response.get("recommendation") or "").strip() or "Improve high-error outcomes first."

    return CausalJudgeCaseResult(
        case_id=default.case_id,
        case_score=round(case_score, 2),
        field_pass_rate=round(pass_rate, 4),
        field_judgements=field_judgements,
        strengths=strengths,
        weaknesses=weaknesses,
        recommendation=recommendation,
        error=default.error,
    )


def _normalise_run_summary(
    *,
    run_id: str,
    eval_run_id: str,
    response: dict[str, Any],
    legacy_summary: dict[str, Any],
    pywhy_summary: dict[str, Any] | None,
) -> CausalJudgeRunSummary:
    if not isinstance(response, dict):
        return _fallback_run_summary(
            run_id=run_id,
            eval_run_id=eval_run_id,
            legacy_summary=legacy_summary,
            pywhy_summary=pywhy_summary,
            error="judge_run_non_dict_response",
        )

    legacy_score = _safe_float(legacy_summary.get("avg_case_score"))
    pywhy_score = _safe_float(pywhy_summary.get("avg_case_score")) if pywhy_summary else legacy_score
    default_uplift = pywhy_score - legacy_score
    default_overall = pywhy_score
    overall = _clamp_score(response.get("overall_score"), default=default_overall)
    uplift = _safe_float(response.get("score_uplift"), default=default_uplift)
    status = str(response.get("status") or "").strip().lower()
    if status not in {"good", "watch", "bad"}:
        if overall >= 80:
            status = "good"
        elif overall >= 60:
            status = "watch"
        else:
            status = "bad"

    return CausalJudgeRunSummary(
        run_id=run_id,
        eval_run_id=eval_run_id,
        status=status,
        overall_score=round(overall, 2),
        score_uplift=round(uplift, 4),
        highlights=_string_list(response.get("highlights")),
        risks=_string_list(response.get("risks")),
        recommendations=_string_list(response.get("recommendations")) or ["Improve highest-error cohorts first."],
        error=None,
    )


def _fallback_run_summary(
    *,
    run_id: str,
    eval_run_id: str,
    legacy_summary: dict[str, Any],
    pywhy_summary: dict[str, Any] | None,
    error: str,
) -> CausalJudgeRunSummary:
    legacy_score = _safe_float(legacy_summary.get("avg_case_score"))
    pywhy_score = _safe_float(pywhy_summary.get("avg_case_score")) if pywhy_summary else legacy_score
    overall = pywhy_score
    uplift = pywhy_score - legacy_score
    if overall >= 80:
        status = "good"
    elif overall >= 60:
        status = "watch"
    else:
        status = "bad"

    return CausalJudgeRunSummary(
        run_id=run_id,
        eval_run_id=eval_run_id,
        status=status,
        overall_score=round(overall, 2),
        score_uplift=round(uplift, 4),
        highlights=["Fallback summary used due to judge error."],
        risks=["Run-level judge response unavailable."],
        recommendations=["Re-run judge stage and inspect case evidence quality."],
        error=error,
    )


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        text = str(item).strip()
        if text:
            out.append(text)
    return out


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp_score(value: Any, *, default: float) -> float:
    score = _safe_float(value, default=default)
    return max(0.0, min(100.0, score))
