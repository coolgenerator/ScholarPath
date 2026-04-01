"""LLM-as-judge utilities for DeepSearch live evaluation."""

from __future__ import annotations

import asyncio
import math
import json
from dataclasses import asdict, dataclass, field
from typing import Any

from scholarpath.llm import LLMClient, get_llm_client
from scholarpath.search.canonical_merge import coerce_numeric, normalise_variable_name

_SCHOOL_JUDGE_SYSTEM_PROMPT = """\
You are a strict evaluation judge for DeepSearch quality.
Use ONLY the provided evidence JSON. Do NOT use external knowledge.

Task:
1) Evaluate each required field for whether the fact is acceptable.
2) Produce a school-level score from 0 to 100.
3) Return concise strengths, weaknesses, and one recommendation.

Return JSON only:
{
  "school_score": 0-100,
  "field_judgements": [
    {
      "variable_name": "field_name",
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
You are a strict evaluation judge for DeepSearch live-eval runs.
Use ONLY the provided run evidence JSON. Do NOT use external knowledge.

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


@dataclass
class JudgeSchoolResult:
    school_name: str
    matched_school: str | None
    school_score: float
    field_pass_rate: float
    field_judgements: list[dict[str, Any]] = field(default_factory=list)
    strengths: list[str] = field(default_factory=list)
    weaknesses: list[str] = field(default_factory=list)
    recommendation: str = ""
    error: str | None = None


@dataclass
class JudgePassResult:
    pass_name: str
    eval_run_id: str
    status: str
    school_results: list[JudgeSchoolResult] = field(default_factory=list)
    school_count: int = 0
    avg_school_score: float = 0.0
    field_pass_rate: float = 0.0
    errors: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class JudgeRunSummary:
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


class DeepSearchLiveJudge:
    """Run school-level and run-level LLM-as-judge assessments."""

    def __init__(
        self,
        llm: LLMClient | None = None,
        *,
        concurrency: int = 2,
        temperature: float = 0.1,
        max_tokens: int = 1200,
    ) -> None:
        self._llm = llm or get_llm_client()
        self._concurrency = max(1, concurrency)
        self._temperature = temperature
        self._max_tokens = max(128, max_tokens)

    async def evaluate_pass(
        self,
        *,
        pass_name: str,
        eval_run_id: str,
        school_cases: list[dict[str, Any]],
        schools_payload: list[dict[str, Any]],
        pass_metadata: dict[str, Any],
        required_fields_override: list[str] | None = None,
    ) -> JudgePassResult:
        school_index = _index_school_payload(schools_payload)
        semaphore = asyncio.Semaphore(self._concurrency)
        errors: list[dict[str, Any]] = []

        async def judge_one(case: dict[str, Any]) -> JudgeSchoolResult:
            async with semaphore:
                school_name = str(case.get("school_name", "")).strip()
                aliases = [
                    str(v).strip()
                    for v in case.get("aliases", [])
                    if str(v).strip()
                ]
                required_fields = required_fields_override or list(
                    case.get("required_fields", []),
                )
                required_fields = sorted(
                    {
                        normalise_variable_name(field)
                        for field in required_fields
                        if field
                    },
                )
                rules = {
                    normalise_variable_name(field): dict(rule)
                    for field, rule in (case.get("rules", {}) or {}).items()
                }

                matched = _find_school_payload(school_name, aliases, school_index)
                school_data = matched.get("data", {}) if matched else {}
                field_inputs = _build_field_inputs(
                    required_fields=required_fields,
                    school_data=school_data,
                    rules=rules,
                )

                request_payload = {
                    "pass_name": pass_name,
                    "school_name": school_name,
                    "matched_school": matched.get("name") if matched else None,
                    "pass_metadata": pass_metadata,
                    "fields": field_inputs,
                }

                try:
                    response = await self._llm.complete_json(
                        [
                            {"role": "system", "content": _SCHOOL_JUDGE_SYSTEM_PROMPT},
                            {
                                "role": "user",
                                "content": json.dumps(request_payload, ensure_ascii=False),
                            },
                        ],
                        temperature=self._temperature,
                        max_tokens=self._max_tokens,
                        caller="eval.deepsearch.judge.school",
                    )
                    return _normalise_school_result(
                        school_name=school_name,
                        matched_school=matched.get("name") if matched else None,
                        required_fields=required_fields,
                        field_inputs=field_inputs,
                        response=response,
                        error=None,
                    )
                except Exception as exc:
                    errors.append(
                        {
                            "stage": "judge_school",
                            "school": school_name,
                            "error": str(exc),
                        },
                    )
                    return _normalise_school_result(
                        school_name=school_name,
                        matched_school=matched.get("name") if matched else None,
                        required_fields=required_fields,
                        field_inputs=field_inputs,
                        response={},
                        error=str(exc),
                    )

        suffix_token = self._llm.set_caller_suffix(eval_run_id)
        try:
            school_results = await asyncio.gather(
                *[judge_one(case) for case in school_cases],
            )
        finally:
            self._llm.reset_caller_suffix(suffix_token)

        school_count = len(school_results)
        avg_school_score = (
            sum(item.school_score for item in school_results) / school_count
            if school_count > 0
            else 0.0
        )
        total_fields = sum(len(item.field_judgements) for item in school_results)
        passed_fields = sum(
            1
            for item in school_results
            for verdict in item.field_judgements
            if bool(verdict.get("pass", False))
        )
        field_pass_rate = (
            passed_fields / total_fields
            if total_fields > 0
            else 0.0
        )
        status = "ok"
        if errors:
            status = "partial"
        return JudgePassResult(
            pass_name=pass_name,
            eval_run_id=eval_run_id,
            status=status,
            school_results=school_results,
            school_count=school_count,
            avg_school_score=round(avg_school_score, 4),
            field_pass_rate=round(field_pass_rate, 4),
            errors=errors,
        )

    async def evaluate_run(
        self,
        *,
        run_id: str,
        eval_run_id: str,
        pass1_summary: dict[str, Any],
        pass2_summary: dict[str, Any] | None,
        aggregate_metrics: dict[str, Any],
    ) -> JudgeRunSummary:
        payload = {
            "run_id": run_id,
            "pass1_summary": pass1_summary,
            "pass2_summary": pass2_summary,
            "aggregate_metrics": aggregate_metrics,
        }
        suffix_token = self._llm.set_caller_suffix(eval_run_id)
        try:
            response = await self._llm.complete_json(
                [
                    {"role": "system", "content": _RUN_JUDGE_SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=self._temperature,
                max_tokens=self._max_tokens,
                caller="eval.deepsearch.judge.run",
            )
        except Exception as exc:
            self._llm.reset_caller_suffix(suffix_token)
            return _fallback_run_summary(
                run_id=run_id,
                eval_run_id=eval_run_id,
                pass1_summary=pass1_summary,
                pass2_summary=pass2_summary,
                error=str(exc),
            )
        self._llm.reset_caller_suffix(suffix_token)

        return _normalise_run_summary(
            run_id=run_id,
            eval_run_id=eval_run_id,
            response=response,
            pass1_summary=pass1_summary,
            pass2_summary=pass2_summary,
        )


def _index_school_payload(
    schools_payload: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for school in schools_payload:
        names: list[str] = []
        name = school.get("name")
        if name:
            names.append(str(name))
        aliases = school.get("aliases")
        if isinstance(aliases, list):
            names.extend([str(v) for v in aliases if v])
        for item in names:
            key = item.strip().lower()
            if key:
                index.setdefault(key, school)
    return index


def _find_school_payload(
    school_name: str,
    aliases: list[str],
    school_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    for candidate in [school_name, *aliases]:
        key = candidate.strip().lower()
        if not key:
            continue
        hit = school_index.get(key)
        if hit is not None:
            return hit
    return {}


def _build_field_inputs(
    *,
    required_fields: list[str],
    school_data: dict[str, Any],
    rules: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_data = {
        normalise_variable_name(field): value
        for field, value in school_data.items()
    }
    fields: list[dict[str, Any]] = []
    for field in required_fields:
        node = normalized_data.get(field)
        value = None
        source = None
        confidence = None
        if isinstance(node, dict):
            value = node.get("value")
            source = node.get("source")
            confidence = node.get("confidence")
        else:
            value = node

        has_value = value is not None and str(value).strip() != ""
        rule = dict(rules.get(field, {}))
        deterministic_pass = _evaluate_rule(value=value, rule=rule, field_name=field)
        fields.append(
            {
                "variable_name": field,
                "has_value": has_value,
                "value": value,
                "source": source,
                "confidence": confidence,
                "rule": rule,
                "deterministic_pass": deterministic_pass,
            },
        )
    return fields


def _normalise_school_result(
    *,
    school_name: str,
    matched_school: str | None,
    required_fields: list[str],
    field_inputs: list[dict[str, Any]],
    response: dict[str, Any],
    error: str | None,
) -> JudgeSchoolResult:
    defaults: dict[str, dict[str, Any]] = {}
    for field in field_inputs:
        variable = normalise_variable_name(str(field.get("variable_name") or ""))
        passed = bool(field.get("deterministic_pass", False))
        defaults[variable] = {
            "variable_name": variable,
            "pass": passed,
            "score": 100.0 if passed else 0.0,
            "reason": "deterministic_fallback",
        }

    items = []
    if isinstance(response, dict):
        bucket = response.get("field_judgements")
        if not isinstance(bucket, list):
            bucket = response.get("fields")
        if isinstance(bucket, list):
            items = [item for item in bucket if isinstance(item, dict)]

    for item in items:
        variable = normalise_variable_name(str(item.get("variable_name") or ""))
        if not variable or variable not in defaults:
            continue
        passed = bool(item.get("pass", defaults[variable]["pass"]))
        score = _clamp_score(item.get("score"), default=(100.0 if passed else 0.0))
        reason = str(item.get("reason") or defaults[variable]["reason"]).strip()
        defaults[variable] = {
            "variable_name": variable,
            "pass": passed,
            "score": score,
            "reason": reason,
        }

    field_judgements = [defaults[field] for field in required_fields if field in defaults]
    if not field_judgements:
        field_judgements = list(defaults.values())

    pass_count = sum(1 for item in field_judgements if bool(item.get("pass")))
    field_pass_rate = pass_count / len(field_judgements) if field_judgements else 0.0

    school_score = None
    if isinstance(response, dict):
        school_score = response.get("school_score")
    score = _clamp_score(school_score, default=field_pass_rate * 100.0)

    strengths = _string_list(response.get("strengths") if isinstance(response, dict) else [])
    weaknesses = _string_list(response.get("weaknesses") if isinstance(response, dict) else [])
    recommendation = ""
    if isinstance(response, dict):
        recommendation = str(response.get("recommendation") or "").strip()
    if not recommendation:
        recommendation = "Improve low-confidence or missing required fields first."

    return JudgeSchoolResult(
        school_name=school_name,
        matched_school=matched_school,
        school_score=round(score, 2),
        field_pass_rate=round(field_pass_rate, 4),
        field_judgements=field_judgements,
        strengths=strengths,
        weaknesses=weaknesses,
        recommendation=recommendation,
        error=error,
    )


def _normalise_run_summary(
    *,
    run_id: str,
    eval_run_id: str,
    response: dict[str, Any],
    pass1_summary: dict[str, Any],
    pass2_summary: dict[str, Any] | None,
) -> JudgeRunSummary:
    if not isinstance(response, dict):
        return _fallback_run_summary(
            run_id=run_id,
            eval_run_id=eval_run_id,
            pass1_summary=pass1_summary,
            pass2_summary=pass2_summary,
            error="judge_run_non_dict_response",
        )

    default_uplift = _safe_float(pass2_summary.get("avg_school_score")) - _safe_float(
        pass1_summary.get("avg_school_score"),
    ) if pass2_summary else 0.0
    default_score = _safe_float(pass2_summary.get("avg_school_score")) if pass2_summary else _safe_float(
        pass1_summary.get("avg_school_score"),
    )
    overall_score = _clamp_score(response.get("overall_score"), default=default_score)
    score_uplift = _safe_float(response.get("score_uplift"), default=default_uplift)
    status = str(response.get("status") or "").strip().lower()
    if status not in {"good", "watch", "bad"}:
        if overall_score >= 80:
            status = "good"
        elif overall_score >= 60:
            status = "watch"
        else:
            status = "bad"

    highlights = _string_list(response.get("highlights"))
    risks = _string_list(response.get("risks"))
    recommendations = _string_list(response.get("recommendations"))
    if not recommendations:
        recommendations = ["Continue improving low-recall and high-conflict fields."]

    return JudgeRunSummary(
        run_id=run_id,
        eval_run_id=eval_run_id,
        status=status,
        overall_score=round(overall_score, 2),
        score_uplift=round(score_uplift, 4),
        highlights=highlights,
        risks=risks,
        recommendations=recommendations,
        error=None,
    )


def _fallback_run_summary(
    *,
    run_id: str,
    eval_run_id: str,
    pass1_summary: dict[str, Any],
    pass2_summary: dict[str, Any] | None,
    error: str,
) -> JudgeRunSummary:
    p1_score = _safe_float(pass1_summary.get("avg_school_score"))
    p2_score = _safe_float(pass2_summary.get("avg_school_score")) if pass2_summary else p1_score
    overall = p2_score
    uplift = p2_score - p1_score
    if overall >= 80:
        status = "good"
    elif overall >= 60:
        status = "watch"
    else:
        status = "bad"
    return JudgeRunSummary(
        run_id=run_id,
        eval_run_id=eval_run_id,
        status=status,
        overall_score=round(overall, 2),
        score_uplift=round(uplift, 4),
        highlights=["Fallback summary used due to judge error."],
        risks=["Run-level judge response unavailable."],
        recommendations=["Re-run judge stage and inspect per-school evidence quality."],
        error=error,
    )


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

    return value is not None and str(value).strip() != ""


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


def _clamp_score(value: Any, *, default: float) -> float:
    score = _safe_float(value, default=default)
    return max(0.0, min(100.0, score))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        text = str(item).strip()
        if text:
            normalized.append(text)
    return normalized[:6]
