"""LLM-as-judge for causal gold evaluation."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from scholarpath.llm import LLMClient, get_llm_client

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CausalJudgeCaseResult:
    case_id: str
    case_score: float
    field_pass_rate: float
    field_judgements: list[dict[str, Any]] = field(default_factory=list)
    recommendation: str = ""
    error: str | None = None


@dataclass(slots=True)
class CausalJudgeRunSummary:
    status: str
    overall_score: float
    field_pass_rate: float
    recommendations: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class CausalGoldJudge:
    """Judge engine constrained to in-run evidence only."""

    def __init__(
        self,
        llm: LLMClient | None = None,
        *,
        temperature: float = 0.1,
        case_max_tokens: int = 1200,
        run_max_tokens: int = 600,
    ) -> None:
        self._llm = llm or get_llm_client()
        self._temperature = temperature
        self._case_max_tokens = case_max_tokens
        self._run_max_tokens = run_max_tokens

    async def judge_case(
        self,
        *,
        run_id: str,
        case_id: str,
        outcomes: dict[str, dict[str, Any]],
    ) -> CausalJudgeCaseResult:
        prompt = {
            "case_id": case_id,
            "outcomes": outcomes,
            "task": (
                "Judge prediction quality by tolerance only. Return strict JSON: "
                "{case_score:0-100, field_judgements:[{outcome_name, pass, score, reason}], "
                "recommendation:string}"
            ),
        }
        try:
            payload = await self._llm.complete_json(
                [
                    {"role": "system", "content": "You are a strict causal eval judge."},
                    {"role": "user", "content": str(prompt)},
                ],
                temperature=self._temperature,
                max_tokens=self._case_max_tokens,
                caller=f"eval.causal.judge.case#{run_id}",
            )
        except Exception as exc:
            logger.warning("Causal judge case failed: %s", exc)
            return CausalJudgeCaseResult(
                case_id=case_id,
                case_score=0.0,
                field_pass_rate=0.0,
                recommendation="Judge call failed",
                error=str(exc),
            )

        judgements = payload.get("field_judgements") if isinstance(payload, dict) else None
        if not isinstance(judgements, list):
            judgements = []
        passed = 0
        for item in judgements:
            if isinstance(item, dict) and bool(item.get("pass")):
                passed += 1
        rate = passed / len(judgements) if judgements else 0.0
        return CausalJudgeCaseResult(
            case_id=case_id,
            case_score=float(payload.get("case_score") or 0.0),
            field_pass_rate=round(rate, 4),
            field_judgements=[item for item in judgements if isinstance(item, dict)],
            recommendation=str(payload.get("recommendation") or ""),
            error=None,
        )

    async def judge_run(
        self,
        *,
        run_id: str,
        pass_name: str,
        case_results: list[CausalJudgeCaseResult],
        metrics: dict[str, Any],
    ) -> CausalJudgeRunSummary:
        payload = {
            "pass_name": pass_name,
            "case_count": len(case_results),
            "avg_case_score": (
                sum(item.case_score for item in case_results) / len(case_results)
                if case_results
                else 0.0
            ),
            "avg_field_pass_rate": (
                sum(item.field_pass_rate for item in case_results) / len(case_results)
                if case_results
                else 0.0
            ),
            "metrics": metrics,
        }
        try:
            judged = await self._llm.complete_json(
                [
                    {"role": "system", "content": "Summarize causal eval quality."},
                    {"role": "user", "content": str(payload)},
                ],
                temperature=self._temperature,
                max_tokens=self._run_max_tokens,
                caller=f"eval.causal.judge.run#{run_id}",
            )
        except Exception as exc:
            logger.warning("Causal judge run failed: %s", exc)
            return CausalJudgeRunSummary(
                status="partial",
                overall_score=payload["avg_case_score"],
                field_pass_rate=payload["avg_field_pass_rate"],
                recommendations=[],
                errors=[str(exc)],
            )

        recs = judged.get("recommendations") if isinstance(judged, dict) else []
        if not isinstance(recs, list):
            recs = []
        return CausalJudgeRunSummary(
            status="ok",
            overall_score=float(judged.get("overall_score") or payload["avg_case_score"]),
            field_pass_rate=float(judged.get("field_pass_rate") or payload["avg_field_pass_rate"]),
            recommendations=[str(item) for item in recs],
            errors=[],
        )
