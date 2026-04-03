"""LLM judge helpers for advisor orchestrator evaluation."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from scholarpath.llm import LLMClient, get_llm_client

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AdvisorJudgeCaseResult:
    case_id: str
    case_score: float
    route_correct: bool
    output_quality: float
    notes: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AdvisorJudgeRunSummary:
    status: str
    overall_score: float
    recommendations: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AdvisorOrchestratorJudge:
    """LLM-as-judge layer for advisor eval reports."""

    def __init__(
        self,
        llm: LLMClient | None = None,
        *,
        temperature: float = 0.1,
        case_max_tokens: int = 900,
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
        lane: str,
        case_payload: dict[str, Any],
    ) -> AdvisorJudgeCaseResult:
        prompt = {
            "task": "Judge advisor orchestrator output quality and route correctness.",
            "lane": lane,
            "case": case_payload,
            "output_schema": {
                "case_score": "0-100",
                "route_correct": "bool",
                "output_quality": "0-1",
                "notes": "string",
            },
        }
        try:
            judged = await self._llm.complete_json(
                [
                    {"role": "system", "content": "You are a strict QA judge for advisor routing."},
                    {"role": "user", "content": str(prompt)},
                ],
                temperature=self._temperature,
                max_tokens=self._case_max_tokens,
                caller=f"eval.advisor.live.judge.case#{run_id}",
            )
        except Exception as exc:
            logger.warning("Advisor judge case failed: %s", exc)
            return AdvisorJudgeCaseResult(
                case_id=str(case_payload.get("case_id", "")),
                case_score=0.0,
                route_correct=False,
                output_quality=0.0,
                notes="judge_call_failed",
                error=str(exc),
            )

        return AdvisorJudgeCaseResult(
            case_id=str(case_payload.get("case_id", "")),
            case_score=float(judged.get("case_score") or 0.0),
            route_correct=bool(judged.get("route_correct")),
            output_quality=float(judged.get("output_quality") or 0.0),
            notes=str(judged.get("notes") or ""),
            error=None,
        )

    async def judge_run(
        self,
        *,
        run_id: str,
        lane: str,
        case_results: list[AdvisorJudgeCaseResult],
        metrics: dict[str, Any],
    ) -> AdvisorJudgeRunSummary:
        avg_score = (
            sum(item.case_score for item in case_results) / len(case_results)
            if case_results
            else 0.0
        )
        prompt = {
            "lane": lane,
            "avg_case_score": avg_score,
            "case_count": len(case_results),
            "metrics": metrics,
            "task": "Return JSON {overall_score:0-100,recommendations:[string]}",
        }
        try:
            judged = await self._llm.complete_json(
                [
                    {"role": "system", "content": "Summarize advisor evaluation quality."},
                    {"role": "user", "content": str(prompt)},
                ],
                temperature=self._temperature,
                max_tokens=self._run_max_tokens,
                caller=f"eval.advisor.live.judge.run#{run_id}",
            )
        except Exception as exc:
            logger.warning("Advisor judge run failed: %s", exc)
            return AdvisorJudgeRunSummary(
                status="partial",
                overall_score=avg_score,
                recommendations=[],
                errors=[str(exc)],
            )

        recs = judged.get("recommendations")
        if not isinstance(recs, list):
            recs = []
        return AdvisorJudgeRunSummary(
            status="ok",
            overall_score=float(judged.get("overall_score") or avg_score),
            recommendations=[str(item) for item in recs],
            errors=[],
        )
