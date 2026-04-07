"""LLM-as-judge for recommendation UX gold evaluation (persona A/B)."""

from __future__ import annotations

import logging
import random
from dataclasses import asdict, dataclass, field
from typing import Any

from scholarpath.llm import LLMClient, get_llm_client

logger = logging.getLogger(__name__)

RECOMMENDATION_RUBRIC_DIMENSIONS: tuple[str, ...] = (
    "constraint_compliance",
    "scenario_usefulness",
    "personalization_fit",
    "tradeoff_clarity",
    "actionability",
    "trustworthiness",
)


@dataclass(slots=True)
class RecommendationJudgeCaseResult:
    case_id: str
    scoring_status: str = "scored"  # scored | unscored
    unscored_reason: str | None = None
    winner: str | None = None  # candidate | baseline | tie
    candidate_scores: dict[str, float] = field(default_factory=dict)
    baseline_scores: dict[str, float] = field(default_factory=dict)
    candidate_mean: float | None = None
    baseline_mean: float | None = None
    mean_delta: float | None = None
    confidence: float | None = None
    reason_codes: list[str] = field(default_factory=list)
    notes: str = ""
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RecommendationJudgeRunSummary:
    status: str
    candidate_win_rate: float
    mean_delta_by_dim: dict[str, float]
    overall_user_feel_mean: float
    scored_case_count: int
    unscored_case_count: int
    risks: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def create_unscored_recommendation_case_result(
    *,
    case_id: str,
    reason: str,
    notes: str = "",
) -> RecommendationJudgeCaseResult:
    return RecommendationJudgeCaseResult(
        case_id=case_id,
        scoring_status="unscored",
        unscored_reason=reason,
        winner=None,
        candidate_scores={},
        baseline_scores={},
        candidate_mean=None,
        baseline_mean=None,
        mean_delta=None,
        confidence=None,
        reason_codes=[reason] if reason else [],
        notes=notes,
        error=None,
    )


class RecommendationABJudge:
    """A/B judge for recommendation UX with strict structured output."""

    def __init__(
        self,
        llm: LLMClient | None = None,
        *,
        temperature: float = 0.1,
        case_max_tokens: int = 1200,
        run_max_tokens: int = 700,
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
        baseline_payload: dict[str, Any],
        candidate_payload: dict[str, Any],
    ) -> RecommendationJudgeCaseResult:
        payload_a, payload_b, a_is_candidate = self._shuffle_pair(
            run_id=run_id,
            case_id=case_id,
            baseline_payload=baseline_payload,
            candidate_payload=candidate_payload,
        )
        schema = {
            "type": "object",
            "properties": {
                "winner_option": {
                    "type": "string",
                    "enum": ["option_1", "option_2", "tie"],
                },
                "scores_option_1": {
                    "type": "object",
                    "properties": {dim: {"type": "number"} for dim in RECOMMENDATION_RUBRIC_DIMENSIONS},
                    "required": list(RECOMMENDATION_RUBRIC_DIMENSIONS),
                },
                "scores_option_2": {
                    "type": "object",
                    "properties": {dim: {"type": "number"} for dim in RECOMMENDATION_RUBRIC_DIMENSIONS},
                    "required": list(RECOMMENDATION_RUBRIC_DIMENSIONS),
                },
                "confidence": {"type": "number"},
                "reason_codes": {"type": "array", "items": {"type": "string"}},
                "notes": {"type": "string"},
            },
            "required": [
                "winner_option",
                "scores_option_1",
                "scores_option_2",
                "confidence",
                "reason_codes",
                "notes",
            ],
        }
        prompt = {
            "task": (
                "Compare two recommendation answers for a simulated applicant persona. "
                "Use only the defined rubric dimensions. Score each dimension in [0,5]. "
                "Do not expose chain-of-thought. Return strict JSON only."
            ),
            "rubric_dimensions": list(RECOMMENDATION_RUBRIC_DIMENSIONS),
            "rubric_hint": {
                "constraint_compliance": "Budget hard gate and top-3 stretch compliance must be explicit and correct.",
                "scenario_usefulness": "Five scenarios should be meaningfully differentiated and useful.",
                "personalization_fit": "Response should align with persona profile and constraints.",
                "tradeoff_clarity": "Pros/cons and tradeoffs should be clear and concrete.",
                "actionability": "Next steps should be specific and executable.",
                "trustworthiness": "Evidence quality and uncertainty handling should inspire trust.",
            },
            "case_id": case_id,
            "option_1": payload_a,
            "option_2": payload_b,
        }
        try:
            judged = await self._llm.complete_json(
                [
                    {
                        "role": "system",
                        "content": (
                            "You are a strict evaluator for recommendation UX quality. "
                            "Return only schema-compliant JSON."
                        ),
                    },
                    {"role": "user", "content": str(prompt)},
                ],
                schema=schema,
                temperature=self._temperature,
                max_tokens=self._case_max_tokens,
                caller=f"eval.recommendation.ux.judge.case#{run_id}",
            )
        except Exception as exc:
            logger.warning("Recommendation judge case failed: %s", exc)
            zero = {dim: 0.0 for dim in RECOMMENDATION_RUBRIC_DIMENSIONS}
            return RecommendationJudgeCaseResult(
                case_id=case_id,
                scoring_status="scored",
                unscored_reason=None,
                winner="tie",
                candidate_scores=zero,
                baseline_scores=zero,
                candidate_mean=0.0,
                baseline_mean=0.0,
                mean_delta=0.0,
                confidence=0.0,
                reason_codes=["judge_call_failed"],
                notes="judge_call_failed",
                error=str(exc),
            )

        scores_1 = self._normalize_scores(judged.get("scores_option_1"))
        scores_2 = self._normalize_scores(judged.get("scores_option_2"))
        if a_is_candidate:
            candidate_scores = scores_1
            baseline_scores = scores_2
            winner = self._map_winner_option(
                str(judged.get("winner_option") or "tie"),
                "option_1",
            )
        else:
            candidate_scores = scores_2
            baseline_scores = scores_1
            winner = self._map_winner_option(
                str(judged.get("winner_option") or "tie"),
                "option_2",
            )

        candidate_mean = self._mean(candidate_scores.values())
        baseline_mean = self._mean(baseline_scores.values())
        reason_codes = judged.get("reason_codes")
        if not isinstance(reason_codes, list):
            reason_codes = []
        confidence = self._clamp(float(judged.get("confidence") or 0.0), 0.0, 1.0)

        return RecommendationJudgeCaseResult(
            case_id=case_id,
            scoring_status="scored",
            unscored_reason=None,
            winner=winner,
            candidate_scores=candidate_scores,
            baseline_scores=baseline_scores,
            candidate_mean=round(candidate_mean, 4),
            baseline_mean=round(baseline_mean, 4),
            mean_delta=round(candidate_mean - baseline_mean, 4),
            confidence=round(confidence, 4),
            reason_codes=[str(item) for item in reason_codes if str(item).strip()],
            notes=str(judged.get("notes") or "").strip(),
            error=None,
        )

    async def judge_run(
        self,
        *,
        run_id: str,
        case_results: list[RecommendationJudgeCaseResult],
        metrics: dict[str, Any],
    ) -> RecommendationJudgeRunSummary:
        scored_cases = [
            item for item in case_results
            if item.scoring_status == "scored"
        ]
        unscored_case_count = len(case_results) - len(scored_cases)
        if not scored_cases:
            return RecommendationJudgeRunSummary(
                status="partial",
                candidate_win_rate=0.0,
                mean_delta_by_dim={dim: 0.0 for dim in RECOMMENDATION_RUBRIC_DIMENSIONS},
                overall_user_feel_mean=0.0,
                scored_case_count=0,
                unscored_case_count=unscored_case_count,
                risks=[],
                recommendations=[],
                errors=["no_scored_case_results"],
            )

        candidate_win_rate = sum(1 for item in scored_cases if item.winner == "candidate") / max(
            1,
            len(scored_cases),
        )
        mean_delta_by_dim = {
            dim: round(
                self._mean(
                    item.candidate_scores.get(dim, 0.0) - item.baseline_scores.get(dim, 0.0)
                    for item in scored_cases
                ),
                4,
            )
            for dim in RECOMMENDATION_RUBRIC_DIMENSIONS
        }
        overall_user_feel_mean = round(
            self._mean(
                item.candidate_mean
                for item in scored_cases
                if item.candidate_mean is not None
            ),
            4,
        )

        prompt = {
            "task": (
                "Summarize recommendation UX A/B judge outcomes with concise risks and recommendations. "
                "Do not include chain-of-thought."
            ),
            "metrics": metrics,
            "candidate_win_rate": candidate_win_rate,
            "mean_delta_by_dim": mean_delta_by_dim,
            "overall_user_feel_mean": overall_user_feel_mean,
            "scored_case_count": len(scored_cases),
            "unscored_case_count": unscored_case_count,
        }
        schema = {
            "type": "object",
            "properties": {
                "risks": {"type": "array", "items": {"type": "string"}},
                "recommendations": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["risks", "recommendations"],
        }

        try:
            judged = await self._llm.complete_json(
                [
                    {
                        "role": "system",
                        "content": (
                            "You summarize recommendation eval outcomes for engineers. "
                            "Return concise structured JSON only."
                        ),
                    },
                    {"role": "user", "content": str(prompt)},
                ],
                schema=schema,
                temperature=self._temperature,
                max_tokens=self._run_max_tokens,
                caller=f"eval.recommendation.ux.judge.run#{run_id}",
            )
            risks_raw = judged.get("risks")
            recs_raw = judged.get("recommendations")
            risks = [str(item) for item in risks_raw] if isinstance(risks_raw, list) else []
            recs = [str(item) for item in recs_raw] if isinstance(recs_raw, list) else []
            return RecommendationJudgeRunSummary(
                status="ok",
                candidate_win_rate=round(candidate_win_rate, 4),
                mean_delta_by_dim=mean_delta_by_dim,
                overall_user_feel_mean=overall_user_feel_mean,
                scored_case_count=len(scored_cases),
                unscored_case_count=unscored_case_count,
                risks=risks,
                recommendations=recs,
                errors=[],
            )
        except Exception as exc:
            logger.warning("Recommendation judge run failed: %s", exc)
            return RecommendationJudgeRunSummary(
                status="partial",
                candidate_win_rate=round(candidate_win_rate, 4),
                mean_delta_by_dim=mean_delta_by_dim,
                overall_user_feel_mean=overall_user_feel_mean,
                scored_case_count=len(scored_cases),
                unscored_case_count=unscored_case_count,
                risks=[],
                recommendations=[],
                errors=[str(exc)],
            )

    @staticmethod
    def _shuffle_pair(
        *,
        run_id: str,
        case_id: str,
        baseline_payload: dict[str, Any],
        candidate_payload: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], bool]:
        seed = f"{run_id}:{case_id}"
        rnd = random.Random(seed)
        if rnd.random() < 0.5:
            return candidate_payload, baseline_payload, True
        return baseline_payload, candidate_payload, False

    @staticmethod
    def _normalize_scores(raw: Any) -> dict[str, float]:
        if not isinstance(raw, dict):
            return {dim: 0.0 for dim in RECOMMENDATION_RUBRIC_DIMENSIONS}
        out: dict[str, float] = {}
        for dim in RECOMMENDATION_RUBRIC_DIMENSIONS:
            out[dim] = RecommendationABJudge._clamp(raw.get(dim), 0.0, 5.0)
        return out

    @staticmethod
    def _map_winner_option(winner_option: str, candidate_option: str) -> str:
        winner = winner_option.strip().lower()
        if winner == "tie":
            return "tie"
        if winner == candidate_option:
            return "candidate"
        return "baseline"

    @staticmethod
    def _mean(values: Any) -> float:
        seq = [float(item) for item in values]
        return (sum(seq) / len(seq)) if seq else 0.0

    @staticmethod
    def _clamp(value: Any, low: float, high: float) -> float:
        try:
            out = float(value)
        except (TypeError, ValueError):
            return low
        return max(low, min(high, out))
