"""Source value scoring for DeepSearch V2."""

from __future__ import annotations

from dataclasses import dataclass


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


@dataclass
class SourceValueInput:
    """Inputs used to estimate the utility of a search source."""

    calls: int
    failures: int
    raw_facts: int
    kept_facts: int
    unique_fields: int
    conflicting_facts: int
    estimated_tokens: int
    avg_latency_ms: float


class SourceValueScorer:
    """Compute a normalized source value score in [0, 1]."""

    def score(
        self,
        *,
        payload: SourceValueInput,
        required_field_count: int,
    ) -> float:
        calls = max(payload.calls, 0)
        failures = max(payload.failures, 0)
        raw_facts = max(payload.raw_facts, 0)
        kept_facts = max(payload.kept_facts, 0)
        unique_fields = max(payload.unique_fields, 0)
        conflicting = max(payload.conflicting_facts, 0)
        tokens = max(payload.estimated_tokens, 0)
        avg_latency_ms = max(float(payload.avg_latency_ms), 0.0)

        success_rate = 1.0
        if calls > 0:
            success_rate = _clamp((calls - failures) / calls)

        coverage_gain = _clamp(unique_fields / max(1, required_field_count))
        keep_ratio = _clamp(kept_facts / max(1, raw_facts))
        conflict_rate = _clamp(conflicting / max(1, kept_facts))
        consistency = 1.0 - conflict_rate

        # Normalize cost and latency with soft caps.
        token_efficiency = 1.0 - _clamp(tokens / 20000.0)
        latency_efficiency = 1.0 - _clamp(avg_latency_ms / 6000.0)

        # Weighted linear score:
        # - reward coverage contribution and stability
        # - penalize conflict, low keep ratio, expensive/slow calls
        value = (
            0.34 * coverage_gain
            + 0.22 * keep_ratio
            + 0.18 * consistency
            + 0.12 * success_rate
            + 0.08 * token_efficiency
            + 0.06 * latency_efficiency
        )
        return round(_clamp(value), 4)

    @staticmethod
    def rank(scores: dict[str, float]) -> list[str]:
        """Return sources sorted by value descending."""
        return [
            source
            for source, _ in sorted(
                scores.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]
