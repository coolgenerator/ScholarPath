"""Intent classification for the chat agent."""

from __future__ import annotations

import enum
import logging
from typing import Any

from scholarpath.llm.client import LLMClient
from scholarpath.llm.prompts import (
    INTENT_CLASSIFICATION_PROMPT,
    format_intent_classification,
)

logger = logging.getLogger(__name__)


class IntentType(str, enum.Enum):
    """Supported user intent categories."""

    PROFILE_INTAKE = "profile_intake"
    SCHOOL_QUERY = "school_query"
    STRATEGY_ADVICE = "strategy_advice"
    OFFER_DECISION = "offer_decision"
    WHAT_IF = "what_if"
    EMOTIONAL_SUPPORT = "emotional_support"
    RECOMMENDATION = "recommendation"
    GENERAL = "general"


# Map raw LLM output labels to enum members.
_LABEL_MAP: dict[str, IntentType] = {v.value: v for v in IntentType}


async def classify_intent(
    llm: LLMClient,
    message: str,
    context: dict[str, Any] | None = None,
) -> tuple[IntentType, float]:
    """Classify a user message into an intent category using the LLM.

    Parameters
    ----------
    llm:
        LLM client instance.
    message:
        The latest user message to classify.
    context:
        Optional recent conversation context for disambiguation.

    Returns
    -------
    tuple[IntentType, float]
        The classified intent and a confidence score in [0, 1].
    """
    context_str: str | None = None
    if context:
        # Summarise recent context as a string for the prompt
        recent = context.get("recent_messages", "")
        if recent:
            context_str = str(recent)

    user_prompt = format_intent_classification(message, context=context_str)
    messages = [
        {"role": "system", "content": INTENT_CLASSIFICATION_PROMPT},
        {"role": "user", "content": user_prompt},
    ]

    try:
        result = await llm.complete_json(messages, temperature=0.1, max_tokens=256)
        raw_intent = result.get("intent", "general")
        confidence = float(result.get("confidence", 0.5))

        intent = _LABEL_MAP.get(raw_intent, IntentType.GENERAL)
        confidence = max(0.0, min(1.0, confidence))

        logger.debug(
            "Intent classified: %s (confidence=%.2f) for message: %.60s",
            intent.value,
            confidence,
            message,
        )
        return intent, confidence

    except Exception:
        logger.warning("Intent classification failed; defaulting to GENERAL", exc_info=True)
        return IntentType.GENERAL, 0.0
