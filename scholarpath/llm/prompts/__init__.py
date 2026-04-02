"""Prompt templates for ScholarPath LLM calls."""

from scholarpath.llm.prompts.profile_extraction import (
    SYSTEM_PROMPT as PROFILE_EXTRACTION_PROMPT,
    format_user_prompt as format_profile_extraction,
)
from scholarpath.llm.prompts.query_decomposition import (
    SYSTEM_PROMPT as QUERY_DECOMPOSITION_PROMPT,
    format_user_prompt as format_query_decomposition,
)
from scholarpath.llm.prompts.entity_alignment import (
    SYSTEM_PROMPT as ENTITY_ALIGNMENT_PROMPT,
    format_user_prompt as format_entity_alignment,
)
from scholarpath.llm.prompts.conflict_detection import (
    SYSTEM_PROMPT as CONFLICT_DETECTION_PROMPT,
    format_user_prompt as format_conflict_detection,
)
from scholarpath.llm.prompts.school_evaluation import (
    SYSTEM_PROMPT as SCHOOL_EVALUATION_PROMPT,
    format_user_prompt as format_school_evaluation,
)
from scholarpath.llm.prompts.strategy_advice import (
    SYSTEM_PROMPT as STRATEGY_ADVICE_PROMPT,
    format_user_prompt as format_strategy_advice,
)
from scholarpath.llm.prompts.causal_narrative import (
    SYSTEM_PROMPT as CAUSAL_NARRATIVE_PROMPT,
    format_user_prompt as format_causal_narrative,
)
from scholarpath.llm.prompts.go_no_go import (
    SYSTEM_PROMPT as GO_NO_GO_PROMPT,
    format_user_prompt as format_go_no_go,
)

__all__ = [
    "PROFILE_EXTRACTION_PROMPT",
    "format_profile_extraction",
    "QUERY_DECOMPOSITION_PROMPT",
    "format_query_decomposition",
    "ENTITY_ALIGNMENT_PROMPT",
    "format_entity_alignment",
    "CONFLICT_DETECTION_PROMPT",
    "format_conflict_detection",
    "SCHOOL_EVALUATION_PROMPT",
    "format_school_evaluation",
    "STRATEGY_ADVICE_PROMPT",
    "format_strategy_advice",
    "CAUSAL_NARRATIVE_PROMPT",
    "format_causal_narrative",
    "GO_NO_GO_PROMPT",
    "format_go_no_go",
]
