"""Chat agent intent handlers."""

from scholarpath.chat.handlers.guided_intake import handle_guided_intake
from scholarpath.chat.handlers.offer_decision import handle_offer_decision
from scholarpath.chat.handlers.profile_intake import handle_profile_intake
from scholarpath.chat.handlers.profile_ops import (
    PENDING_PROFILE_PATCH_KEY,
    apply_pending_profile_patch,
    build_profile_snapshot,
    clear_pending_profile_patch,
    create_profile_patch_proposal,
    resolve_profile_update_gate,
)
from scholarpath.chat.handlers.recommend import handle_recommendation
from scholarpath.chat.handlers.school_query import handle_school_query
from scholarpath.chat.handlers.strategy import handle_strategy
from scholarpath.chat.handlers.what_if import handle_what_if

__all__ = [
    "handle_guided_intake",
    "handle_offer_decision",
    "handle_profile_intake",
    "PENDING_PROFILE_PATCH_KEY",
    "resolve_profile_update_gate",
    "build_profile_snapshot",
    "create_profile_patch_proposal",
    "apply_pending_profile_patch",
    "clear_pending_profile_patch",
    "handle_recommendation",
    "handle_school_query",
    "handle_strategy",
    "handle_what_if",
]
