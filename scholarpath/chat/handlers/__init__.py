"""Chat agent handler utilities."""

from scholarpath.chat.handlers.guided_intake import handle_guided_intake
from scholarpath.chat.handlers.profile_ops import (
    PENDING_PROFILE_PATCH_KEY,
    apply_pending_profile_patch,
    build_profile_snapshot,
    clear_pending_profile_patch,
    create_profile_patch_proposal,
    resolve_profile_update_gate,
)

__all__ = [
    "handle_guided_intake",
    "PENDING_PROFILE_PATCH_KEY",
    "resolve_profile_update_gate",
    "build_profile_snapshot",
    "create_profile_patch_proposal",
    "apply_pending_profile_patch",
    "clear_pending_profile_patch",
]
