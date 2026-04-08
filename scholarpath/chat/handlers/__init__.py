"""Chat agent intent handlers."""

from scholarpath.chat.handlers.guided_intake import handle_guided_intake
from scholarpath.chat.handlers.offer_decision import handle_offer_decision
from scholarpath.chat.handlers.recommend import handle_recommendation
from scholarpath.chat.handlers.school_query import handle_school_query
from scholarpath.chat.handlers.strategy import handle_strategy
from scholarpath.chat.handlers.what_if import handle_what_if

__all__ = [
    "handle_guided_intake",
    "handle_offer_decision",
    "handle_recommendation",
    "handle_school_query",
    "handle_strategy",
    "handle_what_if",
]
