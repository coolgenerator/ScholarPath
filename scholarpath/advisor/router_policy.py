"""Centralized routing policy for Advisor orchestrator."""

from __future__ import annotations

import re

from scholarpath.advisor.contracts import AdvisorCapability, AdvisorDomain

CONFLICT_GAP_THRESHOLD = 0.12
CONFLICT_SECONDARY_MIN_CONFIDENCE = 0.70

DOMAIN_DESCRIPTIONS: dict[AdvisorDomain, str] = {
    "undergrad": "本科阶段择校/选校/申请策略/学校问答/背景采集",
    "offer": "已有录取 offer 的比较、决策与 what-if",
    "common": "通用聊天、portfolio填写引导、情绪支持与澄清",
}

CONFLICT_GROUP_MAP: dict[AdvisorCapability, str] = {
    "undergrad.profile.intake": "undergrad_profile",
    "undergrad.school.recommend": "undergrad_recommend",
    "undergrad.school.query": "undergrad_query",
    "undergrad.strategy.plan": "undergrad_strategy",
    "offer.compare": "offer_decision",
    "offer.decision": "offer_decision",
    "offer.what_if": "offer_what_if",
    "common.general": "common_general",
    "common.emotional_support": "common_support",
    "common.clarify": "common_clarify",
    "graduate.program.recommend": "future_domain",
    "summer.program.recommend": "future_domain",
}

_AMBIGUOUS_KEYWORDS_ZH: tuple[str, ...] = (
    "不确定",
    "不知道",
    "有点乱",
    "先聊聊",
    "不清楚",
    "迷茫",
)
_AMBIGUOUS_KEYWORDS_EN: tuple[str, ...] = (
    "not sure",
    "don't know",
    "feel lost",
    "confused",
    "no idea",
)


def capability_priority(capability: AdvisorCapability) -> int:
    if capability in {"undergrad.school.recommend", "undergrad.school.query"}:
        return 0
    if capability == "undergrad.strategy.plan":
        return 1
    if capability == "undergrad.profile.intake":
        return 2
    if capability in {"offer.compare", "offer.decision"}:
        return 0
    if capability == "offer.what_if":
        return 1
    if capability in {"common.general", "common.emotional_support"}:
        return 3
    if capability == "common.clarify":
        return 4
    return 5


def contains_ambiguous_expression(message: str) -> bool:
    lowered = message.lower()
    if any(keyword in message for keyword in _AMBIGUOUS_KEYWORDS_ZH):
        return True
    if any(keyword in lowered for keyword in _AMBIGUOUS_KEYWORDS_EN):
        return True
    return False


def is_emotional_message(message: str) -> bool:
    zh_keywords = ("焦虑", "压力", "崩溃", "难受", "害怕", "紧张", "失眠")
    en_keywords = ("anxious", "anxiety", "stressed", "overwhelmed", "burned out", "panic", "depressed")
    lowered = message.lower()
    if any(keyword in message for keyword in zh_keywords):
        return True
    if re.search(r"\bemo\b", lowered):
        return True
    if any(keyword in lowered for keyword in en_keywords):
        return True
    return False


def contains_school_or_offer_signal(message: str) -> bool:
    zh_keywords = ("学校", "选校", "本科", "offer", "录取", "申请", "ed", "ea", "rd")
    en_keywords = ("university", "college", "admission", "offer", "application", "ed", "ea", "rd")
    lowered = message.lower()
    if any(keyword in message for keyword in zh_keywords):
        return True
    if any(keyword in lowered for keyword in en_keywords):
        return True
    return False


def contains_portfolio_signal(message: str) -> bool:
    zh_keywords = ("portfolio", "画像", "简历", "活动", "经历", "奖项", "科研", "文书素材")
    en_keywords = ("portfolio", "resume", "activities", "awards")
    lowered = message.lower()
    if any(keyword in message for keyword in zh_keywords):
        return True
    if any(keyword in lowered for keyword in en_keywords):
        return True
    return False


def contains_smalltalk_signal(message: str) -> bool:
    zh_keywords = ("先聊聊", "聊聊", "先聊一下", "聊一下", "先聊会")
    en_keywords = ("chat", "talk first", "just talk", "quick chat", "small talk")
    lowered = message.lower()
    if any(keyword in message for keyword in zh_keywords):
        return True
    if any(keyword in lowered for keyword in en_keywords):
        return True
    return False


def contains_undergrad_signal(message: str) -> bool:
    zh_keywords = ("本科", "学校", "选校", "推荐", "问答", "策略", "画像")
    en_keywords = ("undergrad", "recommend", "query", "strategy", "profile", "intake")
    lowered = message.lower()
    if any(keyword in message for keyword in zh_keywords):
        return True
    if any(keyword in lowered for keyword in en_keywords):
        return True
    return False


def contains_offer_signal(message: str) -> bool:
    zh_keywords = ("offer", "录取", "对比", "取舍", "决策", "模拟")
    en_keywords = ("offer", "compare", "decision", "what-if", "what if")
    lowered = message.lower()
    if any(keyword in message for keyword in zh_keywords):
        return True
    if any(keyword in lowered for keyword in en_keywords):
        return True
    return False


def signal_domain_from_message(message: str) -> AdvisorDomain | None:
    has_undergrad = contains_undergrad_signal(message)
    has_offer = contains_offer_signal(message)
    if has_undergrad and not has_offer:
        return "undergrad"
    if has_offer and not has_undergrad:
        return "offer"
    return None


def fallback_common_capability(message: str) -> tuple[AdvisorCapability, float]:
    if is_emotional_message(message):
        return "common.emotional_support", 0.90
    if contains_ambiguous_expression(message) and contains_school_or_offer_signal(message):
        return "common.clarify", 0.72
    return "common.general", 0.78

