"""Helpers for choosing assistant response language from the latest message."""

from __future__ import annotations

import re
from typing import Literal

ResponseLanguage = Literal["zh", "en", "mixed"]

_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]")
_LATIN_TOKEN_RE = re.compile(r"[A-Za-z]+(?:[/-][A-Za-z]+)?")
_COMMON_ACRONYMS = {
    "act",
    "ai",
    "ap",
    "ea",
    "ed",
    "faang",
    "gpa",
    "ib",
    "ielts",
    "phd",
    "rd",
    "rea",
    "sat",
    "toefl",
    "ui",
}


def detect_response_language(text: str) -> ResponseLanguage:
    """Infer whether the latest user message is Chinese, English, or mixed."""
    if not text:
        return "en"

    has_cjk = bool(_CJK_RE.search(text))
    latin_tokens = _LATIN_TOKEN_RE.findall(text)
    substantive_latin = [
        token for token in latin_tokens if token.lower() not in _COMMON_ACRONYMS
    ]
    has_latin = bool(substantive_latin)

    if has_cjk and has_latin:
        return "mixed"
    if has_cjk:
        return "zh"
    return "en"


def select_localized_text(
    zh: str,
    en: str,
    language: ResponseLanguage,
    *,
    mixed: str | None = None,
) -> str:
    """Pick localized copy for the latest response language."""
    if language == "zh":
        return zh
    if language == "en":
        return en
    return mixed if mixed is not None else f"{zh}\n{en}"


def language_instruction(language: ResponseLanguage) -> str:
    """Return an explicit response-language instruction for LLM prompts."""
    if language == "zh":
        return "Respond in Chinese."
    if language == "en":
        return "Respond in English."
    return (
        "Respond in a natural mixed Chinese-English style that mirrors the "
        "user's latest message. Do not force the reply into only Chinese or only English."
    )
