"""Recommendation skill profiles and routing helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RecommendationSkillProfile:
    skill_id: str
    candidate_pool_size: int
    top_n: int
    weights: dict[str, float]
    budget_hard_gate: bool
    stretch_slots: int
    major_boost: float
    geo_boost: float
    risk_mode: str
    min_results: int
    tier_confidence_min_count: int
    missing_field_trigger_threshold: float
    risk_min_tier_counts: dict[str, int]
    major_match_threshold: float
    major_match_min_ratio: float
    geo_match_threshold: float
    geo_match_min_ratio: float
    roi_career_min_mean: float


@dataclass(frozen=True)
class RecommendationSkillConfig:
    version: str
    default_skill_id: str
    skills: dict[str, RecommendationSkillProfile]
    bucket_to_skill: dict[str, str]


_SKILLS_PATH = Path(__file__).resolve().parents[1] / "data" / "recommendation_skill_profiles.json"
_RISK_HINTS = ("safer admission", "risk", "reach/target/safety", "冲刺", "保底")
_BUDGET_HINTS = ("budget", "$", "usd", "net price", "预算", "费用", "学费")
_MAJOR_HINTS = ("major", "program fit", "专业", "匹配")
_GEO_HINTS = ("region", "state", "city", "urban", "suburban", "location", "地域", "城市")
_ROI_HINTS = ("roi", "career", "salary", "就业", "回报")


def _safe_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _safe_float(raw: Any, default: float) -> float:
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


@lru_cache(maxsize=1)
def load_recommendation_skill_config() -> RecommendationSkillConfig:
    payload = json.loads(_SKILLS_PATH.read_text(encoding="utf-8"))
    raw_skills = payload.get("skills") or {}
    skills: dict[str, RecommendationSkillProfile] = {}
    for skill_id, row in raw_skills.items():
        if not isinstance(row, dict):
            continue
        weights_raw = row.get("weights") or {}
        weights = {
            "academic": _safe_float(weights_raw.get("academic"), 0.3),
            "financial": _safe_float(weights_raw.get("financial"), 0.25),
            "career": _safe_float(weights_raw.get("career"), 0.25),
            "life": _safe_float(weights_raw.get("life"), 0.2),
        }
        total = sum(weights.values()) or 1.0
        normalized_weights = {k: v / total for k, v in weights.items()}
        skills[skill_id] = RecommendationSkillProfile(
            skill_id=skill_id,
            candidate_pool_size=_safe_int(row.get("candidate_pool_size"), 80),
            top_n=_safe_int(row.get("top_n"), 15),
            weights=normalized_weights,
            budget_hard_gate=bool(row.get("budget_hard_gate", True)),
            stretch_slots=_safe_int(row.get("stretch_slots"), 3),
            major_boost=_safe_float(row.get("major_boost"), 0.08),
            geo_boost=_safe_float(row.get("geo_boost"), 0.08),
            risk_mode=str(row.get("risk_mode") or "balanced"),
            min_results=_safe_int(row.get("min_results"), 8),
            tier_confidence_min_count=_safe_int(row.get("tier_confidence_min_count"), 6),
            missing_field_trigger_threshold=min(
                1.0,
                max(0.0, _safe_float(row.get("missing_field_trigger_threshold"), 0.35)),
            ),
            risk_min_tier_counts={
                "reach": _safe_int((row.get("risk_min_tier_counts") or {}).get("reach"), 2),
                "target": _safe_int((row.get("risk_min_tier_counts") or {}).get("target"), 4),
                "safety": _safe_int((row.get("risk_min_tier_counts") or {}).get("safety"), 3),
            },
            major_match_threshold=min(
                1.0,
                max(0.0, _safe_float(row.get("major_match_threshold"), 0.6)),
            ),
            major_match_min_ratio=min(
                1.0,
                max(0.0, _safe_float(row.get("major_match_min_ratio"), 0.35)),
            ),
            geo_match_threshold=min(
                1.0,
                max(0.0, _safe_float(row.get("geo_match_threshold"), 0.7)),
            ),
            geo_match_min_ratio=min(
                1.0,
                max(0.0, _safe_float(row.get("geo_match_min_ratio"), 0.4)),
            ),
            roi_career_min_mean=min(
                1.0,
                max(0.0, _safe_float(row.get("roi_career_min_mean"), 0.55)),
            ),
        )

    default_skill = str(payload.get("default_skill_id") or "recommendation.balanced")
    if default_skill not in skills and skills:
        default_skill = next(iter(skills.keys()))

    bucket_to_skill_raw = payload.get("bucket_to_skill") or {}
    bucket_to_skill = {
        str(bucket): str(skill)
        for bucket, skill in bucket_to_skill_raw.items()
        if str(skill) in skills
    }
    return RecommendationSkillConfig(
        version=str(payload.get("version") or "v1"),
        default_skill_id=default_skill,
        skills=skills,
        bucket_to_skill=bucket_to_skill,
    )


def profile_for_skill(skill_id: str | None) -> RecommendationSkillProfile:
    config = load_recommendation_skill_config()
    candidate = (skill_id or "").strip()
    if candidate and candidate in config.skills:
        return config.skills[candidate]
    return config.skills[config.default_skill_id]


def map_bucket_to_skill(bucket: str | None) -> str:
    config = load_recommendation_skill_config()
    key = (bucket or "").strip()
    mapped = config.bucket_to_skill.get(key)
    if mapped and mapped in config.skills:
        return mapped
    return config.default_skill_id


def infer_skill_from_message(message: str | None) -> str:
    text = (message or "").lower()
    config = load_recommendation_skill_config()

    def _contains_any(hints: tuple[str, ...]) -> bool:
        return any(hint in text for hint in hints)

    if _contains_any(_BUDGET_HINTS):
        return "recommendation.budget_first"
    if _contains_any(_RISK_HINTS):
        return "recommendation.risk_first"
    if _contains_any(_MAJOR_HINTS):
        return "recommendation.major_first"
    if _contains_any(_GEO_HINTS):
        return "recommendation.geo_first"
    if _contains_any(_ROI_HINTS):
        return "recommendation.roi_first"
    return config.default_skill_id


def resolve_skill_id(*, explicit_skill_id: str | None, message: str | None, bucket: str | None = None) -> str:
    """Resolve skill with precedence explicit > bucket > message > default."""
    config = load_recommendation_skill_config()
    explicit = (explicit_skill_id or "").strip()
    if explicit and explicit in config.skills:
        return explicit
    if bucket:
        mapped = map_bucket_to_skill(bucket)
        if mapped in config.skills:
            return mapped
    inferred = infer_skill_from_message(message)
    if inferred in config.skills:
        return inferred
    return config.default_skill_id
