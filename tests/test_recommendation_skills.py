from __future__ import annotations

from scholarpath.services.recommendation_skills import (
    infer_skill_from_message,
    load_recommendation_skill_config,
    map_bucket_to_skill,
    resolve_skill_id,
)


def test_skill_config_loads_with_default():
    cfg = load_recommendation_skill_config()
    assert cfg.default_skill_id in cfg.skills
    assert "recommendation.budget_first" in cfg.skills


def test_map_bucket_to_skill():
    assert map_bucket_to_skill("budget_first") == "recommendation.budget_first"
    assert map_bucket_to_skill("unknown") == load_recommendation_skill_config().default_skill_id


def test_resolve_skill_precedence():
    explicit = resolve_skill_id(
        explicit_skill_id="recommendation.geo_first",
        message="I care about budget",
        bucket="budget_first",
    )
    assert explicit == "recommendation.geo_first"

    bucket = resolve_skill_id(
        explicit_skill_id=None,
        message="I care about major fit",
        bucket="roi_first",
    )
    assert bucket == "recommendation.roi_first"


def test_infer_skill_from_message():
    assert infer_skill_from_message("My budget is 10000 USD") == "recommendation.budget_first"
    assert infer_skill_from_message("I want safer admission outcomes") == "recommendation.risk_first"
