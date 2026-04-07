from __future__ import annotations

from scholarpath.services.recommendation_scenarios import (
    apply_budget_prefilter,
    build_scenario_pack,
)


def _school(
    *,
    school_id: str,
    name: str,
    overall: float,
    admission: float,
    net_price: int | None,
    academic: float = 0.6,
    financial: float = 0.5,
    career: float = 0.6,
    life: float = 0.6,
    programs: list[str] | None = None,
    state: str = "CA",
    city: str = "Test City",
    campus_setting: str = "urban",
) -> dict:
    return {
        "school_id": school_id,
        "school_name": name,
        "tier": "target",
        "overall_score": overall,
        "admission_probability": admission,
        "sub_scores": {
            "academic": academic,
            "financial": financial,
            "career": career,
            "life": life,
        },
        "school_info": {
            "avg_net_price": net_price,
            "acceptance_rate": 0.35,
            "state": state,
            "city": city,
            "campus_setting": campus_setting,
        },
        "program_names": programs or [],
    }


def test_budget_prefilter_hard_gate_and_top_stretch() -> None:
    schools = [
        _school(school_id="A", name="A", overall=0.80, admission=0.5, net_price=9000),
        _school(school_id="B", name="B", overall=0.90, admission=0.4, net_price=12000),
        _school(school_id="C", name="C", overall=0.70, admission=0.6, net_price=18000),
        _school(school_id="D", name="D", overall=0.60, admission=0.7, net_price=22000),
        _school(school_id="E", name="E", overall=0.95, admission=0.8, net_price=None),
    ]

    selected, meta, excluded = apply_budget_prefilter(
        schools,
        budget_cap=10_000,
        stretch_quota=2,
    )

    assert [row["school_id"] for row in selected] == ["B", "A", "C"]
    assert selected[0]["prefilter_tag"] == "stretch"
    assert selected[1]["prefilter_tag"] == "eligible"
    assert selected[2]["prefilter_tag"] == "stretch"
    assert meta["eligible_count"] == 1
    assert meta["stretch_count"] == 2
    assert meta["excluded_count"] == 2
    assert meta["excluded_reasons_summary"] == {
        "missing_net_price": 1,
        "over_budget": 1,
    }
    assert {row["school_id"] for row in excluded} == {"D", "E"}


def test_scenario_pack_is_deterministic_for_same_input() -> None:
    schools = [
        _school(school_id="A", name="A", overall=0.82, admission=0.65, net_price=9000, programs=["Computer Science"]),
        _school(school_id="B", name="B", overall=0.79, admission=0.72, net_price=9500, programs=["Economics"]),
        _school(school_id="C", name="C", overall=0.67, admission=0.51, net_price=14000, programs=["Biology"]),
    ]
    kwargs = {
        "student_budget_usd": 10_000,
        "student_majors": ["Computer Science"],
        "preferences": {"location": ["urban"]},
    }

    baseline_1, pack_1, _ = build_scenario_pack(schools, **kwargs)
    baseline_2, pack_2, _ = build_scenario_pack(schools, **kwargs)

    assert [row["school_id"] for row in baseline_1] == [row["school_id"] for row in baseline_2]
    assert [row["id"] for row in pack_1["scenarios"]] == [
        "budget_first",
        "risk_first",
        "major_first",
        "geo_first",
        "roi_first",
    ]
    for idx, scenario in enumerate(pack_1["scenarios"]):
        first_ids = [row["school_id"] for row in scenario["schools"]]
        second_ids = [row["school_id"] for row in pack_2["scenarios"][idx]["schools"]]
        assert first_ids == second_ids


def test_major_first_promotes_major_match_school() -> None:
    schools = [
        _school(
            school_id="cs",
            name="CS School",
            overall=0.62,
            admission=0.56,
            net_price=9800,
            programs=["Computer Science", "Data Science"],
        ),
        _school(
            school_id="lit",
            name="Literature School",
            overall=0.84,
            admission=0.74,
            net_price=9700,
            programs=["English Literature"],
        ),
    ]

    baseline, pack, _ = build_scenario_pack(
        schools,
        student_budget_usd=10_000,
        student_majors=["Computer Science"],
        preferences={},
    )

    baseline_top = baseline[0]["school_id"]
    assert baseline_top == "lit"

    major_first = next(item for item in pack["scenarios"] if item["id"] == "major_first")
    assert major_first["schools"][0]["school_id"] == "cs"
