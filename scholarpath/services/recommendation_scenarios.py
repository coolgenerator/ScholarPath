"""Deterministic prefilter + multi-scenario ranking for school recommendations."""

from __future__ import annotations

from collections import Counter
from typing import Any


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _to_score(value: Any, *, default: float = 0.5) -> float:
    try:
        return _clip01(float(value))
    except (TypeError, ValueError):
        return default


def _normalise_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _tokenize(value: str) -> set[str]:
    out = set()
    for token in value.replace("/", " ").replace("-", " ").split():
        token = "".join(ch for ch in token if ch.isalnum())
        if len(token) >= 2:
            out.add(token)
    return out


def _major_match_score(student_majors: list[str] | None, program_names: list[str] | None) -> float:
    if not student_majors:
        return 0.5
    if not program_names:
        return 0.5

    program_norm = [_normalise_text(item) for item in program_names if _normalise_text(item)]
    if not program_norm:
        return 0.5
    program_tokens = [_tokenize(item) for item in program_norm]

    major_scores: list[float] = []
    for major_raw in student_majors:
        major = _normalise_text(major_raw)
        if not major:
            continue
        major_tokens = _tokenize(major)
        if not major_tokens:
            continue
        best = 0.0
        for program, p_tokens in zip(program_norm, program_tokens, strict=False):
            if major in program or program in major:
                best = max(best, 1.0)
                continue
            union = major_tokens | p_tokens
            if not union:
                continue
            jaccard = len(major_tokens & p_tokens) / len(union)
            best = max(best, jaccard)
        major_scores.append(best)

    if not major_scores:
        return 0.5
    return _clip01(sum(major_scores) / len(major_scores))


def _geo_fit_score(preferences: dict[str, Any], school: dict[str, Any]) -> float:
    locations = preferences.get("location")
    if not isinstance(locations, list) or not locations:
        return 0.5

    campus_setting = _normalise_text(school.get("school_info", {}).get("campus_setting"))
    school_city = _normalise_text(school.get("school_info", {}).get("city"))
    school_state = _normalise_text(school.get("school_info", {}).get("state"))
    city_state = " ".join(part for part in [school_city, school_state] if part)

    best = 0.35
    for loc_raw in locations:
        loc = _normalise_text(loc_raw)
        if not loc:
            continue
        if loc in {"urban", "suburban", "rural"} and campus_setting:
            if loc == campus_setting:
                best = max(best, 1.0)
            else:
                best = max(best, 0.2)
            continue
        if len(loc) == 2 and loc == school_state:
            best = max(best, 1.0)
            continue
        if loc in city_state and city_state:
            best = max(best, 0.9)
    return _clip01(best)


def _budget_score(net_price: int | None, budget_cap: int | None, financial_score: float) -> float:
    if budget_cap is None or budget_cap <= 0:
        return financial_score
    if net_price is None:
        return 0.0
    if net_price <= budget_cap:
        return 1.0
    overflow = net_price - budget_cap
    ratio = overflow / max(float(budget_cap), 1.0)
    return _clip01(1.0 - ratio)


def _risk_score(admission_probability: float, acceptance_rate: float | None) -> float:
    if acceptance_rate is None:
        return admission_probability
    ar = float(acceptance_rate)
    if ar > 1.0:
        ar = ar / 100.0
    return _clip01(0.7 * admission_probability + 0.3 * _clip01(ar))


def _roi_score(career: float, academic: float, budget: float) -> float:
    return _clip01(0.65 * career + 0.2 * academic + 0.15 * budget)


def _build_excluded_summary(excluded: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(str(item.get("reason", "unknown")) for item in excluded)
    return {key: int(count) for key, count in sorted(counts.items())}


def _normalise_prefilter_tag(entry: dict[str, Any], *, tag: str, is_stretch: bool, budget_cap: int | None) -> dict[str, Any]:
    school_info = entry.get("school_info", {})
    net_price = school_info.get("avg_net_price")
    budget_gap = None
    if isinstance(net_price, (int, float)) and isinstance(budget_cap, int) and budget_cap > 0:
        budget_gap = max(0, int(net_price) - budget_cap)

    out = dict(entry)
    out["prefilter_tag"] = tag
    out["is_stretch"] = is_stretch
    if budget_gap is not None:
        out["budget_gap_usd"] = budget_gap
    return out


def apply_budget_prefilter(
    scored_schools: list[dict[str, Any]],
    *,
    budget_cap: int | None,
    stretch_quota: int = 3,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Apply hard budget gate and keep at most N stretch schools."""
    if budget_cap is None or budget_cap <= 0:
        selected = [
            _normalise_prefilter_tag(entry, tag="no_budget", is_stretch=False, budget_cap=None)
            for entry in scored_schools
        ]
        selected.sort(key=lambda row: float(row.get("overall_score", 0.0)), reverse=True)
        meta = {
            "budget_cap_used": None,
            "eligible_count": len(selected),
            "stretch_count": 0,
            "excluded_count": 0,
            "excluded_reasons_summary": {},
            "prefilter_enabled": False,
        }
        return selected, meta, []

    eligible: list[dict[str, Any]] = []
    over_budget: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []

    for entry in scored_schools:
        school_info = entry.get("school_info", {})
        net_price = school_info.get("avg_net_price")
        school_id = entry.get("school_id")
        school_name = entry.get("school_name")
        if net_price is None:
            excluded.append(
                {
                    "school_id": str(school_id) if school_id is not None else None,
                    "school_name": school_name,
                    "reason": "missing_net_price",
                }
            )
            continue
        if int(net_price) <= budget_cap:
            eligible.append(_normalise_prefilter_tag(entry, tag="eligible", is_stretch=False, budget_cap=budget_cap))
            continue
        over_budget.append(entry)

    over_budget.sort(key=lambda row: float(row.get("overall_score", 0.0)), reverse=True)
    stretch = [
        _normalise_prefilter_tag(entry, tag="stretch", is_stretch=True, budget_cap=budget_cap)
        for entry in over_budget[: max(0, int(stretch_quota))]
    ]
    for entry in over_budget[max(0, int(stretch_quota)):]:
        school_id = entry.get("school_id")
        excluded.append(
            {
                "school_id": str(school_id) if school_id is not None else None,
                "school_name": entry.get("school_name"),
                "reason": "over_budget",
            }
        )

    selected = eligible + stretch
    selected.sort(key=lambda row: float(row.get("overall_score", 0.0)), reverse=True)

    meta = {
        "budget_cap_used": budget_cap,
        "eligible_count": len(eligible),
        "stretch_count": len(stretch),
        "excluded_count": len(excluded),
        "excluded_reasons_summary": _build_excluded_summary(excluded),
        "prefilter_enabled": True,
    }
    return selected, meta, excluded


def _scenario_entry(
    school: dict[str, Any],
    *,
    scenario_id: str,
    scenario_score: float,
    rank: int,
    baseline_rank: int,
    reason: str,
    outcome_breakdown: dict[str, float],
) -> dict[str, Any]:
    row = dict(school)
    row["rank"] = rank
    row["baseline_rank"] = baseline_rank
    row["rank_delta"] = baseline_rank - rank
    row["scenario_score"] = round(_clip01(scenario_score), 4)
    row["score"] = row["scenario_score"]
    row["scenario_id"] = scenario_id
    row["scenario_reason"] = reason
    row["outcome_breakdown"] = outcome_breakdown
    return row


def build_scenario_pack(
    scored_schools: list[dict[str, Any]],
    *,
    student_budget_usd: int | None,
    budget_cap_override: int | None = None,
    stretch_quota: int = 3,
    student_majors: list[str] | None = None,
    preferences: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Build baseline + five deterministic scenario rankings."""
    prefs = preferences or {}
    budget_cap = budget_cap_override if budget_cap_override and budget_cap_override > 0 else student_budget_usd
    selected, prefilter_meta, excluded = apply_budget_prefilter(
        scored_schools,
        budget_cap=budget_cap,
        stretch_quota=stretch_quota,
    )
    if not selected:
        empty_pack = {"baseline": [], "scenarios": [], "meta": {
            "candidate_pool_size": len(scored_schools),
            "selected_pool_size": 0,
            **prefilter_meta,
        }}
        return [], empty_pack, excluded

    def school_features(row: dict[str, Any]) -> dict[str, float]:
        sub = row.get("sub_scores", {})
        academic = _to_score(sub.get("academic"))
        financial = _to_score(sub.get("financial"))
        career = _to_score(sub.get("career"))
        life = _to_score(sub.get("life"))
        admission = _to_score(row.get("admission_probability"))
        info = row.get("school_info", {})
        net_price = info.get("avg_net_price")
        net_price_int = int(net_price) if isinstance(net_price, (int, float)) else None
        budget_fit = _budget_score(net_price_int, budget_cap, financial)
        risk_fit = _risk_score(admission, info.get("acceptance_rate"))
        major_fit = _major_match_score(student_majors, row.get("program_names"))
        geo_fit = _geo_fit_score(prefs, row)
        roi_fit = _roi_score(career, academic, budget_fit)
        phd = _clip01(0.6 * academic + 0.4 * career)
        return {
            "academic": academic,
            "financial": financial,
            "career": career,
            "life": life,
            "admission": admission,
            "budget_fit": budget_fit,
            "risk_fit": risk_fit,
            "major_fit": major_fit,
            "geo_fit": geo_fit,
            "roi_fit": roi_fit,
            "phd": phd,
        }

    scenario_specs: list[tuple[str, str, dict[str, float]]] = [
        ("budget_first", "预算优先", {"budget_fit": 0.55, "financial": 0.20, "admission": 0.15, "career": 0.10}),
        ("risk_first", "录取稳健优先", {"risk_fit": 0.55, "admission": 0.20, "academic": 0.15, "budget_fit": 0.10}),
        ("major_first", "专业匹配优先", {"major_fit": 0.45, "academic": 0.20, "career": 0.15, "admission": 0.10, "budget_fit": 0.10}),
        ("geo_first", "地域偏好优先", {"geo_fit": 0.45, "life": 0.20, "budget_fit": 0.15, "admission": 0.10, "academic": 0.10}),
        ("roi_first", "职业ROI优先", {"roi_fit": 0.45, "career": 0.20, "academic": 0.15, "budget_fit": 0.10, "admission": 0.10}),
    ]

    baseline_sorted = sorted(selected, key=lambda row: float(row.get("overall_score", 0.0)), reverse=True)
    baseline_ids = {
        str(row.get("school_id")): idx + 1
        for idx, row in enumerate(baseline_sorted)
    }

    baseline_rows: list[dict[str, Any]] = []
    feature_by_school: dict[str, dict[str, float]] = {}
    for idx, row in enumerate(baseline_sorted, start=1):
        school_id = str(row.get("school_id"))
        feature_view = school_features(row)
        feature_by_school[school_id] = feature_view
        baseline_rows.append(
            _scenario_entry(
                row,
                scenario_id="baseline",
                scenario_score=_to_score(row.get("overall_score")),
                rank=idx,
                baseline_rank=idx,
                reason="综合分排序基线",
                outcome_breakdown={
                    "admission_probability": feature_view["admission"],
                    "academic_outcome": feature_view["academic"],
                    "career_outcome": feature_view["career"],
                    "life_satisfaction": feature_view["life"],
                    "phd_probability": feature_view["phd"],
                },
            )
        )

    scenarios_payload: list[dict[str, Any]] = []
    for scenario_id, label, weights in scenario_specs:
        scored_rows: list[tuple[dict[str, Any], float, str, dict[str, float]]] = []
        for row in baseline_sorted:
            school_id = str(row.get("school_id"))
            f = feature_by_school[school_id]
            score = sum(f[key] * weight for key, weight in weights.items())
            tag = str(row.get("prefilter_tag") or "")
            if scenario_id == "budget_first":
                reason = "预算内优先排序" if tag == "eligible" else "冲刺位：超预算但综合价值高"
            elif scenario_id == "risk_first":
                reason = "录取稳健性更高"
            elif scenario_id == "major_first":
                reason = "专业匹配度与学术契合优先"
            elif scenario_id == "geo_first":
                reason = "地域偏好与生活体验优先"
            else:
                reason = "职业结果与投入产出优先"
            outcome_breakdown = {
                "admission_probability": f["admission"],
                "academic_outcome": f["academic"],
                "career_outcome": f["career"],
                "life_satisfaction": f["life"],
                "phd_probability": f["phd"],
            }
            scored_rows.append((row, score, reason, outcome_breakdown))

        scored_rows.sort(key=lambda item: item[1], reverse=True)
        schools_payload: list[dict[str, Any]] = []
        for idx, (row, score, reason, outcome_breakdown) in enumerate(scored_rows, start=1):
            school_id = str(row.get("school_id"))
            schools_payload.append(
                _scenario_entry(
                    row,
                    scenario_id=scenario_id,
                    scenario_score=score,
                    rank=idx,
                    baseline_rank=baseline_ids.get(school_id, idx),
                    reason=reason,
                    outcome_breakdown=outcome_breakdown,
                )
            )
        scenarios_payload.append(
            {
                "id": scenario_id,
                "label": label,
                "schools": schools_payload,
            }
        )

    scenario_pack = {
        "baseline": baseline_rows,
        "scenarios": scenarios_payload,
        "meta": {
            "candidate_pool_size": len(scored_schools),
            "selected_pool_size": len(baseline_rows),
            **prefilter_meta,
        },
    }
    return baseline_rows, scenario_pack, excluded

