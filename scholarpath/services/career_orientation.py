"""Three-layer causal orientation scoring engine.

Each of 7 career orientations is scored across three causal layers:

    Layer 1 (L1) — Direct outcome metrics (CareerOutcomeProxy, Scorecard earnings)
    Layer 2 (L2) — School characteristics that cause L1 (programs, faculty, resources)
    Layer 3 (L3) — Environmental/metro factors that cause L2 (industry density, funding, COL)

When L1 confidence is high (≥ 0.6), direct outcomes dominate. When sparse, the
engine leans on causal predictors (L2/L3) as confidence-adjusted proxies.
"""

from __future__ import annotations

import enum
import math
from dataclasses import dataclass, field
from typing import Optional, Sequence

from scholarpath.db.models.career import CareerOutcomeProxy
from scholarpath.db.models.metro_area import MetroAreaProfile
from scholarpath.db.models.school import Program, School


# ── Orientation definitions ─────────────────────────────────────────────

class CareerOrientation(str, enum.Enum):
    BIG_TECH = "big_tech"
    STARTUP = "startup"
    ROI = "roi"
    LIFESTYLE = "lifestyle"
    PHD_RESEARCH = "phd_research"
    FINANCE_BIZ = "finance_biz"
    PUBLIC_SERVICE = "public_service"


# ── Scored layer result ─────────────────────────────────────────────────

@dataclass
class LayerScore:
    value: float = 0.5  # 0-1 normalised score
    confidence: float = 0.0  # 0-1 how much data backed this
    signals: dict = field(default_factory=dict)  # raw signal values for transparency


@dataclass
class OrientationResult:
    orientation: CareerOrientation
    score: float  # 0-1 blended score
    layer1: LayerScore
    layer2: LayerScore
    layer3: LayerScore


# ── Normalisation helpers ───────────────────────────────────────────────

def _norm_rank(rank: Optional[int], best: int = 1, worst: int = 200) -> float:
    """Normalise a ranking (lower is better) to 0-1 (higher is better)."""
    if rank is None:
        return 0.5
    clamped = max(best, min(worst, rank))
    return 1.0 - (clamped - best) / (worst - best)


def _norm_ratio(value: Optional[float], *, low: float = 0.0, high: float = 1.0) -> float:
    """Normalise a value into 0-1 given expected [low, high] range."""
    if value is None:
        return 0.5
    if high == low:
        return 0.5
    return max(0.0, min(1.0, (value - low) / (high - low)))


def _norm_inverse(value: Optional[float], *, scale: float = 1.0) -> float:
    """Inverse normalisation: smaller value → higher score. Uses 1/(1 + v/scale)."""
    if value is None:
        return 0.5
    return 1.0 / (1.0 + max(0.0, value) / scale)


def _proxy_value(
    proxies: Sequence[CareerOutcomeProxy],
    outcome_type: str,
) -> tuple[float, float]:
    """Return (aggregated_estimate, confidence) for a given outcome type."""
    for p in proxies:
        if p.outcome_type == outcome_type:
            spread = p.confidence_upper - p.confidence_lower
            confidence = max(0.0, min(1.0, 1.0 - spread)) if spread > 0 else 0.3
            return p.aggregated_estimate, confidence
    return 0.5, 0.0  # no data


def _avg_confidence(values: list[tuple[float, float]]) -> tuple[float, float]:
    """Average scored signals; return (avg_value, avg_confidence)."""
    if not values:
        return 0.5, 0.0
    avg_v = sum(v for v, _ in values) / len(values)
    avg_c = sum(c for _, c in values) / len(values)
    return avg_v, avg_c


# ── Per-orientation layer computers ─────────────────────────────────────

def _find_program(programs: Sequence[Program], department_hint: str) -> Optional[Program]:
    """Find best-matching program by department substring."""
    hint_lower = department_hint.lower()
    for p in programs:
        if hint_lower in p.department.lower() or hint_lower in p.name.lower():
            return p
    return None


# ━━ BIG TECH ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _big_tech_l1(
    proxies: Sequence[CareerOutcomeProxy],
    scorecard_earnings: Optional[int],
) -> LayerScore:
    bt_val, bt_conf = _proxy_value(proxies, "big_tech")
    earn_score = _norm_ratio(scorecard_earnings, low=30000, high=120000) if scorecard_earnings else 0.5
    earn_conf = 0.7 if scorecard_earnings else 0.0
    val, conf = _avg_confidence([(bt_val, bt_conf), (earn_score, earn_conf)])
    return LayerScore(val, conf, {"big_tech_proxy": bt_val, "earnings_10yr": scorecard_earnings})


def _big_tech_l2(school: School, programs: Sequence[Program]) -> LayerScore:
    cs = _find_program(programs, "Computer Science")
    signals: dict = {}
    scores = []

    cs_rank = _norm_rank(cs.us_news_rank, best=1, worst=100) if cs else 0.5
    signals["cs_rank"] = cs.us_news_rank if cs else None
    scores.append(cs_rank)

    has_coop = 1.0 if (cs and cs.has_coop) else 0.3
    signals["has_coop"] = bool(cs and cs.has_coop)
    scores.append(has_coop)

    sfr = _norm_inverse(school.student_faculty_ratio, scale=15.0)
    signals["student_faculty_ratio"] = school.student_faculty_ratio
    scores.append(sfr)

    has_research = 0.8 if (cs and cs.has_research_opps) else 0.4
    signals["has_research_opps"] = bool(cs and cs.has_research_opps)
    scores.append(has_research)

    return LayerScore(sum(scores) / len(scores), 0.7, signals)


def _big_tech_l3(metro: Optional[MetroAreaProfile]) -> LayerScore:
    if not metro:
        return LayerScore(0.5, 0.0, {})
    tech = _norm_ratio(metro.tech_employer_count, low=0, high=10000)
    vc = _norm_ratio(metro.vc_investment_usd, low=0, high=100_000_000_000)
    return LayerScore(
        (tech * 0.6 + vc * 0.4), 0.8,
        {"tech_employer_count": metro.tech_employer_count, "vc_investment_usd": metro.vc_investment_usd},
    )


# ━━ STARTUP ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _startup_l1(
    proxies: Sequence[CareerOutcomeProxy],
    scorecard_earnings: Optional[int],
) -> LayerScore:
    su_val, su_conf = _proxy_value(proxies, "startup")
    earn_score = _norm_ratio(scorecard_earnings, low=30000, high=120000) if scorecard_earnings else 0.5
    earn_conf = 0.5 if scorecard_earnings else 0.0
    val, conf = _avg_confidence([(su_val, su_conf), (earn_score, earn_conf)])
    return LayerScore(val, conf, {"startup_proxy": su_val, "earnings_10yr": scorecard_earnings})


def _startup_l2(school: School, programs: Sequence[Program]) -> LayerScore:
    scores = []
    signals: dict = {}

    endow = _norm_ratio(school.endowment_per_student, low=0, high=400000)
    signals["endowment_per_student"] = school.endowment_per_student
    scores.append(endow)

    any_coop = any(p.has_coop for p in programs)
    signals["any_coop"] = any_coop
    scores.append(0.8 if any_coop else 0.3)

    # Smaller class size → more mentorship
    sfr = _norm_inverse(school.student_faculty_ratio, scale=12.0)
    signals["student_faculty_ratio"] = school.student_faculty_ratio
    scores.append(sfr)

    return LayerScore(sum(scores) / len(scores), 0.6, signals)


def _startup_l3(metro: Optional[MetroAreaProfile]) -> LayerScore:
    if not metro:
        return LayerScore(0.5, 0.0, {})
    vc = _norm_ratio(metro.vc_investment_usd, low=0, high=100_000_000_000)
    tech = _norm_ratio(metro.tech_employer_count, low=0, high=10000)
    # Low COL helps startup runway
    col_inv = _norm_inverse(metro.cost_of_living_index, scale=100.0) if metro.cost_of_living_index else 0.5
    val = vc * 0.45 + tech * 0.30 + col_inv * 0.25
    return LayerScore(
        val, 0.8,
        {"vc_investment_usd": metro.vc_investment_usd, "cost_of_living_index": metro.cost_of_living_index},
    )


# ━━ ROI ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _roi_l1(
    school: School,
    scorecard_earnings: Optional[int],
) -> LayerScore:
    net_price = school.avg_net_price or school.tuition_oos
    if scorecard_earnings and net_price and net_price > 0:
        # 4-year cost vs 10-year earnings
        roi_ratio = scorecard_earnings / (net_price * 4) * 10
        score = _norm_ratio(roi_ratio, low=0.5, high=5.0)
        return LayerScore(score, 0.8, {"roi_ratio": round(roi_ratio, 2), "earnings_10yr": scorecard_earnings, "net_price": net_price})
    return LayerScore(0.5, 0.1, {"earnings_10yr": scorecard_earnings, "net_price": net_price})


def _roi_l2(school: School) -> LayerScore:
    scores = []
    signals: dict = {}

    grad = _norm_ratio(school.graduation_rate_4yr, low=0.3, high=0.95)
    signals["graduation_rate_4yr"] = school.graduation_rate_4yr
    scores.append(grad)

    # Higher selectivity → better outcomes
    sel = _norm_inverse(school.acceptance_rate, scale=0.5) if school.acceptance_rate else 0.5
    signals["acceptance_rate"] = school.acceptance_rate
    scores.append(sel)

    # Lower net price → better ROI
    price_score = _norm_inverse(school.avg_net_price, scale=40000) if school.avg_net_price else 0.5
    signals["avg_net_price"] = school.avg_net_price
    scores.append(price_score)

    return LayerScore(sum(scores) / len(scores), 0.7, signals)


def _roi_l3(metro: Optional[MetroAreaProfile]) -> LayerScore:
    if not metro:
        return LayerScore(0.5, 0.0, {})
    income = _norm_ratio(metro.median_household_income, low=40000, high=120000)
    col_inv = _norm_inverse(metro.cost_of_living_index, scale=100.0) if metro.cost_of_living_index else 0.5
    return LayerScore(
        income * 0.6 + col_inv * 0.4, 0.7,
        {"median_household_income": metro.median_household_income, "cost_of_living_index": metro.cost_of_living_index},
    )


# ━━ LIFESTYLE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _lifestyle_l1(metro: Optional[MetroAreaProfile]) -> LayerScore:
    """Lifestyle L1 is a composite proxy — no direct outcome data."""
    if not metro:
        return LayerScore(0.5, 0.0, {})
    safety = metro.safety_index or 0.5
    col_score = _norm_inverse(metro.cost_of_living_index, scale=120.0) if metro.cost_of_living_index else 0.5
    val = safety * 0.5 + col_score * 0.5
    return LayerScore(val, 0.5, {"safety_index": metro.safety_index, "cost_of_living_index": metro.cost_of_living_index})


def _lifestyle_l2(school: School) -> LayerScore:
    scores = []
    signals: dict = {}

    # Urban campuses score higher for convenience
    setting_scores = {"urban": 0.85, "suburban": 0.6, "rural": 0.35}
    setting_score = setting_scores.get(school.campus_setting or "", 0.5)
    signals["campus_setting"] = school.campus_setting
    scores.append(setting_score)

    # Higher intl % → more diverse, cosmopolitan
    intl = _norm_ratio(school.intl_student_pct, low=0.0, high=0.25)
    signals["intl_student_pct"] = school.intl_student_pct
    scores.append(intl)

    sfr = _norm_inverse(school.student_faculty_ratio, scale=15.0)
    signals["student_faculty_ratio"] = school.student_faculty_ratio
    scores.append(sfr)

    return LayerScore(sum(scores) / len(scores), 0.6, signals)


def _lifestyle_l3(metro: Optional[MetroAreaProfile]) -> LayerScore:
    if not metro:
        return LayerScore(0.5, 0.0, {})
    signals = {}
    scores = []

    col = _norm_inverse(metro.cost_of_living_index, scale=120.0) if metro.cost_of_living_index else 0.5
    signals["cost_of_living_index"] = metro.cost_of_living_index
    scores.append(col)

    safety = metro.safety_index or 0.5
    signals["safety_index"] = metro.safety_index
    scores.append(safety)

    asian = _norm_ratio(metro.asian_population_pct, low=0, high=20.0)
    signals["asian_population_pct"] = metro.asian_population_pct
    scores.append(asian)

    signals["climate_zone"] = metro.climate_zone
    # Mild climates preferred
    mild_zones = {"Mediterranean", "Marine west coast", "Humid subtropical"}
    climate_score = 0.8 if metro.climate_zone in mild_zones else 0.4
    scores.append(climate_score)

    return LayerScore(sum(scores) / len(scores), 0.75, signals)


# ━━ PHD / RESEARCH ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _phd_research_l1(proxies: Sequence[CareerOutcomeProxy]) -> LayerScore:
    phd_val, phd_conf = _proxy_value(proxies, "phd")
    gs_val, gs_conf = _proxy_value(proxies, "grad_school")
    val, conf = _avg_confidence([(phd_val, phd_conf), (gs_val, gs_conf)])
    return LayerScore(val, conf, {"phd_proxy": phd_val, "grad_school_proxy": gs_val})


def _phd_research_l2(school: School, programs: Sequence[Program]) -> LayerScore:
    scores = []
    signals: dict = {}

    # Any program with research opps
    research_programs = [p for p in programs if p.has_research_opps]
    research_ratio = len(research_programs) / max(1, len(programs))
    signals["research_programs_ratio"] = round(research_ratio, 2)
    scores.append(min(1.0, research_ratio * 1.5))  # boost

    # Best program rank
    ranked_programs = [p for p in programs if p.us_news_rank]
    if ranked_programs:
        best_rank = min(p.us_news_rank for p in ranked_programs)  # type: ignore[arg-type]
        signals["best_program_rank"] = best_rank
        scores.append(_norm_rank(best_rank, best=1, worst=100))
    else:
        scores.append(0.5)

    sfr = _norm_inverse(school.student_faculty_ratio, scale=10.0)
    signals["student_faculty_ratio"] = school.student_faculty_ratio
    scores.append(sfr)

    # Endowment per student → research resources
    endow = _norm_ratio(school.endowment_per_student, low=0, high=400000)
    signals["endowment_per_student"] = school.endowment_per_student
    scores.append(endow)

    return LayerScore(sum(scores) / len(scores), 0.7, signals)


def _phd_research_l3(metro: Optional[MetroAreaProfile]) -> LayerScore:
    if not metro:
        return LayerScore(0.5, 0.0, {})
    labs = _norm_ratio(metro.federal_lab_count, low=0, high=6)
    nsf = _norm_ratio(metro.nsf_funding_total, low=0, high=4_000_000_000)
    return LayerScore(
        labs * 0.4 + nsf * 0.6, 0.8,
        {"federal_lab_count": metro.federal_lab_count, "nsf_funding_total": metro.nsf_funding_total},
    )


# ━━ FINANCE / BIZ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _finance_biz_l1(
    proxies: Sequence[CareerOutcomeProxy],
    scorecard_earnings: Optional[int],
) -> LayerScore:
    fb_val, fb_conf = _proxy_value(proxies, "finance_biz")
    earn_score = _norm_ratio(scorecard_earnings, low=40000, high=150000) if scorecard_earnings else 0.5
    earn_conf = 0.6 if scorecard_earnings else 0.0
    val, conf = _avg_confidence([(fb_val, fb_conf), (earn_score, earn_conf)])
    return LayerScore(val, conf, {"finance_biz_proxy": fb_val, "earnings_10yr": scorecard_earnings})


def _finance_biz_l2(school: School) -> LayerScore:
    scores = []
    signals: dict = {}

    rank = _norm_rank(school.us_news_rank, best=1, worst=100)
    signals["us_news_rank"] = school.us_news_rank
    scores.append(rank)

    # Endowment → alumni network strength
    endow = _norm_ratio(school.endowment_per_student, low=0, high=400000)
    signals["endowment_per_student"] = school.endowment_per_student
    scores.append(endow)

    # Selectivity matters heavily for target school lists
    sel = _norm_inverse(school.acceptance_rate, scale=0.3) if school.acceptance_rate else 0.5
    signals["acceptance_rate"] = school.acceptance_rate
    scores.append(sel)

    return LayerScore(sum(scores) / len(scores), 0.7, signals)


def _finance_biz_l3(metro: Optional[MetroAreaProfile]) -> LayerScore:
    if not metro:
        return LayerScore(0.5, 0.0, {})
    # Closer to finance hub → better
    dist = _norm_inverse(metro.finance_hub_distance_km, scale=200.0)
    income = _norm_ratio(metro.median_household_income, low=40000, high=120000)
    return LayerScore(
        dist * 0.7 + income * 0.3, 0.8,
        {"finance_hub_distance_km": metro.finance_hub_distance_km, "median_household_income": metro.median_household_income},
    )


# ━━ PUBLIC SERVICE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _public_service_l1(proxies: Sequence[CareerOutcomeProxy]) -> LayerScore:
    ps_val, ps_conf = _proxy_value(proxies, "public_service")
    return LayerScore(ps_val, ps_conf, {"public_service_proxy": ps_val})


def _public_service_l2(school: School) -> LayerScore:
    scores = []
    signals: dict = {}

    # Diversity / global perspective
    intl = _norm_ratio(school.intl_student_pct, low=0.0, high=0.20)
    signals["intl_student_pct"] = school.intl_student_pct
    scores.append(intl)

    # Aid capacity → loan repayment assistance potential
    endow = _norm_ratio(school.endowment_per_student, low=0, high=400000)
    signals["endowment_per_student"] = school.endowment_per_student
    scores.append(endow)

    # Overall prestige helps in policy/gov hiring
    rank = _norm_rank(school.us_news_rank, best=1, worst=150)
    signals["us_news_rank"] = school.us_news_rank
    scores.append(rank)

    return LayerScore(sum(scores) / len(scores), 0.6, signals)


def _public_service_l3(metro: Optional[MetroAreaProfile]) -> LayerScore:
    if not metro:
        return LayerScore(0.5, 0.0, {})
    # DC distance is the key signal — but we use finance_hub_distance_km
    # as a rough proxy (DC is a "hub" in the CSV at 0 km for Washington)
    col = _norm_inverse(metro.cost_of_living_index, scale=120.0) if metro.cost_of_living_index else 0.5
    safety = metro.safety_index or 0.5
    return LayerScore(
        col * 0.4 + safety * 0.6, 0.6,
        {"cost_of_living_index": metro.cost_of_living_index, "safety_index": metro.safety_index},
    )


# ── Main scoring function ───────────────────────────────────────────────

def compute_orientation_score(
    orientation: CareerOrientation,
    school: School,
    programs: Sequence[Program],
    metro: Optional[MetroAreaProfile],
    proxies: Sequence[CareerOutcomeProxy],
    scorecard_earnings: Optional[int] = None,
) -> OrientationResult:
    """Compute a three-layer causal score for a single orientation."""

    if orientation == CareerOrientation.BIG_TECH:
        l1 = _big_tech_l1(proxies, scorecard_earnings)
        l2 = _big_tech_l2(school, programs)
        l3 = _big_tech_l3(metro)
    elif orientation == CareerOrientation.STARTUP:
        l1 = _startup_l1(proxies, scorecard_earnings)
        l2 = _startup_l2(school, programs)
        l3 = _startup_l3(metro)
    elif orientation == CareerOrientation.ROI:
        l1 = _roi_l1(school, scorecard_earnings)
        l2 = _roi_l2(school)
        l3 = _roi_l3(metro)
    elif orientation == CareerOrientation.LIFESTYLE:
        l1 = _lifestyle_l1(metro)
        l2 = _lifestyle_l2(school)
        l3 = _lifestyle_l3(metro)
    elif orientation == CareerOrientation.PHD_RESEARCH:
        l1 = _phd_research_l1(proxies)
        l2 = _phd_research_l2(school, programs)
        l3 = _phd_research_l3(metro)
    elif orientation == CareerOrientation.FINANCE_BIZ:
        l1 = _finance_biz_l1(proxies, scorecard_earnings)
        l2 = _finance_biz_l2(school)
        l3 = _finance_biz_l3(metro)
    elif orientation == CareerOrientation.PUBLIC_SERVICE:
        l1 = _public_service_l1(proxies)
        l2 = _public_service_l2(school)
        l3 = _public_service_l3(metro)
    else:
        raise ValueError(f"Unknown orientation: {orientation}")

    # Confidence-weighted blending
    if l1.confidence >= 0.6:
        score = 0.50 * l1.value + 0.30 * l2.value + 0.20 * l3.value
    else:
        # Sparse outcome data → lean on causal predictors
        score = 0.25 * l1.value + 0.45 * l2.value + 0.30 * l3.value

    return OrientationResult(
        orientation=orientation,
        score=max(0.0, min(1.0, score)),
        layer1=l1,
        layer2=l2,
        layer3=l3,
    )


def compute_all_orientations(
    school: School,
    programs: Sequence[Program],
    metro: Optional[MetroAreaProfile],
    proxies: Sequence[CareerOutcomeProxy],
    scorecard_earnings: Optional[int] = None,
) -> dict[str, OrientationResult]:
    """Compute scores for all 7 orientations. Returns {orientation_key: result}."""
    return {
        o.value: compute_orientation_score(o, school, programs, metro, proxies, scorecard_earnings)
        for o in CareerOrientation
    }
