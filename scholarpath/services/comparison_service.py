"""Multi-school comparison report service.

Generates detailed orientation-by-orientation comparison reports with
three-layer causal graphs for the frontend D3 visualization.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncGenerator
from typing import Any, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from scholarpath.api.models.comparison import (
    CausalFactorEdge,
    CausalFactorNode,
    CompareReportResponse,
    OrientationCausalGraph,
    OrientationComparison,
    OrientationLayerDetail,
    SchoolOrientationScore,
)
from scholarpath.db.models import CareerOutcomeProxy, School, Student
from scholarpath.llm.client import LLMClient
from scholarpath.services.career_orientation import (
    CareerOrientation,
    LayerScore,
    OrientationResult,
    compute_all_orientations,
)
from scholarpath.services.portfolio_service import get_student_canonical_preferences
from scholarpath.services.student_service import get_student

logger = logging.getLogger(__name__)

# ── Human-readable labels for signal keys ────────────────────────────────

_SIGNAL_LABELS: dict[str, str] = {
    # L1
    "big_tech_proxy": "Big Tech Employment Rate",
    "earnings_10yr": "10-Year Median Earnings",
    "startup_proxy": "Startup Employment Rate",
    "phd_proxy": "PhD Placement Rate",
    "grad_school_proxy": "Grad School Rate",
    "finance_biz_proxy": "Finance/Biz Employment",
    "public_service_proxy": "Public Service Employment",
    "roi_ratio": "ROI Ratio (Earnings/Cost)",
    "safety_index": "Safety Index",
    "cost_of_living_index": "Cost of Living Index",
    # L2
    "cs_rank": "CS Program Ranking",
    "has_coop": "Has Co-op Program",
    "student_faculty_ratio": "Student-Faculty Ratio",
    "has_research_opps": "Research Opportunities",
    "us_news_rank": "US News Ranking",
    "endowment_per_student": "Endowment per Student",
    "acceptance_rate": "Acceptance Rate",
    "graduation_rate_4yr": "4-Year Graduation Rate",
    "avg_net_price": "Average Net Price",
    "campus_setting": "Campus Setting",
    "intl_student_pct": "International Student %",
    "any_coop": "Any Co-op Available",
    "research_programs_ratio": "Research Programs Ratio",
    "best_program_rank": "Best Program Ranking",
    # L3
    "tech_employer_count": "Tech Employers Nearby",
    "vc_investment_usd": "VC Investment (Metro)",
    "median_household_income": "Median Household Income",
    "finance_hub_distance_km": "Distance to Finance Hub",
    "federal_lab_count": "Federal Labs Nearby",
    "nsf_funding_total": "NSF R&D Funding (Metro)",
    "asian_population_pct": "Asian Population %",
    "climate_zone": "Climate Zone",
    "net_price": "Net Price",
}


def _signal_label(key: str) -> str:
    return _SIGNAL_LABELS.get(key, key.replace("_", " ").title())


def _normalize_signal(value: Any) -> float:
    """Convert a signal value to a float for the graph node."""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    # String values (e.g. climate_zone) → hash to a stable float
    return 0.5


# ── Causal graph builder ─────────────────────────────────────────────────

# ── Per-orientation causal edge definitions ──────────────────────────────
# Each entry: (source_signal, target_signal, strength, mechanism_zh)
# These represent domain-expert causal relationships, NOT a full bipartite graph.

_CAUSAL_EDGES: dict[str, list[tuple[str, str, float, str]]] = {
    "big_tech": [
        # L3 → L2
        ("l3_tech_employer_count", "l2_has_coop", 0.8, "周边大厂多 → 校企合作Co-op项目更多"),
        ("l3_tech_employer_count", "l2_cs_rank", 0.6, "科技集群吸引优质CS教授 → 提升专业排名"),
        ("l3_vc_investment_usd", "l2_has_research_opps", 0.5, "VC活跃地区 → 更多产学研合作机会"),
        # L2 → L1
        ("l2_cs_rank", "l1_big_tech_proxy", 0.9, "CS专业排名高 → 大厂招聘target school"),
        ("l2_has_coop", "l1_big_tech_proxy", 0.7, "Co-op实习经历 → 大厂return offer"),
        ("l2_student_faculty_ratio", "l1_earnings_10yr", 0.5, "师生比低 → 教学质量高 → 薪资更高"),
        ("l2_has_research_opps", "l1_big_tech_proxy", 0.4, "研究经历 → 大厂研究岗竞争力"),
    ],
    "startup": [
        ("l3_vc_investment_usd", "l2_endowment_per_student", 0.4, "VC活跃 → 校友捐赠/创业生态更强"),
        ("l3_vc_investment_usd", "l2_any_coop", 0.5, "VC活跃 → 创业公司实习机会多"),
        ("l3_cost_of_living_index", "l2_student_faculty_ratio", 0.3, "低生活成本 → 学校运营成本低 → 资源充裕"),
        ("l2_endowment_per_student", "l1_startup_proxy", 0.7, "校友网络/资源 → 创业支持"),
        ("l2_any_coop", "l1_startup_proxy", 0.6, "Co-op经历 → 行业认知 → 创业能力"),
        ("l2_student_faculty_ratio", "l1_earnings_10yr", 0.4, "导师关注度 → 职业发展"),
    ],
    "roi": [
        ("l3_median_household_income", "l2_avg_net_price", 0.3, "高收入地区 → 学费也偏高"),
        ("l3_cost_of_living_index", "l2_avg_net_price", 0.5, "高生活成本 → 总费用更高"),
        ("l3_median_household_income", "l2_graduation_rate_4yr", 0.4, "经济环境好 → 学生完成学业比例高"),
        ("l2_graduation_rate_4yr", "l1_roi_ratio", 0.8, "按时毕业 → 少花一年学费 → ROI更高"),
        ("l2_acceptance_rate", "l1_roi_ratio", 0.6, "选择性高 → 品牌溢价 → 毕业后薪资更高"),
        ("l2_avg_net_price", "l1_roi_ratio", 0.9, "净学费直接决定ROI的分母"),
    ],
    "lifestyle": [
        ("l3_cost_of_living_index", "l2_campus_setting", 0.4, "生活成本影响城市规模和校园环境"),
        ("l3_safety_index", "l2_campus_setting", 0.5, "安全指数影响居住体验"),
        ("l3_asian_population_pct", "l2_intl_student_pct", 0.6, "亚裔社区规模 → 国际生友好度"),
        ("l2_campus_setting", "l1_safety_index", 0.5, "城市型校园 vs 郊区 → 不同安全感受"),
        ("l2_intl_student_pct", "l1_cost_of_living_index", 0.3, "国际生多 → 多元文化但也推高周边消费"),
        ("l2_student_faculty_ratio", "l1_safety_index", 0.3, "小班制 → 社区感更强 → 满意度更高"),
    ],
    "phd_research": [
        ("l3_nsf_funding_total", "l2_has_research_opps", 0.9, "联邦科研经费 → 实验室/课题组资源"),
        ("l3_federal_lab_count", "l2_has_research_opps", 0.7, "附近国家实验室 → 合作研究机会"),
        ("l3_nsf_funding_total", "l2_best_program_rank", 0.6, "科研资金充裕 → 吸引顶尖学者 → 提升专排"),
        ("l2_has_research_opps", "l1_phd_proxy", 0.9, "本科研究机会 → PhD录取竞争力"),
        ("l2_best_program_rank", "l1_phd_proxy", 0.8, "专业排名高 → PhD项目认可度"),
        ("l2_student_faculty_ratio", "l1_grad_school_proxy", 0.6, "师生比低 → 导师指导多 → 深造率高"),
        ("l2_endowment_per_student", "l1_phd_proxy", 0.5, "资源充裕 → 更多RA/TA资助"),
    ],
    "finance_biz": [
        ("l3_finance_hub_distance_km", "l2_us_news_rank", 0.3, "金融中心距离影响学校金融声誉"),
        ("l3_finance_hub_distance_km", "l2_endowment_per_student", 0.4, "靠近金融中心 → 校友金融捐赠更多"),
        ("l3_median_household_income", "l2_acceptance_rate", 0.3, "富裕地区 → 竞争更激烈"),
        ("l2_us_news_rank", "l1_finance_biz_proxy", 0.9, "综合排名 → 投行/咨询Target School名单"),
        ("l2_endowment_per_student", "l1_finance_biz_proxy", 0.7, "校友网络强 → 金融业内推资源"),
        ("l2_acceptance_rate", "l1_earnings_10yr", 0.6, "选择性高 → 品牌价值 → 起薪更高"),
    ],
    "public_service": [
        ("l3_cost_of_living_index", "l2_intl_student_pct", 0.3, "生活成本影响国际生选择"),
        ("l3_safety_index", "l2_intl_student_pct", 0.4, "安全环境 → 更吸引国际背景学生"),
        ("l2_intl_student_pct", "l1_public_service_proxy", 0.5, "多元化视野 → 公共服务/国际组织导向"),
        ("l2_endowment_per_student", "l1_public_service_proxy", 0.6, "资源充裕 → 贷款减免计划(LRAP)"),
        ("l2_us_news_rank", "l1_public_service_proxy", 0.7, "学校声誉 → 政府/NGO招聘认可"),
    ],
}


def _build_causal_graph(
    orientation: str,
    school_results: dict[str, OrientationResult],
) -> OrientationCausalGraph:
    """Build a three-layer causal factor graph with domain-specific edges.

    Unlike a full bipartite graph, only meaningful causal relationships
    are included — e.g., "nearby tech employers → more co-op programs → higher big tech placement".
    """
    # Collect all signal keys per layer across all schools
    l3_keys: dict[str, str] = {}
    l2_keys: dict[str, str] = {}
    l1_keys: dict[str, str] = {}

    for result in school_results.values():
        for k in result.layer3.signals:
            l3_keys[k] = _signal_label(k)
        for k in result.layer2.signals:
            l2_keys[k] = _signal_label(k)
        for k in result.layer1.signals:
            l1_keys[k] = _signal_label(k)

    # Build nodes with multi-school values
    nodes: list[CausalFactorNode] = []
    all_node_ids: set[str] = set()

    for key, label in l3_keys.items():
        nid = f"l3_{key}"
        values = {sid: _normalize_signal(r.layer3.signals.get(key)) for sid, r in school_results.items()}
        nodes.append(CausalFactorNode(id=nid, label=label, layer="l3_environment", values=values))
        all_node_ids.add(nid)

    for key, label in l2_keys.items():
        nid = f"l2_{key}"
        values = {sid: _normalize_signal(r.layer2.signals.get(key)) for sid, r in school_results.items()}
        nodes.append(CausalFactorNode(id=nid, label=label, layer="l2_school", values=values))
        all_node_ids.add(nid)

    for key, label in l1_keys.items():
        nid = f"l1_{key}"
        values = {sid: _normalize_signal(r.layer1.signals.get(key)) for sid, r in school_results.items()}
        nodes.append(CausalFactorNode(id=nid, label=label, layer="l1_outcome", values=values))
        all_node_ids.add(nid)

    # Build edges from the domain-specific causal map
    edges: list[CausalFactorEdge] = []
    causal_defs = _CAUSAL_EDGES.get(orientation, [])

    for source_id, target_id, strength, mechanism in causal_defs:
        # Only include edges whose source and target nodes actually exist in this graph
        if source_id in all_node_ids and target_id in all_node_ids:
            edges.append(CausalFactorEdge(
                source=source_id,
                target=target_id,
                strength=strength,
                mechanism=mechanism,
            ))

    return OrientationCausalGraph(
        orientation=orientation,
        nodes=nodes,
        edges=edges,
    )


def _layer_detail(ls: LayerScore) -> OrientationLayerDetail:
    return OrientationLayerDetail(
        value=round(ls.value, 4),
        confidence=round(ls.confidence, 4),
        signals=ls.signals,
    )


# ── Main service function ────────────────────────────────────────────────

async def generate_comparison_report(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    school_ids: list[uuid.UUID],
    orientations: list[str] | None = None,
) -> CompareReportResponse:
    """Generate a multi-school comparison report with causal graphs."""

    # Load student
    student = await get_student(session, student_id)
    prefs = get_student_canonical_preferences(student)
    career_goal = prefs.get("career_goal")

    # Determine orientations
    if orientations:
        orient_set = [CareerOrientation(o) for o in orientations if o in CareerOrientation.__members__.values()]
    else:
        orient_set = list(CareerOrientation)
    if not orient_set:
        orient_set = list(CareerOrientation)

    # Load schools with programs + metro
    schools: list[School] = []
    for sid in school_ids:
        school = await session.get(
            School, sid,
            options=[selectinload(School.programs), selectinload(School.metro_area)],
        )
        if school:
            schools.append(school)

    # Load career outcome proxies per school
    school_proxies: dict[uuid.UUID, list[CareerOutcomeProxy]] = {}
    for school in schools:
        result = await session.execute(
            select(CareerOutcomeProxy).where(CareerOutcomeProxy.school_id == school.id)
        )
        school_proxies[school.id] = list(result.scalars().all())

    # Compute orientation scores for each school
    school_orientation_results: dict[str, dict[str, OrientationResult]] = {}
    # {school_id_str: {orientation_key: OrientationResult}}
    for school in schools:
        scorecard_earnings: int | None = None
        if school.metadata_:
            scorecard_earnings = school.metadata_.get("scorecard_earnings")

        results = compute_all_orientations(
            school=school,
            programs=list(school.programs) if school.programs else [],
            metro=school.metro_area,
            proxies=school_proxies.get(school.id, []),
            scorecard_earnings=scorecard_earnings,
        )
        school_orientation_results[str(school.id)] = results

    # Build per-orientation comparisons + causal graphs
    orientation_comparisons: list[OrientationComparison] = []
    causal_graphs: list[OrientationCausalGraph] = []

    for orient in orient_set:
        orient_key = orient.value

        # Collect scores per school for this orientation
        school_scores: list[SchoolOrientationScore] = []
        orient_school_results: dict[str, OrientationResult] = {}

        for school in schools:
            sid_str = str(school.id)
            result = school_orientation_results[sid_str].get(orient_key)
            if not result:
                continue
            orient_school_results[sid_str] = result
            school_scores.append(SchoolOrientationScore(
                school_id=school.id,
                school_name=school.name,
                score=round(result.score, 4),
                l1=_layer_detail(result.layer1),
                l2=_layer_detail(result.layer2),
                l3=_layer_detail(result.layer3),
            ))

        # Generate narrative for this orientation
        narrative = await _generate_orientation_narrative(
            llm, orient_key, school_scores, student, career_goal,
        )

        orientation_comparisons.append(OrientationComparison(
            orientation=orient_key,
            schools=school_scores,
            narrative=narrative,
        ))

        # Build causal graph
        graph = _build_causal_graph(orient_key, orient_school_results)
        causal_graphs.append(graph)

    # Generate final recommendation
    recommendation, confidence = await _generate_recommendation(
        llm, orientation_comparisons, student, career_goal,
    )

    return CompareReportResponse(
        student_id=student_id,
        school_ids=school_ids,
        orientations=orientation_comparisons,
        causal_graphs=causal_graphs,
        recommendation=recommendation,
        confidence=confidence,
    )


async def generate_comparison_report_stream(
    session: AsyncSession,
    llm: LLMClient,
    student_id: uuid.UUID,
    school_ids: list[uuid.UUID],
    orientations: list[str] | None = None,
) -> AsyncGenerator[dict, None]:
    """Streaming version: yields one NDJSON dict per completed orientation,
    then a final 'recommendation' event.

    Event types:
      {"event": "orientation", "data": {orientation_comparison + causal_graph}}
      {"event": "recommendation", "data": {recommendation, confidence}}
      {"event": "error", "data": {message}}
    """
    # ── Shared data loading (same as non-streaming version) ──
    student = await get_student(session, student_id)
    prefs = get_student_canonical_preferences(student)
    career_goal = prefs.get("career_goal")

    if orientations:
        orient_set = [CareerOrientation(o) for o in orientations if o in CareerOrientation.__members__.values()]
    else:
        orient_set = list(CareerOrientation)
    if not orient_set:
        orient_set = list(CareerOrientation)

    schools: list[School] = []
    for sid in school_ids:
        school = await session.get(
            School, sid,
            options=[selectinload(School.programs), selectinload(School.metro_area)],
        )
        if school:
            schools.append(school)

    school_proxies: dict[uuid.UUID, list[CareerOutcomeProxy]] = {}
    for school in schools:
        result = await session.execute(
            select(CareerOutcomeProxy).where(CareerOutcomeProxy.school_id == school.id)
        )
        school_proxies[school.id] = list(result.scalars().all())

    # Compute all orientation scores (fast, no LLM)
    school_orientation_results: dict[str, dict[str, OrientationResult]] = {}
    for school in schools:
        scorecard_earnings: int | None = None
        if school.metadata_:
            scorecard_earnings = school.metadata_.get("scorecard_earnings")
        results = compute_all_orientations(
            school=school,
            programs=list(school.programs) if school.programs else [],
            metro=school.metro_area,
            proxies=school_proxies.get(school.id, []),
            scorecard_earnings=scorecard_earnings,
        )
        school_orientation_results[str(school.id)] = results

    # ── Stream one orientation at a time ──
    completed_comparisons: list[OrientationComparison] = []

    for orient in orient_set:
        orient_key = orient.value
        try:
            school_scores: list[SchoolOrientationScore] = []
            orient_school_results: dict[str, OrientationResult] = {}

            for school in schools:
                sid_str = str(school.id)
                r = school_orientation_results[sid_str].get(orient_key)
                if not r:
                    continue
                orient_school_results[sid_str] = r
                school_scores.append(SchoolOrientationScore(
                    school_id=school.id,
                    school_name=school.name,
                    score=round(r.score, 4),
                    l1=_layer_detail(r.layer1),
                    l2=_layer_detail(r.layer2),
                    l3=_layer_detail(r.layer3),
                ))

            narrative = await _generate_orientation_narrative(
                llm, orient_key, school_scores, student, career_goal,
            )

            comparison = OrientationComparison(
                orientation=orient_key, schools=school_scores, narrative=narrative,
            )
            graph = _build_causal_graph(orient_key, orient_school_results)
            completed_comparisons.append(comparison)

            yield {
                "event": "orientation",
                "data": {
                    "comparison": comparison.model_dump(mode="json"),
                    "causal_graph": graph.model_dump(mode="json"),
                },
            }
        except Exception:
            logger.exception("Streaming comparison failed for orientation %s", orient_key)
            yield {"event": "error", "data": {"message": f"Failed to generate {orient_key}"}}

    # ── Final recommendation ──
    try:
        recommendation, confidence = await _generate_recommendation(
            llm, completed_comparisons, student, career_goal,
        )
        yield {
            "event": "recommendation",
            "data": {
                "recommendation": recommendation,
                "confidence": confidence,
                "student_id": str(student_id),
                "school_ids": [str(sid) for sid in school_ids],
            },
        }
    except Exception:
        logger.exception("Streaming recommendation generation failed")
        yield {"event": "error", "data": {"message": "Failed to generate recommendation"}}


# ── LLM narrative generation ─────────────────────────────────────────────

async def _generate_orientation_narrative(
    llm: LLMClient,
    orientation: str,
    school_scores: list[SchoolOrientationScore],
    student: Student,
    career_goal: str | None,
) -> str:
    """Generate a concise per-orientation comparison narrative."""
    school_lines = []
    for s in school_scores:
        school_lines.append(
            f"- {s.school_name}: score={s.score:.2f} "
            f"(L1={s.l1.value:.2f}, L2={s.l2.value:.2f}, L3={s.l3.value:.2f})"
        )
    schools_block = "\n".join(school_lines)

    degree = getattr(student, "degree_level", "undergraduate")
    prompt = (
        f"Compare these schools for a {degree} student "
        f"on the '{orientation}' career orientation.\n\n"
        f"Scores:\n{schools_block}\n\n"
        f"Student's career goal: {career_goal or 'not specified'}\n\n"
        "Write a concise 2-3 sentence comparison in the student's language "
        "(Chinese if applicable). Focus on WHY the scores differ — "
        "what specific school characteristics or environmental factors "
        "drive the differences. Be direct."
    )

    try:
        return await llm.complete(
            [{"role": "user", "content": prompt}],
            temperature=0.4,
            max_tokens=300,
        )
    except Exception:
        logger.exception("Failed to generate orientation narrative for %s", orientation)
        return ""


async def _generate_recommendation(
    llm: LLMClient,
    comparisons: list[OrientationComparison],
    student: Student,
    career_goal: str | None,
) -> tuple[str, float]:
    """Generate a final recommendation based on all orientation comparisons."""
    summary_lines = []
    for comp in comparisons:
        best = max(comp.schools, key=lambda s: s.score) if comp.schools else None
        if best:
            summary_lines.append(f"- {comp.orientation}: best={best.school_name} ({best.score:.2f})")

    degree = getattr(student, "degree_level", "undergraduate")
    prompt = (
        f"Based on this multi-orientation comparison for a {degree} student "
        f"(career goal: {career_goal or 'not specified'}):\n\n"
        + "\n".join(summary_lines) + "\n\n"
        "Provide a final recommendation in 3-4 sentences. "
        "Be specific about which school is best for this student and why. "
        "Write in the student's language (Chinese if applicable)."
    )

    try:
        text = await llm.complete(
            [{"role": "user", "content": prompt}],
            temperature=0.4,
            max_tokens=400,
        )
        # Confidence: average of best scores across orientations
        scores = []
        for comp in comparisons:
            if comp.schools:
                scores.append(max(s.score for s in comp.schools))
        confidence = sum(scores) / len(scores) if scores else 0.5
        return text, round(confidence, 4)
    except Exception:
        logger.exception("Failed to generate recommendation")
        return "", 0.5
