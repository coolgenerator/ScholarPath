"""Development-only endpoints to populate the database with sample data."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.data.schools_top50 import EXTRA_SCHOOLS
from scholarpath.db.models import Offer, School, SchoolEvaluation, Student
from scholarpath.db.session import get_session
from scholarpath.llm.embeddings import get_embedding_service

router = APIRouter(prefix="/api/seed", tags=["seed"])

# ---------------------------------------------------------------------------
# Sample school data
# ---------------------------------------------------------------------------

SCHOOLS_DATA: list[dict] = [
    {
        "name": "Stanford University",
        "name_cn": "斯坦福大学",
        "city": "Stanford",
        "state": "CA",
        "school_type": "university",
        "size_category": "medium",
        "us_news_rank": 3,
        "acceptance_rate": 0.04,
        "sat_25": 1500,
        "sat_75": 1570,
        "tuition_oos": 60000,
        "avg_net_price": 18000,
        "intl_student_pct": 0.12,
        "student_faculty_ratio": 5.0,
        "graduation_rate_4yr": 0.75,
        "campus_setting": "suburban",
        "website_url": "https://www.stanford.edu",
    },
    {
        "name": "Massachusetts Institute of Technology",
        "name_cn": "麻省理工学院",
        "city": "Cambridge",
        "state": "MA",
        "school_type": "technical",
        "size_category": "medium",
        "us_news_rank": 2,
        "acceptance_rate": 0.04,
        "sat_25": 1520,
        "sat_75": 1580,
        "tuition_oos": 59000,
        "avg_net_price": 19000,
        "intl_student_pct": 0.11,
        "student_faculty_ratio": 3.0,
        "graduation_rate_4yr": 0.85,
        "campus_setting": "urban",
        "website_url": "https://www.mit.edu",
    },
    {
        "name": "Carnegie Mellon University",
        "name_cn": "卡内基梅隆大学",
        "city": "Pittsburgh",
        "state": "PA",
        "school_type": "university",
        "size_category": "medium",
        "us_news_rank": 24,
        "acceptance_rate": 0.14,
        "sat_25": 1480,
        "sat_75": 1560,
        "tuition_oos": 62000,
        "avg_net_price": 33000,
        "intl_student_pct": 0.22,
        "student_faculty_ratio": 5.0,
        "graduation_rate_4yr": 0.75,
        "campus_setting": "urban",
        "website_url": "https://www.cmu.edu",
    },
    {
        "name": "University of California, Berkeley",
        "name_cn": "加州大学伯克利分校",
        "city": "Berkeley",
        "state": "CA",
        "school_type": "university",
        "size_category": "large",
        "us_news_rank": 15,
        "acceptance_rate": 0.12,
        "sat_25": 1330,
        "sat_75": 1530,
        "tuition_oos": 44000,
        "avg_net_price": 18000,
        "intl_student_pct": 0.13,
        "student_faculty_ratio": 20.0,
        "graduation_rate_4yr": 0.76,
        "campus_setting": "urban",
        "website_url": "https://www.berkeley.edu",
    },
    {
        "name": "Georgia Institute of Technology",
        "name_cn": "佐治亚理工学院",
        "city": "Atlanta",
        "state": "GA",
        "school_type": "technical",
        "size_category": "large",
        "us_news_rank": 33,
        "acceptance_rate": 0.17,
        "sat_25": 1390,
        "sat_75": 1540,
        "tuition_oos": 33000,
        "avg_net_price": 22000,
        "intl_student_pct": 0.11,
        "student_faculty_ratio": 15.0,
        "graduation_rate_4yr": 0.44,
        "campus_setting": "urban",
        "website_url": "https://www.gatech.edu",
    },
    {
        "name": "University of Illinois Urbana-Champaign",
        "name_cn": "伊利诺伊大学厄巴纳-香槟分校",
        "city": "Champaign",
        "state": "IL",
        "school_type": "university",
        "size_category": "large",
        "us_news_rank": 35,
        "acceptance_rate": 0.45,
        "sat_25": 1310,
        "sat_75": 1510,
        "tuition_oos": 36000,
        "avg_net_price": 20000,
        "intl_student_pct": 0.16,
        "student_faculty_ratio": 20.0,
        "graduation_rate_4yr": 0.72,
        "campus_setting": "urban",
        "website_url": "https://illinois.edu",
    },
    {
        "name": "Purdue University",
        "name_cn": "普渡大学",
        "city": "West Lafayette",
        "state": "IN",
        "school_type": "university",
        "size_category": "large",
        "us_news_rank": 43,
        "acceptance_rate": 0.53,
        "sat_25": 1190,
        "sat_75": 1430,
        "tuition_oos": 28000,
        "avg_net_price": 15000,
        "intl_student_pct": 0.14,
        "student_faculty_ratio": 13.0,
        "graduation_rate_4yr": 0.54,
        "campus_setting": "suburban",
        "website_url": "https://www.purdue.edu",
    },
    {
        "name": "University of Wisconsin-Madison",
        "name_cn": "威斯康星大学麦迪逊分校",
        "city": "Madison",
        "state": "WI",
        "school_type": "university",
        "size_category": "large",
        "us_news_rank": 35,
        "acceptance_rate": 0.49,
        "sat_25": 1280,
        "sat_75": 1460,
        "tuition_oos": 39000,
        "avg_net_price": 17000,
        "intl_student_pct": 0.12,
        "student_faculty_ratio": 17.0,
        "graduation_rate_4yr": 0.64,
        "campus_setting": "urban",
        "website_url": "https://www.wisc.edu",
    },
    {
        "name": "Pennsylvania State University",
        "name_cn": "宾夕法尼亚州立大学",
        "city": "University Park",
        "state": "PA",
        "school_type": "university",
        "size_category": "large",
        "us_news_rank": 60,
        "acceptance_rate": 0.55,
        "sat_25": 1160,
        "sat_75": 1370,
        "tuition_oos": 36000,
        "avg_net_price": 22000,
        "intl_student_pct": 0.09,
        "student_faculty_ratio": 15.0,
        "graduation_rate_4yr": 0.68,
        "campus_setting": "suburban",
        "website_url": "https://www.psu.edu",
    },
    {
        "name": "Arizona State University",
        "name_cn": "亚利桑那州立大学",
        "city": "Tempe",
        "state": "AZ",
        "school_type": "university",
        "size_category": "large",
        "us_news_rank": 105,
        "acceptance_rate": 0.88,
        "sat_25": 1100,
        "sat_75": 1340,
        "tuition_oos": 32000,
        "avg_net_price": 16000,
        "intl_student_pct": 0.08,
        "student_faculty_ratio": 22.0,
        "graduation_rate_4yr": 0.49,
        "campus_setting": "urban",
        "website_url": "https://www.asu.edu",
    },
]

# Map school name to tier for the demo student
_SCHOOL_TIERS: dict[str, str] = {
    "Stanford University": "reach",
    "Massachusetts Institute of Technology": "reach",
    "Carnegie Mellon University": "target",
    "University of California, Berkeley": "target",
    "Georgia Institute of Technology": "target",
    "University of Illinois Urbana-Champaign": "target",
    "Purdue University": "safety",
    "University of Wisconsin-Madison": "safety",
    "Pennsylvania State University": "safety",
    "Arizona State University": "likely",
}


@router.post("/schools")
async def seed_schools(session: AsyncSession = Depends(get_session)):
    """Seed ~50 US schools. Idempotent -- skips schools that already exist."""
    all_schools = SCHOOLS_DATA + EXTRA_SCHOOLS
    created: list[str] = []
    for data in all_schools:
        result = await session.execute(
            select(School).where(School.name == data["name"])
        )
        if result.scalar_one_or_none() is not None:
            continue
        school = School(**data)
        session.add(school)
        created.append(data["name"])

    # Batch-embed all newly created schools
    if created:
        await session.flush()
        try:
            emb = get_embedding_service()
            result = await session.execute(
                select(School).where(School.name.in_(created))
            )
            new_schools = list(result.scalars().all())
            texts = []
            for s in new_schools:
                parts = [s.name]
                if s.name_cn:
                    parts.append(f"({s.name_cn})")
                parts.append(f"Location: {s.city}, {s.state}")
                parts.append(f"Type: {s.school_type}")
                if s.us_news_rank:
                    parts.append(f"US News Rank: #{s.us_news_rank}")
                if s.acceptance_rate:
                    parts.append(f"Acceptance rate: {s.acceptance_rate:.0%}")
                texts.append(". ".join(parts))
            vectors = await emb.embed_batch(texts, task_type="RETRIEVAL_DOCUMENT")
            for s, vec in zip(new_schools, vectors):
                s.embedding = vec
        except Exception:
            pass  # Embedding is best-effort during seeding

    return {"created": created, "count": len(created)}


@router.post("/demo-student")
async def seed_demo_student(session: AsyncSession = Depends(get_session)):
    """Create a demo student profile (Chinese student, SAT 1480, GPA 3.8, CS major).

    Idempotent -- returns existing student if email matches.
    """
    result = await session.execute(
        select(Student).where(Student.email == "demo@scholarpath.dev")
    )
    existing = result.scalar_one_or_none()
    if existing is not None:
        return {"student_id": str(existing.id), "status": "already_exists"}

    student = Student(
        name="Demo Student",
        email="demo@scholarpath.dev",
        gpa=3.8,
        gpa_scale="4.0",
        sat_total=1480,
        sat_rw=730,
        sat_math=750,
        toefl_total=108,
        curriculum_type="AP",
        ap_courses=[
            "AP Computer Science A",
            "AP Calculus BC",
            "AP Physics C: Mechanics",
            "AP Statistics",
            "AP English Language",
        ],
        extracurriculars={
            "activities": [
                "Robotics Club (Captain)",
                "Math Olympiad (Regional Finalist)",
                "Open Source Contributor (GitHub)",
                "Volunteer Tutoring",
            ]
        },
        awards={
            "list": [
                "USACO Silver Division",
                "Regional Science Fair 2nd Place",
                "National Merit Semifinalist",
            ]
        },
        intended_majors=["Computer Science", "Data Science"],
        budget_usd=55000,
        need_financial_aid=True,
        preferences={
            "region": ["West Coast", "Northeast"],
            "campus_size": "medium-to-large",
            "research_opportunities": True,
        },
        target_year=2027,
        profile_completed=True,
    )
    session.add(student)
    await session.flush()

    # Generate profile embedding for demo student
    try:
        emb = get_embedding_service()
        profile_data = {
            "intended_majors": student.intended_majors,
            "gpa": student.gpa,
            "gpa_scale": student.gpa_scale,
            "sat_total": student.sat_total,
            "extracurriculars": student.extracurriculars,
            "awards": student.awards,
            "preferences": student.preferences,
            "budget_usd": student.budget_usd,
        }
        student.profile_embedding = await emb.embed_student_profile(profile_data)
        await session.flush()
    except Exception:
        pass  # Embedding is best-effort

    return {"student_id": str(student.id), "status": "created"}


@router.post("/demo-evaluations")
async def seed_demo_evaluations(session: AsyncSession = Depends(get_session)):
    """Create evaluations for the demo student against all seeded schools."""
    # Find the demo student
    result = await session.execute(
        select(Student).where(Student.email == "demo@scholarpath.dev")
    )
    student = result.scalar_one_or_none()
    if student is None:
        return {"error": "Demo student not found. Seed the demo student first."}

    # Find all seeded schools
    result = await session.execute(select(School))
    schools = result.scalars().all()
    if not schools:
        return {"error": "No schools found. Seed schools first."}

    # Pre-built evaluation data keyed by tier
    _EVAL_TEMPLATES: dict[str, dict] = {
        "reach": {
            "academic_fit": 0.82,
            "financial_fit": 0.55,
            "career_fit": 0.95,
            "life_fit": 0.80,
            "overall_score": 0.78,
            "admission_probability": 0.08,
            "ed_ea_recommendation": "rea",
            "reasoning": (
                "Highly competitive program with world-class CS research. "
                "Student's SAT score is below the 25th percentile for this school. "
                "Financial aid for international students is limited."
            ),
        },
        "target": {
            "academic_fit": 0.88,
            "financial_fit": 0.70,
            "career_fit": 0.90,
            "life_fit": 0.82,
            "overall_score": 0.83,
            "admission_probability": 0.35,
            "ed_ea_recommendation": "ea",
            "reasoning": (
                "Strong CS program with good placement outcomes. "
                "Student's profile is competitive for admission. "
                "Moderate financial aid available for international students."
            ),
        },
        "safety": {
            "academic_fit": 0.75,
            "financial_fit": 0.82,
            "career_fit": 0.78,
            "life_fit": 0.76,
            "overall_score": 0.78,
            "admission_probability": 0.75,
            "ed_ea_recommendation": "rd",
            "reasoning": (
                "Solid engineering program with good industry connections. "
                "Student's test scores are well above average for this school. "
                "Reasonable tuition with scholarship potential."
            ),
        },
        "likely": {
            "academic_fit": 0.65,
            "financial_fit": 0.88,
            "career_fit": 0.68,
            "life_fit": 0.72,
            "overall_score": 0.72,
            "admission_probability": 0.92,
            "ed_ea_recommendation": "rd",
            "reasoning": (
                "Large program with many CS students. "
                "Very high admission probability given student's profile. "
                "Good value with competitive tuition."
            ),
        },
    }

    created: list[str] = []
    for school in schools:
        # Skip if evaluation already exists
        existing = await session.execute(
            select(SchoolEvaluation).where(
                SchoolEvaluation.student_id == student.id,
                SchoolEvaluation.school_id == school.id,
            )
        )
        if existing.scalar_one_or_none() is not None:
            continue

        tier = _SCHOOL_TIERS.get(school.name, "target")
        template = _EVAL_TEMPLATES[tier]

        evaluation = SchoolEvaluation(
            student_id=student.id,
            school_id=school.id,
            tier=tier,
            **template,
        )
        session.add(evaluation)
        created.append(school.name)

    return {"created": created, "count": len(created)}


# ---------------------------------------------------------------------------
# Demo offers -- realistic admission results for the demo student
# ---------------------------------------------------------------------------

_DEMO_OFFERS: list[dict] = [
    {
        "school_name": "University of Illinois Urbana-Champaign",
        "status": "admitted",
        "tuition": 36000,
        "room_and_board": 12500,
        "books_supplies": 1200,
        "personal_expenses": 2400,
        "transportation": 1800,
        "merit_scholarship": 8000,
        "need_based_grant": 5000,
        "loan_offered": 5500,
        "work_study": 2000,
        "honors_program": True,
        "notes": "Admitted to CS + Grainger Engineering. Honors college invitation included.",
        "decision_deadline": "2027-05-01",
    },
    {
        "school_name": "Purdue University",
        "status": "admitted",
        "tuition": 28000,
        "room_and_board": 11200,
        "books_supplies": 1100,
        "personal_expenses": 2200,
        "transportation": 1500,
        "merit_scholarship": 12000,
        "need_based_grant": 3000,
        "loan_offered": 5500,
        "work_study": 0,
        "honors_program": True,
        "notes": "Admitted to Computer Science in College of Science. Presidential Scholarship awarded.",
        "decision_deadline": "2027-05-01",
    },
    {
        "school_name": "University of Wisconsin-Madison",
        "status": "admitted",
        "tuition": 39000,
        "room_and_board": 13000,
        "books_supplies": 1100,
        "personal_expenses": 2600,
        "transportation": 1400,
        "merit_scholarship": 6000,
        "need_based_grant": 4000,
        "loan_offered": 5500,
        "work_study": 1500,
        "honors_program": False,
        "notes": "Admitted to Letters & Science, pre-CS track. Can declare CS major after prerequisites.",
        "decision_deadline": "2027-05-01",
    },
    {
        "school_name": "Georgia Institute of Technology",
        "status": "waitlisted",
        "tuition": 33000,
        "room_and_board": 12800,
        "books_supplies": 1000,
        "personal_expenses": 2200,
        "transportation": 1600,
        "merit_scholarship": 0,
        "need_based_grant": 0,
        "loan_offered": 0,
        "work_study": 0,
        "honors_program": False,
        "notes": "Placed on waitlist for College of Computing. Will hear back by mid-April.",
    },
    {
        "school_name": "Carnegie Mellon University",
        "status": "deferred",
        "tuition": 62000,
        "room_and_board": 17000,
        "books_supplies": 1200,
        "personal_expenses": 2800,
        "transportation": 1500,
        "merit_scholarship": 0,
        "need_based_grant": 0,
        "loan_offered": 0,
        "work_study": 0,
        "honors_program": False,
        "notes": "EA application deferred to Regular Decision pool. Decision expected late March.",
    },
    {
        "school_name": "Arizona State University",
        "status": "admitted",
        "tuition": 32000,
        "room_and_board": 13500,
        "books_supplies": 1000,
        "personal_expenses": 2000,
        "transportation": 1200,
        "merit_scholarship": 15000,
        "need_based_grant": 2000,
        "loan_offered": 5500,
        "work_study": 2500,
        "honors_program": True,
        "notes": "Barrett Honors College admission. New American University Scholar award.",
        "decision_deadline": "2027-05-01",
    },
    {
        "school_name": "Pennsylvania State University",
        "status": "admitted",
        "tuition": 36000,
        "room_and_board": 12800,
        "books_supplies": 1100,
        "personal_expenses": 2400,
        "transportation": 1600,
        "merit_scholarship": 5000,
        "need_based_grant": 3000,
        "loan_offered": 5500,
        "work_study": 1800,
        "honors_program": False,
        "notes": "Admitted to College of Engineering, Computer Science major at University Park.",
        "decision_deadline": "2027-05-01",
    },
]


@router.post("/demo-offers")
async def seed_demo_offers(session: AsyncSession = Depends(get_session)):
    """Create realistic admission offers for the demo student. Idempotent."""
    from datetime import date as date_type

    # Find demo student
    result = await session.execute(
        select(Student).where(Student.email == "demo@scholarpath.dev")
    )
    student = result.scalar_one_or_none()
    if student is None:
        return {"error": "Demo student not found. Seed the demo student first."}

    # Build school name -> id lookup
    result = await session.execute(select(School))
    schools = {s.name: s for s in result.scalars().all()}
    if not schools:
        return {"error": "No schools found. Seed schools first."}

    created: list[str] = []
    for offer_data in _DEMO_OFFERS:
        school_name = offer_data["school_name"]
        school = schools.get(school_name)
        if school is None:
            continue

        # Skip if offer already exists for this student+school
        existing = await session.execute(
            select(Offer).where(
                Offer.student_id == student.id,
                Offer.school_id == school.id,
            )
        )
        if existing.scalar_one_or_none() is not None:
            continue

        total_aid = (
            offer_data["merit_scholarship"]
            + offer_data["need_based_grant"]
            + offer_data["loan_offered"]
            + offer_data.get("work_study", 0)
        )
        cost_parts = [
            offer_data.get("tuition"),
            offer_data.get("room_and_board"),
            offer_data.get("books_supplies"),
            offer_data.get("personal_expenses"),
            offer_data.get("transportation"),
        ]
        known_costs = [p for p in cost_parts if p is not None]
        total_cost = sum(known_costs) if known_costs else None
        net_cost = total_cost - total_aid if total_cost is not None else None

        deadline = None
        if offer_data.get("decision_deadline"):
            deadline = date_type.fromisoformat(offer_data["decision_deadline"])

        offer = Offer(
            student_id=student.id,
            school_id=school.id,
            status=offer_data["status"],
            tuition=offer_data.get("tuition"),
            room_and_board=offer_data.get("room_and_board"),
            books_supplies=offer_data.get("books_supplies"),
            personal_expenses=offer_data.get("personal_expenses"),
            transportation=offer_data.get("transportation"),
            merit_scholarship=offer_data["merit_scholarship"],
            need_based_grant=offer_data["need_based_grant"],
            loan_offered=offer_data["loan_offered"],
            work_study=offer_data.get("work_study", 0),
            total_aid=total_aid,
            total_cost=total_cost,
            net_cost=net_cost,
            honors_program=offer_data["honors_program"],
            notes=offer_data["notes"],
            decision_deadline=deadline,
        )
        session.add(offer)
        created.append(school_name)

    return {"created": created, "count": len(created)}
