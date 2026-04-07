"""Create a deterministic fixture for landing-page screenshot capture."""

from __future__ import annotations

import asyncio
import json

from sqlalchemy import delete, select

from scholarpath.db.models import GoNoGoReport, Offer, School, SchoolEvaluation, Student
from scholarpath.db.session import async_session_factory

LANDING_CAPTURE_EMAIL = "landing-capture@scholarpath.dev"
LANDING_CAPTURE_NAME = "Luna Chen"

LANDING_CAPTURE_STUDENT = {
    "name": LANDING_CAPTURE_NAME,
    "email": LANDING_CAPTURE_EMAIL,
    "gpa": 3.86,
    "gpa_scale": "4.0",
    "sat_total": 1510,
    "sat_rw": 740,
    "sat_math": 770,
    "toefl_total": 111,
    "curriculum_type": "IB",
    "ap_courses": [],
    "extracurriculars": {
        "activities": [
            "国际学校 CS Club 联合创始人",
            "机器人队程序负责人",
            "AI + 教育公益项目志愿者",
            "EdTech 产品实习",
        ]
    },
    "awards": {
        "list": [
            "USACO Gold",
            "AMC 12 Distinction",
            "Hackathon 最佳产品奖",
        ]
    },
    "intended_majors": ["Computer Science", "Data Science"],
    "budget_usd": 70000,
    "need_financial_aid": True,
    "preferences": {
        "region": ["Midwest", "West Coast", "Northeast"],
        "campus_size": "medium-to-large",
        "research_opportunities": True,
        "internship_focus": True,
        "ui_preference_tags": ["landing-capture"],
    },
    "ed_preference": None,
    "target_year": 2027,
    "profile_completed": True,
}

LANDING_EVALUATION_TIERS: dict[str, str] = {
    "Stanford University": "reach",
    "Massachusetts Institute of Technology": "reach",
    "Carnegie Mellon University": "reach",
    "University of California, Berkeley": "target",
    "Georgia Institute of Technology": "target",
    "University of Illinois Urbana-Champaign": "target",
    "Purdue University": "safety",
    "University of Wisconsin-Madison": "safety",
    "Pennsylvania State University": "safety",
    "Arizona State University": "likely",
}

LANDING_EVALUATION_TEMPLATES: dict[str, dict[str, object]] = {
    "reach": {
        "academic_fit": 0.93,
        "financial_fit": 0.58,
        "career_fit": 0.96,
        "life_fit": 0.83,
        "overall_score": 0.84,
        "admission_probability": 0.12,
        "ed_ea_recommendation": "rea",
        "reasoning": "顶尖 CS 资源与科研机会非常强，但录取门槛高，预算与奖助学金不确定性也更大。",
        "fit_details": {
            "research_depth": 0.94,
            "internship_access": 0.9,
            "budget_pressure": 0.42,
            "selectivity_risk": 0.32,
        },
    },
    "target": {
        "academic_fit": 0.89,
        "financial_fit": 0.76,
        "career_fit": 0.92,
        "life_fit": 0.84,
        "overall_score": 0.86,
        "admission_probability": 0.39,
        "ed_ea_recommendation": "ea",
        "reasoning": "专业实力、实习去向和预算平衡都比较适合，属于值得重点保留的主力匹配层。",
        "fit_details": {
            "research_depth": 0.86,
            "internship_access": 0.88,
            "scholarship_friendliness": 0.73,
            "admission_risk": 0.56,
        },
    },
    "safety": {
        "academic_fit": 0.8,
        "financial_fit": 0.86,
        "career_fit": 0.82,
        "life_fit": 0.78,
        "overall_score": 0.81,
        "admission_probability": 0.74,
        "ed_ea_recommendation": "rd",
        "reasoning": "录取把握更高、预算更稳，是组合里负责兜底和控制风险的关键层。",
        "fit_details": {
            "research_depth": 0.72,
            "internship_access": 0.79,
            "scholarship_friendliness": 0.84,
            "admission_risk": 0.82,
        },
    },
    "likely": {
        "academic_fit": 0.73,
        "financial_fit": 0.92,
        "career_fit": 0.76,
        "life_fit": 0.77,
        "overall_score": 0.77,
        "admission_probability": 0.9,
        "ed_ea_recommendation": "rd",
        "reasoning": "性价比和奖学金空间更友好，适合作为预算安全垫与结果稳定器。",
        "fit_details": {
            "research_depth": 0.61,
            "internship_access": 0.72,
            "scholarship_friendliness": 0.91,
            "admission_risk": 0.95,
        },
    },
}

LANDING_OFFERS: list[dict[str, object]] = [
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
        "notes": "CS + Grainger Engineering 录取，项目强度与就业回报都很亮眼。",
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
        "notes": "Computer Science 录取并附带奖学金，预算压力明显更低。",
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
        "notes": "项目与校园体验均衡，但总花费略高于预期。",
        "decision_deadline": "2027-05-01",
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
        "notes": "Barrett Honors College + 奖学金，预算安全感最强。",
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
        "notes": "工程学院录取，整体稳健但亮点不如前几所。",
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
        "notes": "候补中，仍具吸引力但结果不确定。",
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
        "notes": "EA 延期到 RD，值得观察但不适合作为主决策对象。",
    },
]


async def main() -> None:
    required_school_names = list(LANDING_EVALUATION_TIERS.keys())

    async with async_session_factory() as session:
        student_result = await session.execute(
            select(Student).where(Student.email == LANDING_CAPTURE_EMAIL).limit(1)
        )
        student = student_result.scalar_one_or_none()

        if student is None:
            student = Student(**LANDING_CAPTURE_STUDENT)
            session.add(student)
            await session.flush()
        else:
            for key, value in LANDING_CAPTURE_STUDENT.items():
                setattr(student, key, value)
            await session.flush()

        schools_result = await session.execute(
            select(School).where(School.name.in_(required_school_names))
        )
        schools = {school.name: school for school in schools_result.scalars().all()}
        missing_schools = sorted(set(required_school_names) - set(schools))
        if missing_schools:
            raise RuntimeError(
                "Landing capture fixture is missing seeded schools: "
                + ", ".join(missing_schools)
            )

        await session.execute(delete(GoNoGoReport).where(GoNoGoReport.student_id == student.id))
        await session.execute(delete(Offer).where(Offer.student_id == student.id))
        await session.execute(delete(SchoolEvaluation).where(SchoolEvaluation.student_id == student.id))
        await session.flush()

        for school_name, tier in LANDING_EVALUATION_TIERS.items():
            school = schools[school_name]
            session.add(
                SchoolEvaluation(
                    student_id=student.id,
                    school_id=school.id,
                    tier=tier,
                    **LANDING_EVALUATION_TEMPLATES[tier],
                )
            )

        from datetime import date as date_type

        for offer_data in LANDING_OFFERS:
            school = schools.get(str(offer_data["school_name"]))
            if school is None:
                continue

            total_aid = (
                int(offer_data["merit_scholarship"])
                + int(offer_data["need_based_grant"])
                + int(offer_data["loan_offered"])
                + int(offer_data.get("work_study", 0))
            )
            cost_parts = [
                offer_data.get("tuition"),
                offer_data.get("room_and_board"),
                offer_data.get("books_supplies"),
                offer_data.get("personal_expenses"),
                offer_data.get("transportation"),
            ]
            known_costs = [int(value) for value in cost_parts if value is not None]
            total_cost = sum(known_costs) if known_costs else None
            net_cost = total_cost - total_aid if total_cost is not None else None
            deadline = (
                date_type.fromisoformat(str(offer_data["decision_deadline"]))
                if offer_data.get("decision_deadline")
                else None
            )

            session.add(
                Offer(
                    student_id=student.id,
                    school_id=school.id,
                    status=str(offer_data["status"]),
                    tuition=offer_data.get("tuition"),
                    room_and_board=offer_data.get("room_and_board"),
                    books_supplies=offer_data.get("books_supplies"),
                    personal_expenses=offer_data.get("personal_expenses"),
                    transportation=offer_data.get("transportation"),
                    merit_scholarship=int(offer_data["merit_scholarship"]),
                    need_based_grant=int(offer_data["need_based_grant"]),
                    loan_offered=int(offer_data["loan_offered"]),
                    work_study=int(offer_data.get("work_study", 0)),
                    total_aid=total_aid,
                    total_cost=total_cost,
                    net_cost=net_cost,
                    honors_program=bool(offer_data["honors_program"]),
                    notes=str(offer_data["notes"]),
                    decision_deadline=deadline,
                )
            )

        await session.commit()

        print(
            json.dumps(
                {
                    "student_id": str(student.id),
                    "student_name": student.name,
                    "seeded_evaluations": len(LANDING_EVALUATION_TIERS),
                    "seeded_offers": len(LANDING_OFFERS),
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
