from __future__ import annotations

import pytest
from sqlalchemy import func, select

from scholarpath.db.models import School, SchoolExternalId
from scholarpath.search.sources.ipeds_college_navigator import IPEDSCollegeNavigatorSource
from scholarpath.services import causal_data_service


def _new_school(*, name: str, state: str, website_url: str | None = None) -> School:
    return School(
        name=name,
        city="Test City",
        state=state,
        school_type="university",
        size_category="large",
        website_url=website_url,
    )


@pytest.mark.asyncio
async def test_map_ipeds_external_ids_match_modes_and_skip_reasons(session, monkeypatch):
    existing = _new_school(name="Existing U", state="IL", website_url="https://existing.edu")
    foo = _new_school(name="Foo University", state="CA", website_url="https://foo.edu")
    bar = _new_school(name="Bar College", state="NY")
    fuzzy = _new_school(name="University of Californi Berkeley", state="CA")
    cross_state = _new_school(name="Mercy College", state="MA")
    conflict = _new_school(name="Saint Marys College", state="CA")
    low_conf = _new_school(name="Random Institute", state="TX")
    session.add_all([existing, foo, bar, fuzzy, cross_state, conflict, low_conf])
    await session.flush()

    session.add(
        SchoolExternalId(
            school_id=existing.id,
            provider="ipeds",
            external_id="1000",
            is_primary=True,
            match_method="seed",
            confidence=0.99,
            metadata_={},
        )
    )
    await session.commit()

    institution_rows = [
        {
            "external_id": "2001",
            "school_name": "Foo University",
            "state": "CA",
            "city": "City",
            "website_url": "https://foo.edu",
        },
        {
            "external_id": "2002",
            "school_name": "Bar College",
            "state": "NY",
            "city": "City",
            "website_url": "",
        },
        {
            "external_id": "2003",
            "school_name": "University of California Berkeley",
            "state": "CA",
            "city": "City",
            "website_url": "",
        },
        {
            "external_id": "2004",
            "school_name": "Mercy College",
            "state": "NY",
            "city": "City",
            "website_url": "",
        },
        {
            "external_id": "2005",
            "school_name": "Saint Marys College",
            "state": "CA",
            "city": "City",
            "website_url": "",
        },
        {
            "external_id": "2006",
            "school_name": "Saint Marys College",
            "state": "CA",
            "city": "City",
            "website_url": "",
        },
        {
            "external_id": "2007",
            "school_name": "Random Technical College",
            "state": "TX",
            "city": "City",
            "website_url": "",
        },
    ]

    async def _fake_list_institutions(self: IPEDSCollegeNavigatorSource) -> list[dict]:
        return [dict(item) for item in institution_rows]

    monkeypatch.setattr(
        IPEDSCollegeNavigatorSource,
        "list_institutions",
        _fake_list_institutions,
    )

    report = await causal_data_service.map_ipeds_external_ids(
        session,
        run_id="map-dry-run",
        dry_run=True,
        fuzzy_threshold=0.88,
        school_names=[
            "Existing U",
            "Foo University",
            "Bar College",
            "University of Californi Berkeley",
            "Mercy College",
            "Saint Marys College",
            "Random Institute",
        ],
    )

    assert report["schools_scanned"] == 7
    assert report["mapped"] == 3
    assert report["skipped_existing"] == 1
    assert report["skipped_cross_state"] == 1
    assert report["skipped_conflict"] == 1
    assert report["skipped_low_confidence"] == 1
    assert report["match_method_counts"]["domain_exact"] == 1
    assert report["match_method_counts"]["name_state_exact"] == 1
    assert report["match_method_counts"]["name_state_fuzzy"] == 1

    mapping_count = int(
        (
            await session.scalar(
                select(func.count()).select_from(SchoolExternalId).where(SchoolExternalId.provider == "ipeds")
            )
        )
        or 0
    )
    assert mapping_count == 1


@pytest.mark.asyncio
async def test_map_ipeds_external_ids_apply_is_idempotent(session, monkeypatch):
    school = _new_school(name="Domain University", state="CA", website_url="https://domainu.edu")
    session.add(school)
    await session.commit()

    async def _fake_list_institutions(self: IPEDSCollegeNavigatorSource) -> list[dict]:
        return [
            {
                "external_id": "3001",
                "school_name": "Domain University",
                "state": "CA",
                "city": "City",
                "website_url": "https://domainu.edu",
            }
        ]

    monkeypatch.setattr(
        IPEDSCollegeNavigatorSource,
        "list_institutions",
        _fake_list_institutions,
    )

    first = await causal_data_service.map_ipeds_external_ids(
        session,
        run_id="map-apply-1",
        dry_run=False,
        fuzzy_threshold=0.88,
        school_names=["Domain University"],
    )
    await session.commit()

    second = await causal_data_service.map_ipeds_external_ids(
        session,
        run_id="map-apply-2",
        dry_run=False,
        fuzzy_threshold=0.88,
        school_names=["Domain University"],
    )
    await session.commit()

    mapping_rows = (
        (
            await session.execute(
                select(SchoolExternalId).where(
                    SchoolExternalId.provider == "ipeds",
                    SchoolExternalId.school_id == school.id,
                )
            )
        )
        .scalars()
        .all()
    )
    assert first["mapped"] == 1
    assert second["mapped"] == 0
    assert second["skipped_existing"] == 1
    assert len(mapping_rows) == 1
    assert mapping_rows[0].external_id == "3001"
