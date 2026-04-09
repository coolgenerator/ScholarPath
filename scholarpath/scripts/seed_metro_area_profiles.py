"""Seed metro_area_profiles table and link schools.

Usage:
    python -m scholarpath.scripts.seed_metro_area_profiles [--dry-run]
"""

from __future__ import annotations

import asyncio
import csv
import sys
import uuid
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from scholarpath.db.models import MetroAreaProfile, School
from scholarpath.db.session import async_session_factory

CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "metro_area_profiles.csv"

# Numeric columns and their Python types
_FLOAT_COLS = {"cost_of_living_index", "safety_index", "asian_population_pct", "finance_hub_distance_km"}
_INT_COLS = {
    "tech_employer_count", "vc_investment_usd", "median_household_income",
    "federal_lab_count", "nsf_funding_total", "data_year",
}


def _parse_row(row: dict[str, str]) -> dict:
    """Convert CSV string values to typed values."""
    out: dict = {}
    for key, val in row.items():
        val = val.strip()
        if not val:
            out[key] = None
        elif key in _INT_COLS:
            out[key] = int(val)
        elif key in _FLOAT_COLS:
            out[key] = float(val)
        else:
            out[key] = val
    return out


async def seed(dry_run: bool = False) -> None:
    rows = []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(_parse_row(row))

    print(f"Loaded {len(rows)} metro area rows from CSV")

    async with async_session_factory() as session:
        # --- Upsert metro area profiles ---
        inserted = 0
        updated = 0
        for row in rows:
            stmt = (
                pg_insert(MetroAreaProfile)
                .values(id=uuid.uuid4(), **row)
                .on_conflict_do_update(
                    constraint="uq_metro_city_state_year",
                    set_={
                        k: v for k, v in row.items()
                        if k not in ("city", "state", "data_year")
                    },
                )
            )
            if dry_run:
                print(f"  [dry-run] upsert: {row['city']}, {row['state']}")
            else:
                result = await session.execute(stmt)
                if result.rowcount:  # type: ignore[union-attr]
                    inserted += 1

        if not dry_run:
            await session.flush()
            print(f"Upserted {inserted} metro area profiles")

        # --- Link schools to metro areas ---
        metro_lookup: dict[tuple[str, str], uuid.UUID] = {}
        metro_rows = await session.execute(select(MetroAreaProfile))
        for m in metro_rows.scalars():
            metro_lookup[(m.city.lower(), m.state.lower())] = m.id

        school_rows = await session.execute(select(School))
        linked = 0
        unlinked = []
        for school in school_rows.scalars():
            key = (school.city.lower(), school.state.lower())
            metro_id = metro_lookup.get(key)
            if metro_id:
                if dry_run:
                    print(f"  [dry-run] link: {school.name} -> {school.city}, {school.state}")
                else:
                    await session.execute(
                        update(School)
                        .where(School.id == school.id)
                        .values(metro_area_id=metro_id)
                    )
                linked += 1
            else:
                unlinked.append(f"{school.name} ({school.city}, {school.state})")

        if not dry_run:
            await session.commit()

        print(f"Linked {linked} schools to metro areas")
        if unlinked:
            print(f"Unlinked schools ({len(unlinked)}):")
            for name in unlinked:
                print(f"  - {name}")


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN MODE ===")
    asyncio.run(seed(dry_run=dry_run))


if __name__ == "__main__":
    main()
