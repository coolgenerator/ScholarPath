"""MetroAreaProfile model -- city/region-level environmental data (Layer 3)."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import BigInteger, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin, UUIDPrimaryKey


class MetroAreaProfile(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "metro_area_profiles"
    __table_args__ = (
        UniqueConstraint("city", "state", "data_year", name="uq_metro_city_state_year"),
    )

    # Lookup key — matches School.city / School.state
    city: Mapped[str] = mapped_column(String(120))
    state: Mapped[str] = mapped_column(String(60))

    # ── Layer 3 environmental data ──────────────────────────────────────

    # Tech ecosystem
    tech_employer_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
    )  # BLS QCEW NAICS 5112 + 5415
    vc_investment_usd: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True,
    )  # NVCA yearbook / PitchBook public metro totals

    # Cost & quality of life
    cost_of_living_index: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True,
    )  # BLS C2ER — national avg = 100
    safety_index: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True,
    )  # FBI UCR inverted crime rate, 0-1 (1 = safest)
    median_household_income: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
    )  # Census ACS table B19013

    # Demographics
    asian_population_pct: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True,
    )  # Census ACS table B02001

    # Climate
    climate_zone: Mapped[Optional[str]] = mapped_column(
        String(40), nullable=True,
    )  # NOAA climate zone classification

    # Proximity to industry hubs
    finance_hub_distance_km: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True,
    )  # Haversine distance to nearest of NYC / Chicago / SF

    # Research ecosystem
    federal_lab_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
    )  # DOE FFRDC directory — labs within 50 mi
    nsf_funding_total: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True,
    )  # NSF HERD survey — total R&D in metro (USD)

    # Data vintage
    data_year: Mapped[int] = mapped_column(Integer)
