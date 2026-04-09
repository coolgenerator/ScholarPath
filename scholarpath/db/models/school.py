"""School and Program models."""

from __future__ import annotations

import enum
import uuid
from typing import TYPE_CHECKING, Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import Boolean, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from scholarpath.config import settings
from .base import Base, TimestampMixin, UUIDPrimaryKey

if TYPE_CHECKING:
    from .data_point import DataPoint
    from .metro_area import MetroAreaProfile


class SchoolType(str, enum.Enum):
    UNIVERSITY = "university"
    LAC = "lac"
    TECHNICAL = "technical"


class School(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "schools"

    name: Mapped[str] = mapped_column(String(300))
    name_cn: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    city: Mapped[str] = mapped_column(String(120))
    state: Mapped[str] = mapped_column(String(60))
    school_type: Mapped[str] = mapped_column(String(20))  # SchoolType value
    size_category: Mapped[str] = mapped_column(String(30))

    # Rankings & selectivity
    us_news_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    qs_world_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    forbes_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    acceptance_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Test score ranges
    sat_25: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sat_75: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    act_25: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    act_75: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Cost — three tuition tiers
    tuition_in_state: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tuition_oos: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tuition_intl: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    avg_net_price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Demographics & quality
    intl_student_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    student_faculty_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    graduation_rate_4yr: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    endowment_per_student: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Metro area link (Layer 3 environmental data)
    metro_area_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("metro_area_profiles.id"), nullable=True,
    )

    # Misc
    campus_setting: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    website_url: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    cds_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    metadata_: Mapped[Optional[dict]] = mapped_column("metadata", JSON, nullable=True)

    # Embedding (Gemini gemini-embedding-001 -> 3072 dims)
    embedding: Mapped[Optional[list]] = mapped_column(
        Vector(settings.EMBEDDING_DIMENSION), nullable=True
    )

    # Relationships
    metro_area: Mapped[Optional[MetroAreaProfile]] = relationship()
    programs: Mapped[list[Program]] = relationship(
        back_populates="school", cascade="all, delete-orphan"
    )
    data_points: Mapped[list[DataPoint]] = relationship(back_populates="school")


class Program(UUIDPrimaryKey, TimestampMixin, Base):
    __tablename__ = "programs"

    school_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schools.id"))
    name: Mapped[str] = mapped_column(String(300))
    department: Mapped[str] = mapped_column(String(200))

    us_news_rank: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    avg_class_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    has_research_opps: Mapped[bool] = mapped_column(Boolean, default=False)
    has_coop: Mapped[bool] = mapped_column(Boolean, default=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Relationships
    school: Mapped[School] = relationship(back_populates="programs")
