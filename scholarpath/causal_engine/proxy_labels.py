"""Proxy label generation for outcomes without direct ground truth."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from scholarpath.db.models import Offer, OfferStatus, School, Student


def _clip01(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


@dataclass(slots=True)
class OutcomeLabel:
    outcome_name: str
    outcome_value: float
    label_type: str
    label_confidence: float
    source: str
    metadata: dict
    observed_at: datetime


def build_proxy_labels(
    *,
    student: Student,
    school: School | None,
    offer: Offer | None,
) -> list[OutcomeLabel]:
    now = datetime.now(UTC)
    labels: list[OutcomeLabel] = []

    # Admission uses real offer status when available.
    if offer and offer.status:
        admitted = 1.0 if offer.status in (OfferStatus.ADMITTED.value, OfferStatus.COMMITTED.value) else 0.0
        labels.append(
            OutcomeLabel(
                outcome_name="admission_probability",
                outcome_value=admitted,
                label_type="true",
                label_confidence=0.98,
                source="offer_status",
                metadata={"offer_id": str(offer.id), "offer_status": offer.status},
                observed_at=now,
            )
        )

    if school is None:
        return labels

    grad_rate = _clip01(float(school.graduation_rate_4yr or 0.5))
    endowment_norm = _clip01(float((school.endowment_per_student or 100_000) / 1_000_000))
    intl_norm = _clip01(float(school.intl_student_pct or 0.1))
    net_price_norm = _clip01(float((school.avg_net_price or 45_000) / 90_000))
    budget_norm = _clip01(float((student.budget_usd or 30_000) / 100_000))
    affordability = _clip01(1.0 - max(0.0, net_price_norm - budget_norm))
    location_score = 0.7 if (school.campus_setting or "").lower() in ("urban", "suburban") else 0.5

    academic_proxy = _clip01(0.65 * grad_rate + 0.35 * (1.0 - net_price_norm))
    career_proxy = _clip01(0.45 * grad_rate + 0.35 * endowment_norm + 0.20 * intl_norm)
    life_proxy = _clip01(0.45 * affordability + 0.35 * location_score + 0.20 * grad_rate)
    phd_proxy = _clip01(0.55 * academic_proxy + 0.45 * endowment_norm)

    labels.extend(
        [
            OutcomeLabel(
                outcome_name="academic_outcome",
                outcome_value=academic_proxy,
                label_type="proxy",
                label_confidence=0.67,
                source="school_public_metrics",
                metadata={"school_id": str(school.id)},
                observed_at=now,
            ),
            OutcomeLabel(
                outcome_name="career_outcome",
                outcome_value=career_proxy,
                label_type="proxy",
                label_confidence=0.63,
                source="school_public_metrics",
                metadata={"school_id": str(school.id)},
                observed_at=now,
            ),
            OutcomeLabel(
                outcome_name="life_satisfaction",
                outcome_value=life_proxy,
                label_type="proxy",
                label_confidence=0.56,
                source="school_public_metrics",
                metadata={"school_id": str(school.id)},
                observed_at=now,
            ),
            OutcomeLabel(
                outcome_name="phd_probability",
                outcome_value=phd_proxy,
                label_type="proxy",
                label_confidence=0.6,
                source="school_public_metrics",
                metadata={"school_id": str(school.id)},
                observed_at=now,
            ),
        ]
    )
    return labels
