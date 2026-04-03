"""Deprecated compatibility routes for causal data APIs.

Write operations moved to `/api/students/{student_id}/...` so there is a
single authoritative mutation path.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from scholarpath.api.deps import SessionDep
from scholarpath.api.models.causal_data import (
    AdmissionEvidenceCreate,
    AdmissionEvidenceResponse,
    AdmissionEventCreate,
    AdmissionEventResponse,
    CausalDatasetVersionResponse,
)
from scholarpath.db.models import CausalDatasetVersion

router = APIRouter(prefix="/causal-data", tags=["causal-data"])
_SUNSET = "Wed, 01 Oct 2026 00:00:00 GMT"


def _deprecated_headers(*, successor_path: str) -> dict[str, str]:
    return {
        "Deprecation": "true",
        "Sunset": _SUNSET,
        "Link": f'<{successor_path}>; rel="successor-version"',
    }


@router.post(
    "/students/{student_id}/admission-evidence",
    response_model=AdmissionEvidenceResponse,
    status_code=status.HTTP_201_CREATED,
    deprecated=True,
)
async def create_admission_evidence(
    student_id: uuid.UUID,
    _payload: AdmissionEvidenceCreate,
    _session: SessionDep,
) -> dict:
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail=(
            "Deprecated write endpoint. Use "
            f"/api/students/{student_id}/admission-evidence"
        ),
        headers=_deprecated_headers(
            successor_path=f"/api/students/{student_id}/admission-evidence",
        ),
    )


@router.post(
    "/students/{student_id}/admission-events",
    response_model=AdmissionEventResponse,
    status_code=status.HTTP_201_CREATED,
    deprecated=True,
)
async def create_admission_event(
    student_id: uuid.UUID,
    _payload: AdmissionEventCreate,
    _session: SessionDep,
) -> dict:
    raise HTTPException(
        status_code=status.HTTP_410_GONE,
        detail=(
            "Deprecated write endpoint. Use "
            f"/api/students/{student_id}/admission-events"
        ),
        headers=_deprecated_headers(
            successor_path=f"/api/students/{student_id}/admission-events",
        ),
    )


@router.get(
    "/datasets/{version}",
    response_model=CausalDatasetVersionResponse,
)
async def get_dataset_version(version: str, session: SessionDep) -> CausalDatasetVersion:
    row = await session.scalar(
        select(CausalDatasetVersion).where(CausalDatasetVersion.version == version),
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset version '{version}' not found",
        )
    return row
