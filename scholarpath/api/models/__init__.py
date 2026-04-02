"""Pydantic API schemas for ScholarPath."""

from .evaluation import (
    EvaluationRequest,
    EvaluationResponse,
    TieredSchoolList,
)
from .offer import (
    OfferComparisonResponse,
    OfferCreate,
    OfferResponse,
    OfferUpdate,
)
from .report import GoNoGoResponse
from .school import (
    ProgramResponse,
    SchoolListResponse,
    SchoolResponse,
    SchoolSearchParams,
)
from .simulation import (
    ScenarioCompareRequest,
    ScenarioCompareResponse,
    ScenarioRequest,
    ScenarioResult,
    WhatIfRequest,
    WhatIfResponse,
)
from .student import StudentCreate, StudentResponse, StudentUpdate

__all__ = [
    # Student
    "StudentCreate",
    "StudentUpdate",
    "StudentResponse",
    # School
    "SchoolResponse",
    "SchoolListResponse",
    "SchoolSearchParams",
    "ProgramResponse",
    # Evaluation
    "EvaluationRequest",
    "EvaluationResponse",
    "TieredSchoolList",
    # Offer
    "OfferCreate",
    "OfferUpdate",
    "OfferResponse",
    "OfferComparisonResponse",
    # Simulation
    "WhatIfRequest",
    "WhatIfResponse",
    "ScenarioRequest",
    "ScenarioResult",
    "ScenarioCompareRequest",
    "ScenarioCompareResponse",
    # Report
    "GoNoGoResponse",
]
