"""Pydantic API schemas for ScholarPath."""

from .chat import ChatMessage, ChatResponse
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
    WhatIfRequest,
    WhatIfResponse,
)
from .student import (
    PortfolioCompletion,
    PortfolioPreferences,
    StudentCreate,
    StudentPortfolioPatch,
    StudentPortfolioResponse,
    StudentResponse,
    StudentUpdate,
)
from .causal_data import (
    AdmissionEvidenceCreate,
    AdmissionEvidenceResponse,
    AdmissionEventCreate,
    AdmissionEventResponse,
    CausalDatasetVersionResponse,
)

__all__ = [
    # Student
    "StudentCreate",
    "StudentUpdate",
    "StudentResponse",
    "StudentPortfolioPatch",
    "StudentPortfolioResponse",
    "PortfolioPreferences",
    "PortfolioCompletion",
    "AdmissionEvidenceCreate",
    "AdmissionEvidenceResponse",
    "AdmissionEventCreate",
    "AdmissionEventResponse",
    "CausalDatasetVersionResponse",
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
    "ScenarioCompareRequest",
    "ScenarioCompareResponse",
    # Report
    "GoNoGoResponse",
    # Chat
    "ChatMessage",
    "ChatResponse",
]
