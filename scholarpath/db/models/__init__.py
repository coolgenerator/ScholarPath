"""ScholarPath database models -- re-exports for convenience."""

from .base import Base, TimestampMixin, UUIDPrimaryKey
from .career import CareerOutcomeProxy, OutcomeType
from .causal import CausalContext, CausalGraph
from .causal_data import (
    AdmissionEvent,
    CanonicalFact,
    CausalDatasetVersion,
    CausalFeatureSnapshot,
    CausalModelRegistry,
    CausalOutcomeEvent,
    CausalShadowComparison,
    CausalTrendSignal,
    EvidenceArtifact,
    FactLineage,
    FactQuarantine,
    SchoolExternalId,
)
from .metro_area import MetroAreaProfile
from .conflict import Conflict, ResolutionStatus, Severity
from .data_point import DataPoint, SourceType
from .evaluation import SchoolEvaluation, Tier
from .offer import Offer, OfferStatus
from .report import GoNoGoReport, Recommendation
from .school import Program, School, SchoolType
from .student import CurriculumType, DegreeLevel, Student
from .token_usage import TokenUsage
from .user import User
from .chat_session import ChatSession
from .community_review import CommunityReview, SchoolCommunityReport
from .school_claims import SchoolClaims
from .admission_pipeline import (
    DocumentChunk,
    Institution,
    PolicyFact,
    PolicyFactAudit,
    RawDocument,
    RawSourceSnapshot,
    RawStructuredRecord,
    SchoolMetricsYear,
    SourceEntityMap,
)

__all__ = [
    "Base",
    "TimestampMixin",
    "UUIDPrimaryKey",
    # Auth
    "User",
    # Core entities
    "Student",
    "CurriculumType",
    "DegreeLevel",
    "School",
    "SchoolType",
    "Program",
    "MetroAreaProfile",
    # Data layer
    "DataPoint",
    "SourceType",
    "Conflict",
    "Severity",
    "ResolutionStatus",
    # Analysis
    "CareerOutcomeProxy",
    "OutcomeType",
    "CausalGraph",
    "CausalContext",
    "CausalFeatureSnapshot",
    "CausalOutcomeEvent",
    "CausalModelRegistry",
    "CausalShadowComparison",
    "EvidenceArtifact",
    "SchoolExternalId",
    "AdmissionEvent",
    "CanonicalFact",
    "FactLineage",
    "FactQuarantine",
    "CausalDatasetVersion",
    "CausalTrendSignal",
    # Student-facing
    "SchoolEvaluation",
    "Tier",
    "Offer",
    "OfferStatus",
    "GoNoGoReport",
    "Recommendation",
    # Usage tracking
    "TokenUsage",
    "ChatSession",
    # Phase-1 admissions pipeline
    "RawSourceSnapshot",
    "RawStructuredRecord",
    "Institution",
    "SourceEntityMap",
    "SchoolMetricsYear",
    # Phase-2 official facts pipeline
    "RawDocument",
    "DocumentChunk",
    "PolicyFact",
    "PolicyFactAudit",
    # Community reviews
    "CommunityReview",
    "SchoolCommunityReport",
    # Claims graph
    "SchoolClaims",
]
