"""Protocol definitions for the standalone causal inference V2 module."""

from __future__ import annotations

from typing import Protocol

from scholarpath.causal_v2.contracts import (
    CausalEvaluationResult,
    CausalScenarioComparisonResult,
    CausalScenarioInput,
    CausalSchoolProfile,
    CausalStudentProfile,
    CausalWhatIfResult,
)


class CausalEngineProtocol(Protocol):
    """Engine protocol designed to match future service integration seams."""

    def evaluate(
        self,
        student_profile: CausalStudentProfile,
        school_profile: CausalSchoolProfile,
        *,
        include_confidence: bool = True,
    ) -> CausalEvaluationResult:
        """Return dimension + outcome scores for one student-school pair."""

    def what_if(
        self,
        student_profile: CausalStudentProfile,
        school_profile: CausalSchoolProfile,
        interventions: dict[str, float],
    ) -> CausalWhatIfResult:
        """Apply interventions and return baseline vs modified outcomes."""

    def compare_scenarios(
        self,
        student_profile: CausalStudentProfile,
        scenarios: list[CausalScenarioInput],
    ) -> CausalScenarioComparisonResult:
        """Compare two or more explicit scenarios and return ranked outputs."""

