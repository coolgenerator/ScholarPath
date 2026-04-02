"""Route-level advisor error response contract tests."""

from __future__ import annotations

from scholarpath.advisor.contracts import AdvisorResponse
from scholarpath.api.routes.advisor import _error_response


def test_invalid_input_error_response_contract() -> None:
    payload = _error_response(
        turn_id="turn-invalid",
        session_id="session-invalid",
        message="Invalid request payload: student_id must be UUID",
        code="INVALID_INPUT",
        capability="common.general",
        guard_result="invalid_input",
        guard_reason="invalid_input",
        retriable=False,
    )
    response = AdvisorResponse.model_validate(payload)

    assert response.domain == "common"
    assert response.capability == "common.general"
    assert response.error is not None
    assert response.error.code == "INVALID_INPUT"
    assert response.route_meta.guard_result == "invalid_input"
    assert response.route_meta.guard_reason == "invalid_input"
    assert response.done == []
    assert response.pending == []
    assert len(response.next_actions) >= 1
    assert any(action.action_id == "route.clarify" for action in response.next_actions)
