from __future__ import annotations

from types import SimpleNamespace

import httpx
import openai
import pytest

from scholarpath.llm.client import LLMClient


def _fake_response(content: str) -> SimpleNamespace:
    return SimpleNamespace(
        id="resp_test",
        usage=None,
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
    )


def _rate_limit_error(message: str) -> openai.RateLimitError:
    request = httpx.Request("POST", "https://example.com/v1/chat/completions")
    response = httpx.Response(status_code=429, request=request)
    return openai.RateLimitError(
        message,
        response=response,
        body={"error": {"message": message}},
    )


def _api_timeout_error() -> openai.APITimeoutError:
    request = httpx.Request("POST", "https://example.com/v1/chat/completions")
    return openai.APITimeoutError(request=request)


class _NoopLimiter:
    async def acquire(self) -> None:
        return None


def _fake_stream_response(chunks: list[str]):
    async def _gen():
        for chunk in chunks:
            yield SimpleNamespace(
                choices=[SimpleNamespace(delta=SimpleNamespace(content=chunk))],
            )

    return _gen()


@pytest.mark.asyncio
async def test_complete_json_empty_content_returns_empty_dict(monkeypatch):
    client = LLMClient(
        api_key="test-key",
        api_keys=["test-key"],
        base_url="https://example.com/v1",
        model="test-model",
        max_rpm=10,
    )
    captured: dict[str, object] = {}

    async def _fake_chat_completion_with_failover(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(index=0), _fake_response("")

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr(client, "_chat_completion_with_failover", _fake_chat_completion_with_failover)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json([{"role": "user", "content": "x"}], caller="test.empty")

    assert result == {}
    response_format = captured.get("response_format")
    assert isinstance(response_format, dict)
    assert response_format.get("type") == "json_schema"
    json_schema = response_format.get("json_schema")
    assert isinstance(json_schema, dict)
    assert json_schema.get("schema") == {
        "type": "object",
        "properties": {
            "_": {
                "type": "string",
                "description": "Optional placeholder. Real output can use any keys.",
            },
        },
        "additionalProperties": True,
    }
    assert captured.get("json_transport_hints") is True


@pytest.mark.asyncio
async def test_complete_json_beecode_disables_json_transport_hints(monkeypatch):
    client = LLMClient(
        api_key="test-key",
        api_keys=["test-key"],
        base_url="https://beecode.cc/v1",
        model="test-model",
        max_rpm=10,
    )
    captured: dict[str, object] = {}

    async def _fake_chat_completion_with_failover(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(index=0), _fake_stream_response(['{"ok": true}'])

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr(client, "_chat_completion_with_failover", _fake_chat_completion_with_failover)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json([{"role": "user", "content": "x"}], caller="test.beecode")

    assert result == {"ok": True}
    assert captured.get("stream") is True
    response_format = captured.get("response_format")
    assert isinstance(response_format, dict)
    assert response_format.get("type") == "json_schema"
    json_schema = response_format.get("json_schema")
    assert isinstance(json_schema, dict)
    assert json_schema.get("schema") == {
        "type": "object",
        "properties": {
            "_": {
                "type": "string",
                "description": "Optional placeholder. Real output can use any keys.",
            },
        },
        "additionalProperties": True,
    }
    assert "json_transport_hints" not in captured


@pytest.mark.asyncio
async def test_complete_json_uses_callsite_schema_in_response_format(monkeypatch):
    client = LLMClient(
        api_key="test-key",
        api_keys=["test-key"],
        base_url="https://example.com/v1",
        model="test-model",
        max_rpm=10,
    )
    captured: dict[str, object] = {}
    expected_schema = {
        "type": "object",
        "properties": {
            "ok": {"type": "boolean"},
        },
        "required": ["ok"],
        "additionalProperties": False,
    }

    async def _fake_chat_completion_with_failover(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(index=0), _fake_response('{"ok": true}')

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr(client, "_chat_completion_with_failover", _fake_chat_completion_with_failover)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json(
        [{"role": "user", "content": "x"}],
        schema=expected_schema,
        caller="test.schema",
    )

    assert result == {"ok": True}
    response_format = captured.get("response_format")
    assert isinstance(response_format, dict)
    assert response_format.get("type") == "json_schema"
    json_schema = response_format.get("json_schema")
    assert isinstance(json_schema, dict)
    assert json_schema.get("schema") == expected_schema


@pytest.mark.asyncio
async def test_complete_json_invalid_callsite_schema_falls_back_to_default(monkeypatch):
    client = LLMClient(
        api_key="test-key",
        api_keys=["test-key"],
        base_url="https://example.com/v1",
        model="test-model",
        max_rpm=10,
    )
    captured: dict[str, object] = {}
    invalid_schema = {
        "type": None,
        "properties": {"ok": {"type": "boolean"}},
    }

    async def _fake_chat_completion_with_failover(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(index=0), _fake_response('{"ok": true}')

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr(client, "_chat_completion_with_failover", _fake_chat_completion_with_failover)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json(
        [{"role": "user", "content": "x"}],
        schema=invalid_schema,
        caller="test.invalid-schema",
    )

    assert result == {"ok": True}
    response_format = captured.get("response_format")
    assert isinstance(response_format, dict)
    json_schema = response_format.get("json_schema")
    assert isinstance(json_schema, dict)
    assert json_schema.get("schema") == {
        "type": "object",
        "properties": {
            "_": {
                "type": "string",
                "description": "Optional placeholder. Real output can use any keys.",
            },
        },
        "additionalProperties": True,
    }


@pytest.mark.asyncio
async def test_complete_json_xcode_stream_mode_parses_json(monkeypatch):
    client = LLMClient(
        api_key="test-key",
        api_keys=["test-key"],
        base_url="https://api.xcode.best/v1",
        model="test-model",
        max_rpm=10,
    )
    captured: dict[str, object] = {}

    async def _fake_chat_completion_with_failover(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(index=0), _fake_stream_response(
            ['{"ok": true, ', '"score": 97}'],
        )

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr(client, "_chat_completion_with_failover", _fake_chat_completion_with_failover)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json([{"role": "user", "content": "x"}], caller="test.xcode.stream")

    assert result == {"ok": True, "score": 97}
    assert captured.get("stream") is True
    response_format = captured.get("response_format")
    assert isinstance(response_format, dict)
    assert response_format.get("type") == "json_schema"


@pytest.mark.asyncio
async def test_complete_json_parses_fenced_json(monkeypatch):
    client = LLMClient(
        api_key="test-key",
        api_keys=["test-key"],
        base_url="https://example.com/v1",
        model="test-model",
        max_rpm=10,
    )
    raw = """```json
{"overall_score": 88, "field_pass_rate": 0.74}
```"""

    async def _fake_chat_completion_with_failover(**_kwargs):
        return SimpleNamespace(index=0), _fake_response(raw)

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr(client, "_chat_completion_with_failover", _fake_chat_completion_with_failover)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json([{"role": "user", "content": "x"}], caller="test.fenced")

    assert result == {"overall_score": 88, "field_pass_rate": 0.74}


@pytest.mark.asyncio
async def test_complete_json_wraps_list_payload(monkeypatch):
    client = LLMClient(
        api_key="test-key",
        api_keys=["test-key"],
        base_url="https://example.com/v1",
        model="test-model",
        max_rpm=10,
    )

    async def _fake_chat_completion_with_failover(**_kwargs):
        return SimpleNamespace(index=0), _fake_response("[1, 2, 3]")

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr(client, "_chat_completion_with_failover", _fake_chat_completion_with_failover)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json([{"role": "user", "content": "x"}], caller="test.list")

    assert result == {"data": [1, 2, 3]}


@pytest.mark.asyncio
async def test_complete_json_rate_limit_same_task_retry_success(monkeypatch):
    client = LLMClient(
        api_key="test-key-1",
        api_keys=["test-key-1", "test-key-2"],
        base_url="https://beecode.cc/v1",
        model="test-model",
        max_rpm=10,
    )
    ep0, ep1 = client._endpoints
    ep0.rate_limiter = _NoopLimiter()
    ep1.rate_limiter = _NoopLimiter()

    call_counts = {"ep0": 0, "ep1": 0}

    async def ep0_chat_create(**_kwargs):
        call_counts["ep0"] += 1
        if call_counts["ep0"] == 1:
            raise _rate_limit_error("Too many pending requests, please retry later")
        return _fake_stream_response(['{"ok": true, "path": "same-task-retry"}'])

    async def ep1_chat_create(**_kwargs):
        call_counts["ep1"] += 1
        return _fake_stream_response(['{"ok": true, "path": "failover"}'])

    ep0.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=ep0_chat_create)),
        responses=SimpleNamespace(create=None),
    )
    ep1.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=ep1_chat_create)),
        responses=SimpleNamespace(create=None),
    )

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        return None

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr("scholarpath.llm.client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json([{"role": "user", "content": "x"}], caller="test.same-retry.ok")

    assert result == {"ok": True, "path": "same-task-retry"}
    assert call_counts == {"ep0": 2, "ep1": 0}
    assert sleep_calls == [5.0]
    assert ep0.same_task_retry_triggered == 1
    assert ep0.same_task_retry_success == 1
    assert ep0.same_task_retry_failed == 0

    health = await client.endpoint_health(window_seconds=60)
    assert health["endpoints"][0]["same_task_retry_triggered"] == 1
    assert health["endpoints"][0]["same_task_retry_success"] == 1
    assert health["endpoints"][0]["same_task_retry_failed"] == 0


@pytest.mark.asyncio
async def test_complete_json_timeout_same_task_retry_success(monkeypatch):
    client = LLMClient(
        api_key="test-key-1",
        api_keys=["test-key-1", "test-key-2"],
        base_url="https://beecode.cc/v1",
        model="test-model",
        max_rpm=10,
    )
    ep0, ep1 = client._endpoints
    ep0.rate_limiter = _NoopLimiter()
    ep1.rate_limiter = _NoopLimiter()

    call_counts = {"ep0": 0, "ep1": 0}
    seen_timeout_values: list[float] = []

    async def ep0_chat_create(**kwargs):
        call_counts["ep0"] += 1
        seen_timeout_values.append(float(kwargs.get("timeout", 0.0)))
        if call_counts["ep0"] == 1:
            raise _api_timeout_error()
        return _fake_stream_response(['{"ok": true, "path": "timeout-retry"}'])

    async def ep1_chat_create(**_kwargs):
        call_counts["ep1"] += 1
        return _fake_stream_response(['{"ok": true, "path": "failover"}'])

    ep0.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=ep0_chat_create)),
        responses=SimpleNamespace(create=None),
    )
    ep1.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=ep1_chat_create)),
        responses=SimpleNamespace(create=None),
    )

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        return None

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr("scholarpath.llm.client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json(
        [{"role": "user", "content": "x"}],
        caller="test.timeout-retry.ok",
    )

    assert result == {"ok": True, "path": "timeout-retry"}
    assert call_counts == {"ep0": 2, "ep1": 0}
    assert sleep_calls == [0.4]
    assert ep0.same_task_retry_triggered == 1
    assert ep0.same_task_retry_success == 1
    assert ep0.same_task_retry_failed == 0
    assert seen_timeout_values
    assert all(value == pytest.approx(client._request_timeout_seconds) for value in seen_timeout_values)


@pytest.mark.asyncio
async def test_complete_json_rate_limit_same_task_retry_fail_then_failover(monkeypatch):
    client = LLMClient(
        api_key="test-key-1",
        api_keys=["test-key-1", "test-key-2"],
        base_url="https://beecode.cc/v1",
        model="test-model",
        max_rpm=10,
    )
    ep0, ep1 = client._endpoints
    ep0.rate_limiter = _NoopLimiter()
    ep1.rate_limiter = _NoopLimiter()

    call_counts = {"ep0": 0, "ep1": 0}

    async def ep0_chat_create(**_kwargs):
        call_counts["ep0"] += 1
        raise _rate_limit_error("Too many pending requests, please retry later")

    async def ep1_chat_create(**_kwargs):
        call_counts["ep1"] += 1
        return _fake_stream_response(['{"ok": true, "path": "failover"}'])

    ep0.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=ep0_chat_create)),
        responses=SimpleNamespace(create=None),
    )
    ep1.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=ep1_chat_create)),
        responses=SimpleNamespace(create=None),
    )

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        return None

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr("scholarpath.llm.client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json([{"role": "user", "content": "x"}], caller="test.same-retry.failover")

    assert result == {"ok": True, "path": "failover"}
    assert call_counts == {"ep0": 2, "ep1": 1}
    assert sleep_calls == [5.0]
    assert ep0.same_task_retry_triggered == 1
    assert ep0.same_task_retry_success == 0
    assert ep0.same_task_retry_failed == 1


@pytest.mark.asyncio
async def test_complete_json_non_provider_limit_rate_error_skips_same_task_retry(monkeypatch):
    client = LLMClient(
        api_key="test-key-1",
        api_keys=["test-key-1", "test-key-2"],
        base_url="https://beecode.cc/v1",
        model="test-model",
        max_rpm=10,
    )
    ep0, ep1 = client._endpoints
    ep0.rate_limiter = _NoopLimiter()
    ep1.rate_limiter = _NoopLimiter()

    call_counts = {"ep0": 0, "ep1": 0}

    async def ep0_chat_create(**_kwargs):
        call_counts["ep0"] += 1
        raise _rate_limit_error("Rate limit exceeded")

    async def ep1_chat_create(**_kwargs):
        call_counts["ep1"] += 1
        return _fake_stream_response(['{"ok": true, "path": "failover"}'])

    ep0.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=ep0_chat_create)),
        responses=SimpleNamespace(create=None),
    )
    ep1.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=ep1_chat_create)),
        responses=SimpleNamespace(create=None),
    )

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        return None

    async def _fake_track(**_kwargs):
        return None

    monkeypatch.setattr("scholarpath.llm.client.asyncio.sleep", fake_sleep)
    monkeypatch.setattr(client, "_track", _fake_track)

    result = await client.complete_json([{"role": "user", "content": "x"}], caller="test.non-provider-limit")

    assert result == {"ok": True, "path": "failover"}
    assert call_counts == {"ep0": 1, "ep1": 1}
    assert sleep_calls == []
    assert ep0.same_task_retry_triggered == 0
    assert ep0.same_task_retry_success == 0
    assert ep0.same_task_retry_failed == 0


@pytest.mark.asyncio
async def test_responses_rate_limit_same_task_retry_success(monkeypatch):
    client = LLMClient(
        api_key="test-key-1",
        api_keys=["test-key-1", "test-key-2"],
        base_url="https://beecode.cc/v1",
        model="test-model",
        max_rpm=10,
    )
    ep0, ep1 = client._endpoints
    ep0.rate_limiter = _NoopLimiter()
    ep1.rate_limiter = _NoopLimiter()

    call_counts = {"ep0": 0, "ep1": 0}

    async def ep0_resp_create(**_kwargs):
        call_counts["ep0"] += 1
        if call_counts["ep0"] == 1:
            raise _rate_limit_error("Too many pending requests, please retry later")
        return SimpleNamespace(id="resp_ok", usage=None, output_text="OK", output=[])

    async def ep1_resp_create(**_kwargs):
        call_counts["ep1"] += 1
        return SimpleNamespace(id="resp_failover", usage=None, output_text="OK", output=[])

    ep0.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=None)),
        responses=SimpleNamespace(create=ep0_resp_create),
    )
    ep1.client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=None)),
        responses=SimpleNamespace(create=ep1_resp_create),
    )

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        return None

    monkeypatch.setattr("scholarpath.llm.client.asyncio.sleep", fake_sleep)

    endpoint, response = await client._responses_with_failover(
        input_text="Reply with OK only",
        tools=None,
        temperature=0.0,
        max_output_tokens=32,
    )

    assert endpoint.index == 0
    assert response.output_text == "OK"
    assert call_counts == {"ep0": 2, "ep1": 0}
    assert sleep_calls == [5.0]
    assert ep0.same_task_retry_triggered == 1
    assert ep0.same_task_retry_success == 1
    assert ep0.same_task_retry_failed == 0
