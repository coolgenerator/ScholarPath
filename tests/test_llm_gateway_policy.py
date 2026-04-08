from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scholarpath.config import Settings
from scholarpath.llm.client import LLMClient


def _write_policy(path: Path) -> None:
    payload = {
        "modes": {
            "test": {
                "endpoints": [
                    {
                        "id": "e1",
                        "base_url": "https://unit-1.example/v1",
                        "model": "m1",
                        "api_key_env": "UNIT_KEY_1",
                        "rpm": 60,
                    },
                    {
                        "id": "e2",
                        "base_url": "https://unit-2.example/v1",
                        "model": "m2",
                        "api_key_env": "UNIT_KEY_2",
                        "rpm": 60,
                    },
                ],
            },
        },
        "policies": {
            "test-policy": {
                "route": {
                    "chat.extract_school": "e2",
                },
                "call_defaults": {
                    "complete_json": {
                        "response_format": {
                            "type": "json_schema",
                            "json_schema": {"name": "default_name", "strict": False},
                        },
                        "stream_json_enabled": False,
                        "schema_hint_enabled": True,
                        "parse_mode": "strict",
                        "json_transport_hints_enabled": True,
                    },
                },
                "endpoint_overrides": {
                    "e2": {
                        "complete_json": {
                            "response_format": {"type": "json_object"},
                        },
                    },
                },
                "caller_overrides": {
                    "chat.extract_school": {
                        "complete_json": {
                            "response_format": {"type": "none"},
                        },
                    },
                },
                "strict_json_callers": ["chat.strict_json"],
            },
        },
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_settings_resolve_active_mode_endpoints(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    policy_path = tmp_path / "llm_gateway_policies.json"
    _write_policy(policy_path)

    monkeypatch.setenv("UNIT_KEY_1", "k1")
    monkeypatch.setenv("UNIT_KEY_2", "k2")

    cfg = Settings(
        LLM_GATEWAY_POLICIES_PATH=str(policy_path),
        LLM_ACTIVE_MODE="test",
        LLM_ACTIVE_POLICY="test-policy",
    )

    endpoints = cfg.resolve_active_mode_endpoints()
    assert len(endpoints) == 2
    assert endpoints[0].endpoint_id == "e1"
    assert endpoints[0].api_key == "k1"
    assert cfg.llm_active_policy.route["chat.extract_school"] == "e2"


def test_settings_missing_endpoint_key_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    policy_path = tmp_path / "llm_gateway_policies.json"
    _write_policy(policy_path)

    monkeypatch.delenv("UNIT_KEY_1", raising=False)
    monkeypatch.setenv("UNIT_KEY_2", "k2")

    cfg = Settings(
        LLM_GATEWAY_POLICIES_PATH=str(policy_path),
        LLM_ACTIVE_MODE="test",
        LLM_ACTIVE_POLICY="test-policy",
    )

    with pytest.raises(ValueError, match="UNIT_KEY_1"):
        cfg.resolve_active_mode_endpoints()


@pytest.mark.asyncio
async def test_llm_client_applies_route_and_policy_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy_path = tmp_path / "llm_gateway_policies.json"
    _write_policy(policy_path)

    monkeypatch.setenv("UNIT_KEY_1", "k1")
    monkeypatch.setenv("UNIT_KEY_2", "k2")

    cfg = Settings(
        LLM_GATEWAY_POLICIES_PATH=str(policy_path),
        LLM_ACTIVE_MODE="test",
        LLM_ACTIVE_POLICY="test-policy",
    )

    created_clients: list[SimpleNamespace] = []

    class _FakeAsyncOpenAI:
        def __init__(self, **kwargs):
            self.calls: list[dict] = []
            self.base_url = kwargs.get("base_url")
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._chat_create))
            created_clients.append(self)

        async def _chat_create(self, **payload):
            self.calls.append(payload)
            return SimpleNamespace(
                id="resp_1",
                usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2),
                choices=[SimpleNamespace(message=SimpleNamespace(content='{"ok": true}'))],
            )

    monkeypatch.setattr("scholarpath.llm.client.openai.AsyncOpenAI", _FakeAsyncOpenAI)

    llm = LLMClient(
        mode_name=cfg.llm_active_mode.name,
        policy_name=cfg.llm_active_policy.name,
        policy=cfg.llm_active_policy,
        endpoints=list(cfg.resolve_active_mode_endpoints()),
    )

    result = await llm.complete_json(
        messages=[{"role": "user", "content": "json"}],
        schema={
            "type": "object",
            "properties": {"ok": {"type": "boolean"}},
            "required": ["ok"],
            "additionalProperties": False,
        },
        caller="chat.extract_school",
    )

    assert result == {"ok": True}

    # route forces e2 first (second endpoint in mode list)
    assert len(created_clients) == 2
    assert len(created_clients[0].calls) == 0
    assert len(created_clients[1].calls) == 1

    # caller override wins over endpoint/default => response_format removed
    payload = created_clients[1].calls[0]
    assert "response_format" not in payload

    stats = llm.endpoint_stats()
    by_endpoint = {row["endpoint_id"]: row for row in stats}
    assert by_endpoint["e2"]["preferred_route_hits"] >= 1
    assert by_endpoint["e2"]["policy_applied_counts_by_method"]["complete_json"] >= 1


@pytest.mark.asyncio
async def test_llm_client_rejects_unknown_caller(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy_path = tmp_path / "llm_gateway_policies.json"
    _write_policy(policy_path)

    monkeypatch.setenv("UNIT_KEY_1", "k1")
    monkeypatch.setenv("UNIT_KEY_2", "k2")

    cfg = Settings(
        LLM_GATEWAY_POLICIES_PATH=str(policy_path),
        LLM_ACTIVE_MODE="test",
        LLM_ACTIVE_POLICY="test-policy",
    )

    class _FakeAsyncOpenAI:
        def __init__(self, **_kwargs):
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._chat_create))

        async def _chat_create(self, **_payload):
            return SimpleNamespace(
                id="resp_1",
                usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2),
                choices=[SimpleNamespace(message=SimpleNamespace(content='{"ok": true}'))],
            )

    monkeypatch.setattr("scholarpath.llm.client.openai.AsyncOpenAI", _FakeAsyncOpenAI)

    llm = LLMClient(
        mode_name=cfg.llm_active_mode.name,
        policy_name=cfg.llm_active_policy.name,
        policy=cfg.llm_active_policy,
        endpoints=list(cfg.resolve_active_mode_endpoints()),
    )

    with pytest.raises(ValueError, match="caller must be explicitly named"):
        await llm.complete_json(
            messages=[{"role": "user", "content": "json"}],
            schema={"type": "object", "properties": {"ok": {"type": "boolean"}}},
        )


@pytest.mark.asyncio
async def test_strict_json_caller_fails_on_non_json_and_tracks_counters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy_path = tmp_path / "llm_gateway_policies.json"
    _write_policy(policy_path)

    monkeypatch.setenv("UNIT_KEY_1", "k1")
    monkeypatch.setenv("UNIT_KEY_2", "k2")

    cfg = Settings(
        LLM_GATEWAY_POLICIES_PATH=str(policy_path),
        LLM_ACTIVE_MODE="test",
        LLM_ACTIVE_POLICY="test-policy",
    )

    class _FakeAsyncOpenAI:
        def __init__(self, **kwargs):
            self.base_url = kwargs.get("base_url")
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._chat_create))

        async def _chat_create(self, **_payload):
            # strict caller should reject this non-json content.
            return SimpleNamespace(
                id="resp_1",
                usage=SimpleNamespace(prompt_tokens=1, completion_tokens=1, total_tokens=2),
                choices=[SimpleNamespace(message=SimpleNamespace(content="not-json"))],
            )

    monkeypatch.setattr("scholarpath.llm.client.openai.AsyncOpenAI", _FakeAsyncOpenAI)

    llm = LLMClient(
        mode_name=cfg.llm_active_mode.name,
        policy_name=cfg.llm_active_policy.name,
        policy=cfg.llm_active_policy,
        endpoints=list(cfg.resolve_active_mode_endpoints()),
    )

    with pytest.raises(ValueError, match="Strict JSON caller"):
        await llm.complete_json(
            messages=[{"role": "user", "content": "json"}],
            schema={
                "type": "object",
                "properties": {"ok": {"type": "boolean"}},
                "required": ["ok"],
                "additionalProperties": False,
            },
            caller="chat.strict_json",
        )

    stats = llm.endpoint_stats()
    assert sum(int(item.get("parse_fail", 0)) for item in stats) >= 1
    assert sum(int(item.get("non_json", 0)) for item in stats) >= 1
