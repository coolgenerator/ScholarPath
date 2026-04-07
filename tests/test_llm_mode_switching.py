from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from scholarpath.config import LLMModeConfig, Settings
import scholarpath.llm.client as llm_client_module


@pytest.fixture(autouse=True)
def _reset_llm_singleton(monkeypatch):
    monkeypatch.setattr(llm_client_module, "_singleton", None)
    yield
    monkeypatch.setattr(llm_client_module, "_singleton", None)


def test_settings_parse_llm_modes_and_active_mode():
    payload = json.dumps(
        {
            "beecode": {
                "base_url": "https://beecode.cc/v1",
                "model": "gpt-5.4-mini",
                "api_keys": ["k1", "k2"],
            },
            "zai": {
                "base_url": "https://api.xcode.best/v1",
                "model": "gpt-5.4-mini",
                "api_keys": ["z1"],
            },
        }
    )

    cfg = Settings(
        _env_file=None,
        LLM_MODES_JSON=payload,
        LLM_ACTIVE_MODE="beecode",
    )

    assert sorted(cfg.llm_modes.keys()) == ["beecode", "zai"]
    assert cfg.llm_active_mode is not None
    assert cfg.llm_active_mode.name == "beecode"
    assert cfg.llm_active_mode.base_url == "https://beecode.cc/v1"
    assert cfg.llm_active_mode.model == "gpt-5.4-mini"
    assert list(cfg.llm_active_mode.api_keys) == ["k1", "k2"]


def test_settings_invalid_active_mode_raises_clear_error():
    cfg = Settings(
        _env_file=None,
        LLM_MODES_JSON='{"beecode":{"base_url":"https://beecode.cc/v1","model":"gpt-5.4-mini","api_keys":["k1"]}}',
        LLM_ACTIVE_MODE="missing-mode",
    )

    with pytest.raises(ValueError, match="LLM_ACTIVE_MODE 'missing-mode' not found"):
        _ = cfg.llm_active_mode


def test_settings_empty_api_keys_raise_error():
    cfg = Settings(
        _env_file=None,
        LLM_MODES_JSON='{"beecode":{"base_url":"https://beecode.cc/v1","model":"gpt-5.4-mini","api_keys":[]}}',
        LLM_ACTIVE_MODE="beecode",
    )

    with pytest.raises(ValueError, match="at least one api key"):
        _ = cfg.llm_modes


def test_get_llm_client_prefers_active_mode(monkeypatch):
    k1 = "beecode-k1"
    k2 = "beecode-k2"
    fake_settings = SimpleNamespace(
        llm_active_mode=LLMModeConfig(
            name="beecode",
            base_url="https://beecode.cc/v1",
            model="gpt-5.4-mini",
            api_keys=(k1, k2),
        ),
        zai_api_keys=["legacy-k1"],
        ZAI_API_KEY="legacy-k1",
        ZAI_BASE_URL="https://legacy.example/v1",
        ZAI_MODEL="legacy-model",
        LLM_RATE_LIMIT_RPM=100,
        REDIS_URL="",
    )
    monkeypatch.setattr(llm_client_module, "settings", fake_settings)

    client = llm_client_module.get_llm_client()

    assert len(client._endpoints) == 2
    assert client._model == "gpt-5.4-mini"
    assert [ep.key_id for ep in client._endpoints] == [
        llm_client_module._api_key_fingerprint(k1),
        llm_client_module._api_key_fingerprint(k2),
    ]
    assert all(ep.rate_limiter._local._max_rpm == 100 for ep in client._endpoints)


def test_get_llm_client_falls_back_to_legacy_zai(monkeypatch):
    legacy_keys = ["legacy-k1", "legacy-k2"]
    fake_settings = SimpleNamespace(
        llm_active_mode=None,
        zai_api_keys=legacy_keys,
        ZAI_API_KEY="legacy-primary",
        ZAI_BASE_URL="https://legacy.example/v1",
        ZAI_MODEL="legacy-model",
        LLM_RATE_LIMIT_RPM=100,
        REDIS_URL="",
    )
    monkeypatch.setattr(llm_client_module, "settings", fake_settings)

    client = llm_client_module.get_llm_client()

    assert len(client._endpoints) == 2
    assert client._model == "legacy-model"
    assert [ep.key_id for ep in client._endpoints] == [
        llm_client_module._api_key_fingerprint("legacy-k1"),
        llm_client_module._api_key_fingerprint("legacy-k2"),
    ]
