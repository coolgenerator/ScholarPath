from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


_ALLOWED_LLM_METHODS = {
    "complete",
    "complete_json",
    "stream",
}


@dataclass(frozen=True)
class LLMEndpointConfig:
    endpoint_id: str
    base_url: str
    model: str
    api_key_env: str
    rpm: int


@dataclass(frozen=True)
class ResolvedLLMEndpointConfig:
    endpoint_id: str
    base_url: str
    model: str
    api_key_env: str
    api_key: str
    rpm: int


@dataclass(frozen=True)
class LLMModeConfig:
    name: str
    endpoints: tuple[LLMEndpointConfig, ...]


@dataclass(frozen=True)
class LLMGatewayPolicyConfig:
    name: str
    route: dict[str, str]
    call_defaults: dict[str, dict[str, Any]]
    endpoint_overrides: dict[str, dict[str, dict[str, Any]]]
    caller_overrides: dict[str, dict[str, dict[str, Any]]]
    strict_json_callers: tuple[str, ...]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    DATABASE_URL: str = (
        "postgresql+asyncpg://scholarpath:scholarpath@localhost:55432/scholarpath"
    )

    # Redis
    REDIS_URL: str = "redis://localhost:56379/0"

    # LLM gateway (policy-file driven)
    LLM_GATEWAY_POLICIES_PATH: str = "scholarpath/data/llm_gateway_policies.json"
    LLM_ACTIVE_MODE: str = "beecode"
    LLM_ACTIVE_POLICY: str = "default"

    # Global fallback limiter when endpoint rpm is missing/invalid
    LLM_RATE_LIMIT_RPM: int = 100
    # Per-request timeout to unblock failover quickly
    LLM_REQUEST_TIMEOUT_SECONDS: float = 4.5

    # DeepSearch parallelism defaults (safe under LLM limiter).
    DEEPSEARCH_SCHOOL_CONCURRENCY: int = 8
    DEEPSEARCH_SOURCE_HTTP_CONCURRENCY: int = 16
    DEEPSEARCH_SELF_EXTRACT_CONCURRENCY: int = 12
    DEEPSEARCH_INTERNAL_WEBSEARCH_CONCURRENCY: int = 8

    # Optional web search provider for DeepSearch source.
    WEB_SEARCH_API_URL: str = ""
    WEB_SEARCH_API_KEY: str = ""
    SCORECARD_API_KEY: str = ""
    SCORECARD_BULK_URL: str = "https://ed-public-download.scorecard.network/downloads/Most-Recent-Cohorts-Institution_05192025.zip"
    SCORECARD_BULK_PATH: str = ""
    SCHOOL_PROFILE_SEARCH_API_URL: str = ""
    SCHOOL_PROFILE_SEARCH_API_KEY: str = ""
    IPEDS_DATASET_URL: str = ""
    IPEDS_DATASET_PATH: str = ""
    IPEDS_COMPLETIONS_DATASET_URL: str = ""
    IPEDS_COMPLETIONS_DATASET_PATH: str = ""
    IPEDS_INSTITUTION_DATASET_URL: str = ""
    IPEDS_INSTITUTION_DATASET_PATH: str = ""
    COMMON_APP_TREND_URL: str = ""
    COMMON_APP_TREND_PATH: str = ""

    # Causal runtime/training
    CAUSAL_ENGINE_MODE: str = "shadow"  # legacy | pywhy | shadow
    CAUSAL_PYWHY_PRIMARY_PERCENT: int = 100
    CAUSAL_CLEAN_MAX_RPM_TOTAL: int = 180

    # Google Gemini Embedding
    GOOGLE_API_KEY: str = ""
    EMBEDDING_MODEL: str = "gemini-embedding-001"
    EMBEDDING_DIMENSION: int = 3072

    # CORS
    CORS_ORIGINS: str = '["http://localhost:5173"]'

    @property
    def cors_origin_list(self) -> list[str]:
        return json.loads(self.CORS_ORIGINS)

    @property
    def _llm_gateway_path(self) -> Path:
        raw = (self.LLM_GATEWAY_POLICIES_PATH or "").strip()
        if not raw:
            raise ValueError("LLM_GATEWAY_POLICIES_PATH must be set")
        path = Path(raw)
        if not path.is_absolute():
            path = Path.cwd() / path
        return path

    @property
    def llm_gateway_document(self) -> dict[str, Any]:
        path = self._llm_gateway_path
        if not path.exists():
            raise ValueError(f"LLM gateway policies file not found: {path}")
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"LLM gateway policies file is not valid JSON: {path}") from exc

        if not isinstance(parsed, dict):
            raise ValueError("LLM gateway policies root must be a JSON object")
        return parsed

    @property
    def llm_modes(self) -> dict[str, LLMModeConfig]:
        raw_modes = self.llm_gateway_document.get("modes")
        if not isinstance(raw_modes, dict) or not raw_modes:
            raise ValueError("LLM gateway policies must include non-empty 'modes' object")

        result: dict[str, LLMModeConfig] = {}
        for mode_name, mode_raw in raw_modes.items():
            normalized_mode = str(mode_name or "").strip()
            if not normalized_mode:
                raise ValueError("LLM mode name cannot be empty")
            if not isinstance(mode_raw, dict):
                raise ValueError(f"LLM mode '{normalized_mode}' must be a JSON object")

            endpoints_raw = mode_raw.get("endpoints")
            if not isinstance(endpoints_raw, list) or not endpoints_raw:
                raise ValueError(f"LLM mode '{normalized_mode}' must include non-empty endpoints list")

            endpoint_ids: set[str] = set()
            endpoints: list[LLMEndpointConfig] = []
            for row in endpoints_raw:
                if not isinstance(row, dict):
                    raise ValueError(f"LLM mode '{normalized_mode}' endpoint entries must be JSON objects")

                endpoint_id = str(row.get("id", "")).strip()
                base_url = str(row.get("base_url", "")).strip()
                model = str(row.get("model", "")).strip()
                api_key_env = str(row.get("api_key_env", "")).strip()
                rpm_raw = row.get("rpm", self.LLM_RATE_LIMIT_RPM)

                if not endpoint_id:
                    raise ValueError(f"LLM mode '{normalized_mode}' has endpoint with empty id")
                if endpoint_id in endpoint_ids:
                    raise ValueError(f"LLM mode '{normalized_mode}' has duplicate endpoint id '{endpoint_id}'")
                if not base_url:
                    raise ValueError(f"LLM mode '{normalized_mode}' endpoint '{endpoint_id}' missing base_url")
                if not model:
                    raise ValueError(f"LLM mode '{normalized_mode}' endpoint '{endpoint_id}' missing model")
                if not api_key_env:
                    raise ValueError(f"LLM mode '{normalized_mode}' endpoint '{endpoint_id}' missing api_key_env")

                try:
                    rpm = int(rpm_raw)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        f"LLM mode '{normalized_mode}' endpoint '{endpoint_id}' has invalid rpm={rpm_raw!r}",
                    ) from exc
                if rpm <= 0:
                    raise ValueError(
                        f"LLM mode '{normalized_mode}' endpoint '{endpoint_id}' rpm must be > 0",
                    )

                endpoint_ids.add(endpoint_id)
                endpoints.append(
                    LLMEndpointConfig(
                        endpoint_id=endpoint_id,
                        base_url=base_url,
                        model=model,
                        api_key_env=api_key_env,
                        rpm=rpm,
                    ),
                )

            result[normalized_mode] = LLMModeConfig(
                name=normalized_mode,
                endpoints=tuple(endpoints),
            )

        return result

    @property
    def llm_policies(self) -> dict[str, LLMGatewayPolicyConfig]:
        raw_policies = self.llm_gateway_document.get("policies")
        if not isinstance(raw_policies, dict) or not raw_policies:
            raise ValueError("LLM gateway policies must include non-empty 'policies' object")

        result: dict[str, LLMGatewayPolicyConfig] = {}
        for policy_name, policy_raw in raw_policies.items():
            normalized_name = str(policy_name or "").strip()
            if not normalized_name:
                raise ValueError("LLM policy name cannot be empty")
            if not isinstance(policy_raw, dict):
                raise ValueError(f"LLM policy '{normalized_name}' must be a JSON object")

            route = _parse_route_map(policy_raw.get("route"), policy_name=normalized_name)
            call_defaults = _parse_method_map(
                policy_raw.get("call_defaults"),
                section=f"policy '{normalized_name}'.call_defaults",
            )
            endpoint_overrides = _parse_endpoint_method_map(
                policy_raw.get("endpoint_overrides"),
                section=f"policy '{normalized_name}'.endpoint_overrides",
            )
            caller_overrides = _parse_caller_method_map(
                policy_raw.get("caller_overrides"),
                section=f"policy '{normalized_name}'.caller_overrides",
            )
            strict_json_callers = _parse_str_list(
                policy_raw.get("strict_json_callers"),
                section=f"policy '{normalized_name}'.strict_json_callers",
            )

            result[normalized_name] = LLMGatewayPolicyConfig(
                name=normalized_name,
                route=route,
                call_defaults=call_defaults,
                endpoint_overrides=endpoint_overrides,
                caller_overrides=caller_overrides,
                strict_json_callers=strict_json_callers,
            )

        return result

    @property
    def llm_active_mode(self) -> LLMModeConfig:
        mode_name = (self.LLM_ACTIVE_MODE or "").strip()
        if not mode_name:
            raise ValueError("LLM_ACTIVE_MODE must be set")
        mode = self.llm_modes.get(mode_name)
        if mode is None:
            available = ", ".join(sorted(self.llm_modes.keys()))
            raise ValueError(f"LLM_ACTIVE_MODE '{mode_name}' not found. Available: {available}")
        return mode

    @property
    def llm_active_policy(self) -> LLMGatewayPolicyConfig:
        policy_name = (self.LLM_ACTIVE_POLICY or "").strip()
        if not policy_name:
            raise ValueError("LLM_ACTIVE_POLICY must be set")
        policy = self.llm_policies.get(policy_name)
        if policy is None:
            available = ", ".join(sorted(self.llm_policies.keys()))
            raise ValueError(f"LLM_ACTIVE_POLICY '{policy_name}' not found. Available: {available}")

        endpoint_ids = {endpoint.endpoint_id for endpoint in self.llm_active_mode.endpoints}

        # Validate route and overrides against current active mode endpoints.
        for caller, endpoint_id in policy.route.items():
            if endpoint_id not in endpoint_ids:
                raise ValueError(
                    f"Policy '{policy_name}' route caller '{caller}' references unknown endpoint '{endpoint_id}'",
                )

        for endpoint_id in policy.endpoint_overrides.keys():
            if endpoint_id not in endpoint_ids:
                raise ValueError(
                    f"Policy '{policy_name}' endpoint_overrides references unknown endpoint '{endpoint_id}'",
                )

        return policy

    def resolve_active_mode_endpoints(self) -> tuple[ResolvedLLMEndpointConfig, ...]:
        env_file_vals = _read_env_like_file(Path(".env"))
        resolved: list[ResolvedLLMEndpointConfig] = []
        for endpoint in self.llm_active_mode.endpoints:
            api_key = os.getenv(endpoint.api_key_env, "").strip()
            if not api_key:
                api_key = str(env_file_vals.get(endpoint.api_key_env, "")).strip()
            if not api_key:
                raise ValueError(
                    "LLM endpoint key missing. "
                    f"Set env '{endpoint.api_key_env}' for endpoint '{endpoint.endpoint_id}'.",
                )
            resolved.append(
                ResolvedLLMEndpointConfig(
                    endpoint_id=endpoint.endpoint_id,
                    base_url=endpoint.base_url,
                    model=endpoint.model,
                    api_key_env=endpoint.api_key_env,
                    api_key=api_key,
                    rpm=endpoint.rpm,
                ),
            )
        return tuple(resolved)


def _parse_route_map(raw: Any, *, policy_name: str) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"Policy '{policy_name}' route must be an object")

    route: dict[str, str] = {}
    for caller, endpoint_id in raw.items():
        caller_name = str(caller or "").strip()
        endpoint_name = str(endpoint_id or "").strip()
        if not caller_name:
            raise ValueError(f"Policy '{policy_name}' route contains empty caller key")
        if not endpoint_name:
            raise ValueError(f"Policy '{policy_name}' route caller '{caller_name}' has empty endpoint id")
        route[caller_name] = endpoint_name
    return route


def _parse_method_map(raw: Any, *, section: str) -> dict[str, dict[str, Any]]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{section} must be an object")

    out: dict[str, dict[str, Any]] = {}
    for method_name, method_cfg in raw.items():
        method = str(method_name or "").strip()
        if method not in _ALLOWED_LLM_METHODS:
            raise ValueError(f"{section} has unsupported method '{method}'")
        if not isinstance(method_cfg, dict):
            raise ValueError(f"{section}.{method} must be an object")
        out[method] = dict(method_cfg)
    return out


def _read_env_like_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    out: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip("'").strip('"')
    return out


def _parse_endpoint_method_map(raw: Any, *, section: str) -> dict[str, dict[str, dict[str, Any]]]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{section} must be an object")

    out: dict[str, dict[str, dict[str, Any]]] = {}
    for endpoint_id, method_map in raw.items():
        eid = str(endpoint_id or "").strip()
        if not eid:
            raise ValueError(f"{section} has empty endpoint id")
        out[eid] = _parse_method_map(method_map, section=f"{section}.{eid}")
    return out


def _parse_caller_method_map(raw: Any, *, section: str) -> dict[str, dict[str, dict[str, Any]]]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f"{section} must be an object")

    out: dict[str, dict[str, dict[str, Any]]] = {}
    for caller, method_map in raw.items():
        cname = str(caller or "").strip()
        if not cname:
            raise ValueError(f"{section} has empty caller")
        out[cname] = _parse_method_map(method_map, section=f"{section}.{cname}")
    return out


def _parse_str_list(raw: Any, *, section: str) -> tuple[str, ...]:
    if raw is None:
        return tuple()
    if not isinstance(raw, list):
        raise ValueError(f"{section} must be an array")
    values: list[str] = []
    for item in raw:
        text = str(item or "").strip()
        if not text:
            raise ValueError(f"{section} contains an empty value")
        values.append(text)
    return tuple(values)


settings = Settings()

# Fail fast for config/policy topology issues.
_ = settings.llm_active_mode
_ = settings.llm_active_policy
