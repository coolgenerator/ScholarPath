from __future__ import annotations

import json
from dataclasses import dataclass

from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class LLMModeConfig:
    name: str
    base_url: str
    model: str
    api_keys: tuple[str, ...]


def _parse_key_list(raw: str) -> list[str]:
    text = (raw or "").strip()
    if not text:
        return []

    parsed: list[str] | None = None
    if text.startswith("["):
        try:
            obj = json.loads(text)
            if isinstance(obj, list):
                parsed = [str(v).strip() for v in obj]
        except json.JSONDecodeError:
            parsed = None

    if parsed is None:
        parsed = [v.strip() for v in text.replace("\n", ",").split(",")]

    return [k for k in parsed if k]


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

    # ZAI / LLM
    ZAI_API_KEY: str = ""
    # Optional multi-key config (JSON array or comma/newline separated string).
    # When set, requests can be balanced across keys.
    ZAI_API_KEYS: str = ""
    ZAI_BASE_URL: str = "https://api.z.ai/api/paas/v4"
    ZAI_MODEL: str = "glm-5"
    # Optional multi-mode runtime config. When configured, active mode takes
    # precedence over legacy ZAI_* settings.
    LLM_MODES_JSON: str = ""
    LLM_ACTIVE_MODE: str = ""

    # College Scorecard API (US Dept of Education)
    COLLEGE_SCORECARD_API_KEY: str = ""

    # Rate limiting
    LLM_RATE_LIMIT_RPM: int = 100  # max requests per minute
    LLM_REQUEST_TIMEOUT_SECONDS: float = 4.5  # per-request timeout to unblock failover within capability budget

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
    COMMON_APP_TREND_URL: str = ""
    COMMON_APP_TREND_PATH: str = ""

    # Causal runtime/training
    CAUSAL_ENGINE_MODE: str = "shadow"  # legacy | pywhy | shadow
    CAUSAL_PYWHY_PRIMARY_PERCENT: int = 100
    CAUSAL_CLEAN_MAX_RPM_TOTAL: int = 180

    # Google Gemini Embedding
    GOOGLE_API_KEY: str = ""
    EMBEDDING_MODEL: str = "gemini-embedding-001"
    EMBEDDING_DIMENSION: int = 3072  # gemini-embedding-001 outputs 3072-dim vectors

    # Auth
    AUTH_SECRET_KEY: str = "change-me-in-production"
    AUTH_TOKEN_EXPIRE_HOURS: int = 24
    AUTH_OTP_TTL_SECONDS: int = 600
    AUTH_OTP_MAX_ATTEMPTS: int = 5

    # CORS
    CORS_ORIGINS: str = '["http://localhost:5173"]'

    @property
    def cors_origin_list(self) -> list[str]:
        return json.loads(self.CORS_ORIGINS)

    @property
    def zai_api_keys(self) -> list[str]:
        keys = _parse_key_list(self.ZAI_API_KEYS)

        # Backward compatible fallback.
        if not keys and self.ZAI_API_KEY:
            keys.append(self.ZAI_API_KEY.strip())

        return keys

    @property
    def llm_modes(self) -> dict[str, LLMModeConfig]:
        raw = (self.LLM_MODES_JSON or "").strip()
        if not raw:
            return {}

        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("LLM_MODES_JSON must be valid JSON") from exc

        if not isinstance(obj, dict):
            raise ValueError("LLM_MODES_JSON must be a JSON object keyed by mode name")

        result: dict[str, LLMModeConfig] = {}
        for mode_name, mode_raw in obj.items():
            normalized_name = str(mode_name).strip()
            if not normalized_name:
                raise ValueError("LLM mode name cannot be empty")
            if not isinstance(mode_raw, dict):
                raise ValueError(f"LLM mode '{normalized_name}' must be a JSON object")

            base_url = str(mode_raw.get("base_url", "")).strip()
            model = str(mode_raw.get("model", "")).strip()
            keys_raw = mode_raw.get("api_keys")

            keys: list[str]
            if isinstance(keys_raw, list):
                keys = [str(v).strip() for v in keys_raw if str(v).strip()]
            else:
                keys = _parse_key_list(str(keys_raw or ""))

            if not base_url:
                raise ValueError(f"LLM mode '{normalized_name}' missing 'base_url'")
            if not model:
                raise ValueError(f"LLM mode '{normalized_name}' missing 'model'")
            if not keys:
                raise ValueError(f"LLM mode '{normalized_name}' must include at least one api key")

            result[normalized_name] = LLMModeConfig(
                name=normalized_name,
                base_url=base_url,
                model=model,
                api_keys=tuple(keys),
            )
        return result

    @property
    def llm_active_mode(self) -> LLMModeConfig | None:
        modes = self.llm_modes
        if not modes:
            return None

        active_mode_name = (self.LLM_ACTIVE_MODE or "").strip()
        if not active_mode_name:
            raise ValueError(
                "LLM_ACTIVE_MODE must be set when LLM_MODES_JSON is configured",
            )

        selected = modes.get(active_mode_name)
        if selected is None:
            available = ", ".join(sorted(modes.keys()))
            raise ValueError(
                f"LLM_ACTIVE_MODE '{active_mode_name}' not found in LLM_MODES_JSON. "
                f"Available: {available}",
            )
        return selected


settings = Settings()

# Fail fast on startup when multi-mode config is present but invalid.
if (settings.LLM_MODES_JSON or "").strip():
    _ = settings.llm_active_mode
