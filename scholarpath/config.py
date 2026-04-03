from __future__ import annotations

import json
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # Rate limiting
    LLM_RATE_LIMIT_RPM: int = 100  # max requests per minute

    # DeepSearch parallelism defaults (safe under LLM limiter).
    DEEPSEARCH_SCHOOL_CONCURRENCY: int = 8
    DEEPSEARCH_SOURCE_HTTP_CONCURRENCY: int = 16
    DEEPSEARCH_SELF_EXTRACT_CONCURRENCY: int = 12
    DEEPSEARCH_INTERNAL_WEBSEARCH_CONCURRENCY: int = 8

    # Optional web search provider for DeepSearch source.
    WEB_SEARCH_API_URL: str = ""
    WEB_SEARCH_API_KEY: str = ""
    SCORECARD_API_KEY: str = ""
    SCHOOL_PROFILE_SEARCH_API_URL: str = ""
    SCHOOL_PROFILE_SEARCH_API_KEY: str = ""
    IPEDS_DATASET_URL: str = ""
    IPEDS_DATASET_PATH: str = ""
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

    # CORS
    CORS_ORIGINS: str = '["http://localhost:5173"]'

    @property
    def cors_origin_list(self) -> list[str]:
        return json.loads(self.CORS_ORIGINS)

    @property
    def zai_api_keys(self) -> list[str]:
        raw = (self.ZAI_API_KEYS or "").strip()
        keys: list[str] = []

        if raw:
            parsed: list[str] | None = None
            if raw.startswith("["):
                try:
                    obj = json.loads(raw)
                    if isinstance(obj, list):
                        parsed = [str(v).strip() for v in obj]
                except json.JSONDecodeError:
                    parsed = None

            if parsed is None:
                split_vals = [v.strip() for v in raw.replace("\n", ",").split(",")]
                parsed = split_vals

            keys.extend([k for k in parsed if k])

        # Backward compatible fallback.
        if not keys and self.ZAI_API_KEY:
            keys.append(self.ZAI_API_KEY.strip())

        return keys


settings = Settings()
