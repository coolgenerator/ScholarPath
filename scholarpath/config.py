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
    # Advisor internal DeepSearch refresh (on school query when critical fields are missing).
    ADVISOR_INTERNAL_DEEPSEARCH_ENABLED: bool = True
    ADVISOR_INTERNAL_DEEPSEARCH_FRESHNESS_DAYS: int = 90
    ADVISOR_INTERNAL_DEEPSEARCH_MAX_INTERNAL_WEBSEARCH_PER_SCHOOL: int = 1
    ADVISOR_INTERNAL_DEEPSEARCH_BUDGET_MODE: str = "balanced"
    ADVISOR_STYLE_POLISH_ENABLED: bool = True
    ADVISOR_STYLE_POLISH_CAPABILITIES: str = (
        "undergrad.school.recommend,offer.compare,offer.what_if"
    )
    ADVISOR_STYLE_POLISH_MAX_TOKENS: int = 600
    ADVISOR_STYLE_POLISH_TEMPERATURE: float = 0.2

    # Optional web search provider for DeepSearch source.
    WEB_SEARCH_API_URL: str = ""
    WEB_SEARCH_API_KEY: str = ""
    SCORECARD_API_KEY: str = ""

    # Google Gemini Embedding
    GOOGLE_API_KEY: str = ""
    EMBEDDING_MODEL: str = "gemini-embedding-001"
    EMBEDDING_DIMENSION: int = 3072  # gemini-embedding-001 outputs 3072-dim vectors

    # Causal engine rollout
    CAUSAL_ENGINE_MODE: str = "shadow"  # legacy | pywhy | shadow
    # In shadow mode, route this percentage of requests to PyWhy as primary.
    # 0 keeps legacy as primary for all requests.
    CAUSAL_PYWHY_PRIMARY_PERCENT: int = 0
    CAUSAL_MODEL_VERSION: str = "latest_stable"
    CAUSAL_PROXY_LABELS_ENABLED: bool = True
    CAUSAL_SHADOW_LOGGING: bool = True

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

    @property
    def advisor_style_polish_capabilities(self) -> set[str]:
        raw = (self.ADVISOR_STYLE_POLISH_CAPABILITIES or "").strip()
        if not raw:
            return set()

        values: list[str] = []
        if raw.startswith("["):
            try:
                obj = json.loads(raw)
                if isinstance(obj, list):
                    values = [str(item).strip() for item in obj]
            except json.JSONDecodeError:
                values = []

        if not values:
            values = [item.strip() for item in raw.replace("\n", ",").split(",")]

        return {item for item in values if item}


settings = Settings()
