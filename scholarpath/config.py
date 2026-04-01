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
        "postgresql+asyncpg://scholarpath:scholarpath@localhost:5432/scholarpath"
    )

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # ZAI / LLM
    ZAI_API_KEY: str = ""
    ZAI_BASE_URL: str = "https://api.z.ai/api/paas/v4"
    ZAI_MODEL: str = "glm-5"

    # Rate limiting
    LLM_RATE_LIMIT_RPM: int = 100  # max requests per minute

    # Google Gemini Embedding
    GOOGLE_API_KEY: str = ""
    EMBEDDING_MODEL: str = "gemini-embedding-001"
    EMBEDDING_DIMENSION: int = 3072  # gemini-embedding-001 outputs 3072-dim vectors

    # CORS
    CORS_ORIGINS: str = '["http://localhost:5173"]'

    @property
    def cors_origin_list(self) -> list[str]:
        return json.loads(self.CORS_ORIGINS)


settings = Settings()
