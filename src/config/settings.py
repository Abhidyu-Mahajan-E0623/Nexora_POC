"""Application settings loaded from .env."""

from __future__ import annotations

from functools import lru_cache
from urllib.parse import urlparse

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed application settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Optional legacy Azure OpenAI settings kept only for backward compatibility.
    AZURE_OPENAI_ENDPOINT: str | None = None
    AZURE_OPENAI_API_KEY: str | None = None
    AZURE_OPENAI_API_VERSION: str = "2024-02-15-preview"
    AZURE_OPENAI_CHAT_DEPLOYMENT: str | None = None
    AZURE_OPENAI_EMBED_DEPLOYMENT: str | None = None

    # Databricks
    DATABRICKS_HOST: str
    DATABRICKS_TOKEN: str
    DATABRICKS_SQL_WAREHOUSE_ID: str = Field(min_length=1)
    DATABRICKS_CATALOG: str = "nexora_poc_catalog"
    DATABRICKS_SCHEMA_DOMAIN: str = "bronze"
    DATABRICKS_SCHEMA_MONITORING: str = "monitoring"

    # Behavior
    DEFAULT_TOP_K: int = 10
    OUTPUT_TIMEZONE: str = "UTC"
    ANOMALY_RESCUED_ROW_LIMIT: int = 500
    ENVIRONMENT: str = "Production"

    # Security
    API_KEY: str = "Abhidyumadethiscode-SECURE-2026"

    APP_NAME: str = "schema-maker"
    APP_VERSION: str = "0.2.0"

    @field_validator("AZURE_OPENAI_ENDPOINT", "DATABRICKS_HOST")
    @classmethod
    def validate_https_url(cls, value: str | None) -> str | None:
        """Ensure endpoint-like settings are https URLs when provided."""
        if value is None or not value.strip():
            return value
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("must be a valid https URL")
        return value.rstrip("/")

    @field_validator("DEFAULT_TOP_K", "ANOMALY_RESCUED_ROW_LIMIT")
    @classmethod
    def validate_positive_int(cls, value: int) -> int:
        """Require positive integer settings."""
        if value < 1:
            raise ValueError("must be >= 1")
        return value

    @property
    def databricks_server_hostname(self) -> str:
        """Databricks SQL connector expects hostname without scheme."""
        parsed = urlparse(self.DATABRICKS_HOST)
        return parsed.netloc

    @property
    def databricks_http_path(self) -> str:
        """Build SQL warehouse http path."""
        warehouse = self.DATABRICKS_SQL_WAREHOUSE_ID.strip()
        if warehouse.startswith("/"):
            return warehouse
        return f"/sql/1.0/warehouses/{warehouse}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and cache settings."""
    return Settings()


def load_settings_or_raise() -> Settings:
    """Load settings with a short error message."""
    try:
        return get_settings()
    except ValidationError as exc:  # pragma: no cover - exercised via CLI
        raise RuntimeError(
            "Failed to load settings from .env. Fill the Databricks connection values before running."
        ) from exc
