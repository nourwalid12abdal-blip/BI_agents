# config/settings.py

from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import Optional
from pathlib import Path


class Settings(BaseSettings):
    """
    Central config for the BI Agent.
    All values are loaded from .env automatically.
    Import the singleton `settings` everywhere — never instantiate Settings directly.
    """

    # ── LLM ──────────────────────────────────────────────────────────────────
    openai_api_key: str = Field(..., description="OpenAI API key")
    HF_TOKEN: str = Field(..., description="hugging face API key")

    groq_api_key: str = Field(..., description="Groq API key")
    groq_model: str = Field("llama-3.3-70b-versatile", description="Groq model name")

    openai_model: str = Field("gpt-4o", description="Model used for all LLM calls")
    HF_MODEL: str = Field(
        "meta-llama/Llama-3.1-8B-Instruct",
        description="judge for cross_relation databases",
    )

    # ── SQL database ─────────────────────────────────────────────────────────
    sql_db_url: str = Field(
        "sqlite:///./bi_agent.db",
        description="SQLAlchemy connection URL. Supports SQLite, Postgres, MySQL, MSSQL.",
    )

    # ── MongoDB ───────────────────────────────────────────────────────────────
    mongo_uri: Optional[str] = Field(
        None,
        description="MongoDB connection URI. Leave empty to disable MongoDB support.",
    )
    mongo_db_name: str = Field("bi_agent_dev", description="MongoDB database name")

    # ── App ───────────────────────────────────────────────────────────────────
    app_env: str = Field(
        "development", description="development | staging | production"
    )
    app_port: int = Field(8000, description="Port the FastAPI server listens on")
    log_level: str = Field("INFO", description="DEBUG | INFO | WARNING | ERROR")

    # ── Schema graph ──────────────────────────────────────────────────────────
    schema_graph_path: Path = Field(
        Path("./schema_graph.json"),
        description="Where the crawled schema graph JSON is saved",
    )
    schema_sample_limit: int = Field(
        50,
        description="How many documents to sample per Mongo collection for schema inference",
    )

    # ── Cache (Redis — optional) ───────────────────────────────────────────────
    redis_url: Optional[str] = Field(
        None,
        description="Redis connection URL. Leave empty to disable caching.",
    )
    cache_ttl_seconds: int = Field(
        300,
        description="How long query results are cached (seconds)",
    )

    # ── Vector store (Qdrant — optional) ─────────────────────────────────────
    vector_store_url: Optional[str] = Field(
        None,
        description="Qdrant URL. Leave empty to skip vector storage (Phase 1 only needs the JSON file).",
    )
    vector_collection_name: str = Field(
        "bi_schema",
        description="Name of the Qdrant collection that stores schema embeddings",
    )

    # ── Derived helpers ───────────────────────────────────────────────────────
    @property
    def is_development(self) -> bool:
        return self.app_env == "development"

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def caching_enabled(self) -> bool:
        return bool(self.redis_url)

    @property
    def vector_store_enabled(self) -> bool:
        return bool(self.vector_store_url)

    @property
    def sql_is_sqlite(self) -> bool:
        return self.sql_db_url.startswith("sqlite")

    # ── Validation ────────────────────────────────────────────────────────────
    @field_validator("log_level")
    def validate_log_level(cls, v):
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.upper()

    @field_validator("app_env")
    def validate_app_env(cls, v):
        allowed = {"development", "staging", "production"}
        if v not in allowed:
            raise ValueError(f"app_env must be one of {allowed}")
        return v

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False  # OPENAI_API_KEY and openai_api_key both work


# Singleton — import this everywhere, never Settings() directly
settings = Settings()
