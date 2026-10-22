"""Runtime configuration for the IQ Analyzer serving layer."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

DISCLAIMER = "Información exclusivamente referencial. No constituye asesoramiento financiero."

BASE_DIR = Path(__file__).resolve().parent
REPO_DIR = BASE_DIR.parent


def _path_from_env(name: str, default: Path) -> Path:
    raw = os.getenv(name)
    return Path(raw).expanduser() if raw else default


def _bool_from_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    app_name: str
    version: str
    environment: str
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_db: str
    ollama_base_url: str
    ollama_model: str
    ollama_timeout_seconds: float
    ollama_embedding_model: str
    groq_api_key: str
    groq_base_url: str
    groq_model: str
    groq_timeout_seconds: float
    semantic_router_enabled: bool
    semantic_router_threshold: float
    semantic_router_cache_path: Path
    rag_index_path: Path
    rag_docs_dir: Path
    catalog_path: Path
    glossary_path: Path
    frontend_dir: Path
    static_dir: Path
    cors_origins: tuple[str, ...]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    frontend_dir = _path_from_env("FRONTEND_DIR", REPO_DIR / "frontend")
    static_dir = _path_from_env("STATIC_DIR", Path("/app/static"))
    docs_dir = _path_from_env("RAG_DOCS_DIR", REPO_DIR / "metadata")

    origins = os.getenv("CORS_ORIGINS", "*")
    parsed_origins = tuple(origin.strip() for origin in origins.split(",") if origin.strip())

    return Settings(
        app_name="IQ Analyzer Serving API",
        version=os.getenv("APP_VERSION", "6.0.0"),
        environment=os.getenv("ENVIRONMENT", "development"),
        postgres_host=os.getenv("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
        postgres_user=os.getenv("POSTGRES_USER", "postgres"),
        postgres_password=os.getenv("POSTGRES_PASSWORD", "postgres_secure_pass"),
        postgres_db=os.getenv("POSTGRES_DB", "market_platform"),
        ollama_base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        ollama_model=os.getenv("OLLAMA_MODEL", "qwen2.5:3b-instruct"),
        ollama_timeout_seconds=float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "60")),
        ollama_embedding_model=os.getenv("OLLAMA_EMBEDDING_MODEL", "bge-m3"),
        groq_api_key=os.getenv("GROQ_API_KEY", ""),
        groq_base_url=os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1"),
        groq_model=os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"),
        groq_timeout_seconds=float(os.getenv("GROQ_TIMEOUT_SECONDS", "30")),
        semantic_router_enabled=_bool_from_env("SEMANTIC_ROUTER_ENABLED", True),
        semantic_router_threshold=float(os.getenv("SEMANTIC_ROUTER_THRESHOLD", "0.72")),
        semantic_router_cache_path=_path_from_env(
            "SEMANTIC_ROUTER_CACHE_PATH", REPO_DIR / "metadata" / "router_embeddings.json"
        ),
        rag_index_path=_path_from_env("RAG_INDEX_PATH", REPO_DIR / "metadata" / "rag_index.json"),
        rag_docs_dir=docs_dir,
        catalog_path=_path_from_env("CATALOG_PATH", docs_dir / "catalog.json"),
        glossary_path=_path_from_env("GLOSSARY_PATH", docs_dir / "glossary.json"),
        frontend_dir=frontend_dir,
        static_dir=static_dir,
        cors_origins=parsed_origins or ("*",),
    )


def reset_settings_cache() -> None:
    """Test helper: clear env-derived settings."""
    get_settings.cache_clear()
