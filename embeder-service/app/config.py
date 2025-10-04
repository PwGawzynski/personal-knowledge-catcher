import os
import logging
from typing import Literal
from pydantic import BaseModel, Field, validator


class Settings(BaseModel):
    # Batching / scheduler
    batch_size: int = Field(..., alias="BATCH_SIZE", ge=1)
    max_wait_ms: int = Field(..., alias="MAX_WAIT_MS", ge=0)
    poll_sleep_ms: float = Field(..., alias="POLL_SLEEP_MS", ge=0)
    max_search_per_batch_if_ingest: int = Field(..., alias="MAX_SEARCH_PER_BATCH_IF_INGEST", ge=0)
    max_search_per_batch_if_empty: int = Field(..., alias="MAX_SEARCH_PER_BATCH_IF_EMPTY", ge=0)

    vector_dim: int = Field(..., alias="VECTOR_DIM", ge=1)
    max_concurrent_search: int = Field(..., alias="MAX_CONCURRENT_SEARCH", ge=1)

    # Limits
    max_text_length: int = Field(..., alias="MAX_TEXT_LENGTH", ge=1)

    # Redis
    redis_url: str = Field(..., alias="REDIS_URL")
    redis_ingest_list: str = Field(..., alias="REDIS_INGEST_LIST")
    redis_embed_list: str = Field(..., alias="REDIS_EMBED_LIST")
    redis_upsert_list: str = Field(..., alias="REDIS_UPSERT_LIST")
    redis_brpop_timeout: int = Field(..., alias="REDIS_BRPOP_TIMEOUT", ge=0)

    # Qdrant
    qdrant_host: str = Field(..., alias="QDRANT_HOST")
    qdrant_api_key: str = Field(..., alias="QDRANT_API_KEY")
    qdrant_collection: str = Field(..., alias="COLLECTION_NAME")
    qdrant_vectors_mode: Literal["single", "named"] = Field(..., alias="VECTORS_MODE")
    qdrant_vector_name: str = Field(..., alias="VECTOR_NAME")
    inserter_batch_upsert: int = Field(..., alias="INSERTER_BATCH_UPSERT", ge=1)
    inserter_flush_ms: int = Field(..., alias="INSERTER_FLUSH_MS", ge=0)

    # Logging
    log_level: str = Field(..., alias="LOG_LEVEL")

    @validator("qdrant_host", pre=True)
    def _strip_trailing_slash(cls, v: str) -> str:
        return (v or "").rstrip("/")

    @validator(
        "redis_url",
        "redis_ingest_list",
        "redis_embed_list",
        "redis_upsert_list",
        "qdrant_host",
        "qdrant_api_key",
        "qdrant_collection",
        "qdrant_vector_name",
        "log_level",
        pre=True,
    )
    def _require_non_empty(cls, v: str) -> str:
        if v is None:
            raise ValueError("required env var missing")
        if isinstance(v, str) and v.strip() == "":
            raise ValueError("env var must not be empty")
        return v

    @validator("qdrant_vectors_mode", pre=True)
    def _normalize_vectors_mode(cls, v: str) -> str:
        if v is None or (isinstance(v, str) and v.strip() == ""):
            raise ValueError("VECTORS_MODE must be provided")
        return v.lower()

    @validator("log_level")
    def _upper_level(cls, v: str) -> str:
        return (v or "").upper()


def _load_settings() -> Settings:
    # Build kwargs from environment explicitly to avoid silent defaults
    env = os.environ
    kwargs = {
        "BATCH_SIZE": env.get("BATCH_SIZE"),
        "MAX_WAIT_MS": env.get("MAX_WAIT_MS"),
        "POLL_SLEEP_MS": env.get("POLL_SLEEP_MS"),
        "MAX_SEARCH_PER_BATCH_IF_INGEST": env.get("MAX_SEARCH_PER_BATCH_IF_INGEST"),
        "MAX_SEARCH_PER_BATCH_IF_EMPTY": env.get("MAX_SEARCH_PER_BATCH_IF_EMPTY"),
        "VECTOR_DIM": env.get("VECTOR_DIM"),
        "MAX_CONCURRENT_SEARCH": env.get("MAX_CONCURRENT_SEARCH"),
        "MAX_TEXT_LENGTH": env.get("MAX_TEXT_LENGTH"),
        "REDIS_URL": env.get("REDIS_URL"),
        "REDIS_INGEST_LIST": env.get("REDIS_INGEST_LIST"),
        "REDIS_EMBED_LIST": env.get("REDIS_EMBED_LIST"),
        "REDIS_UPSERT_LIST": env.get("REDIS_UPSERT_LIST"),
        "REDIS_BRPOP_TIMEOUT": env.get("REDIS_BRPOP_TIMEOUT"),
        "QDRANT_HOST": env.get("QDRANT_HOST"),
        "QDRANT_API_KEY": env.get("QDRANT_API_KEY"),
        "COLLECTION_NAME": env.get("COLLECTION_NAME"),
        "VECTORS_MODE": env.get("VECTORS_MODE"),
        "VECTOR_NAME": env.get("VECTOR_NAME"),
        "INSERTER_BATCH_UPSERT": env.get("INSERTER_BATCH_UPSERT"),
        "INSERTER_FLUSH_MS": env.get("INSERTER_FLUSH_MS"),
        "LOG_LEVEL": env.get("LOG_LEVEL"),
    }
    return Settings(**kwargs)


try:
    _SETTINGS = _load_settings()
except Exception as e:
    # Fail fast with clear message
    raise RuntimeError(f"Invalid embeder-service configuration: {e}") from e


# Expose validated constants for existing imports
BATCH_SIZE = _SETTINGS.batch_size
MAX_WAIT_MS = _SETTINGS.max_wait_ms
POLL_SLEEP_MS = _SETTINGS.poll_sleep_ms
MAX_SEARCH_PER_BATCH_IF_INGEST = _SETTINGS.max_search_per_batch_if_ingest
MAX_SEARCH_PER_BATCH_IF_EMPTY = _SETTINGS.max_search_per_batch_if_empty

VECTOR_DIM = _SETTINGS.vector_dim
MAX_CONCURRENT_SEARCH = _SETTINGS.max_concurrent_search

MAX_TEXT_LENGTH = _SETTINGS.max_text_length

REDIS_URL = _SETTINGS.redis_url
REDIS_INGEST_LIST = _SETTINGS.redis_ingest_list
REDIS_EMBED_LIST = _SETTINGS.redis_embed_list
REDIS_UPSERT_LIST = _SETTINGS.redis_upsert_list
REDIS_BRPOP_TIMEOUT = _SETTINGS.redis_brpop_timeout

QDRANT_HOST = _SETTINGS.qdrant_host
QDRANT_API_KEY = _SETTINGS.qdrant_api_key
QDRANT_COLLECTION = _SETTINGS.qdrant_collection
QDRANT_VECTORS_MODE = _SETTINGS.qdrant_vectors_mode
QDRANT_VECTOR_NAME = _SETTINGS.qdrant_vector_name
INSERTER_BATCH_UPSERT = _SETTINGS.inserter_batch_upsert
INSERTER_FLUSH_MS = _SETTINGS.inserter_flush_ms

LOG_LEVEL = _SETTINGS.log_level

# Configure basic logging level globally if not already configured
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))