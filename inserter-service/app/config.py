import os
import logging
from typing import Literal
from pydantic import BaseModel, Field, validator


class Settings(BaseModel):
    # Logging (required, no defaults)
    log_level: str = Field(..., alias="LOG_LEVEL")

    # Redis
    redis_url: str = Field(..., alias="REDIS_URL")
    # Consume from UPSERT list by design; support EMBED list for backward-compat if provided only
    redis_upsert_list: str = Field(..., alias="REDIS_UPSERT_LIST")
    redis_brpop_timeout: int = Field(..., alias="REDIS_BRPOP_TIMEOUT", ge=0)

    # Qdrant
    qdrant_host: str = Field(..., alias="QDRANT_HOST")
    qdrant_api_key: str = Field(..., alias="QDRANT_API_KEY")
    qdrant_collection: str = Field(..., alias="COLLECTION_NAME")
    qdrant_vectors_mode: Literal["single", "named"] = Field(..., alias="VECTORS_MODE")
    qdrant_vector_name: str = Field(..., alias="VECTOR_NAME")

    @validator("qdrant_host", pre=True)
    def _strip_trailing_slash(cls, v: str) -> str:
        return (v or "").rstrip("/")

    @validator("log_level")
    def _upper_level(cls, v: str) -> str:
        return (v or "").upper()

    @validator(
        "redis_url",
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


def _load_settings() -> Settings:
    env = os.environ
    kwargs = {
        "LOG_LEVEL": env.get("LOG_LEVEL"),
        "REDIS_URL": env.get("REDIS_URL"),
        "REDIS_UPSERT_LIST": env.get("REDIS_UPSERT_LIST"),
        "REDIS_BRPOP_TIMEOUT": env.get("REDIS_BRPOP_TIMEOUT"),
        "QDRANT_HOST": env.get("QDRANT_HOST"),
        "QDRANT_API_KEY": env.get("QDRANT_API_KEY"),
        "COLLECTION_NAME": env.get("COLLECTION_NAME"),
        "VECTORS_MODE": env.get("VECTORS_MODE"),
        "VECTOR_NAME": env.get("VECTOR_NAME"),
    }
    return Settings(**kwargs)


try:
    _SETTINGS = _load_settings()
except Exception as e:
    raise RuntimeError(f"Invalid inserter-service configuration: {e}") from e


# Expose constants
LOG_LEVEL = _SETTINGS.log_level
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

REDIS_URL = _SETTINGS.redis_url
REDIS_UPSERT_LIST = _SETTINGS.redis_upsert_list
REDIS_BRPOP_TIMEOUT = _SETTINGS.redis_brpop_timeout

QDRANT_HOST = _SETTINGS.qdrant_host
QDRANT_API_KEY = _SETTINGS.qdrant_api_key
QDRANT_COLLECTION = _SETTINGS.qdrant_collection
QDRANT_VECTORS_MODE = _SETTINGS.qdrant_vectors_mode
QDRANT_VECTOR_NAME = _SETTINGS.qdrant_vector_name
