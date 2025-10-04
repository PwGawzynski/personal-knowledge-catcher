import os
import pathlib
from typing import Literal
from pydantic import BaseModel, Field, validator


class Settings(BaseModel):
    # General (all required)
    data_dir: str = Field(..., alias="DATA_DIR")
    max_text_length: int = Field(..., alias="MAX_TEXT_LENGTH", ge=1)

    # Whisper
    chunk_seconds: int = Field(..., alias="CHUNK_SECONDS", ge=1)
    whisper_workers: int = Field(..., alias="WHISPER_WORKERS", ge=1)
    whisper_model: str = Field(..., alias="WHISPER_MODEL")
    whisper_device: Literal["cpu", "cuda"] = Field(..., alias="WHISPER_DEVICE")
    whisper_compute: str = Field(..., alias="WHISPER_COMPUTE")

    # Splitter
    window_seconds: float = Field(..., alias="WINDOW_SECONDS", ge=0)
    silence_min_d: float = Field(..., alias="SILENCE_MIN_D", ge=0)
    min_seg_sec: float = Field(..., alias="MIN_SEG_SEC", ge=0)
    pad_sec: float = Field(..., alias="PAD_SEC", ge=0)
    noise_offset_db: float = Field(..., alias="NOISE_OFFSET_DB")
    noise_min_db: float = Field(..., alias="NOISE_MIN_DB")
    noise_max_db: float = Field(..., alias="NOISE_MAX_DB")
    global_nearest_limit: float = Field(..., alias="GLOBAL_NEAREST_LIMIT")

    # Redis (all required, no fallbacks)
    redis_url: str = Field(..., alias="REDIS_URL")
    redis_host: str = Field(..., alias="REDIS_HOST")
    redis_port: int = Field(..., alias="REDIS_PORT")
    redis_db: int = Field(..., alias="REDIS_DB")
    # password may be empty string depending on deployment, but must be present
    redis_password: str = Field(..., alias="REDIS_PASSWORD")
    redis_env: str = Field(..., alias="REDIS_ENV")
    redis_ingest_list: str = Field(..., alias="REDIS_INGEST_LIST")
    redis_brpop_timeout: int = Field(..., alias="REDIS_BRPOP_TIMEOUT", ge=0)
    redis_embed_list: str = Field(..., alias="REDIS_EMBED_LIST")
    ingest_user_id: str = Field(..., alias="INGEST_USER_ID")

    @validator(
        "data_dir",
        "whisper_model",
        "whisper_device",
        "whisper_compute",
        "redis_url",
        "redis_host",
        "redis_env",
        "redis_ingest_list",
        "redis_embed_list",
        "ingest_user_id",
        pre=True,
    )
    def _require_non_empty(cls, v: str) -> str:
        if v is None:
            raise ValueError("required env var missing")
        if isinstance(v, str) and v.strip() == "":
            raise ValueError("env var must not be empty")
        return v


def _load_settings() -> Settings:
    env = os.environ
    kwargs = {
        # General
        "DATA_DIR": env.get("DATA_DIR"),
        "MAX_TEXT_LENGTH": env.get("MAX_TEXT_LENGTH"),
        # Whisper
        "CHUNK_SECONDS": env.get("CHUNK_SECONDS"),
        "WHISPER_WORKERS": env.get("WHISPER_WORKERS"),
        "WHISPER_MODEL": env.get("WHISPER_MODEL"),
        "WHISPER_DEVICE": env.get("WHISPER_DEVICE"),
        "WHISPER_COMPUTE": env.get("WHISPER_COMPUTE"),
        # Splitter
        "WINDOW_SECONDS": env.get("WINDOW_SECONDS"),
        "SILENCE_MIN_D": env.get("SILENCE_MIN_D"),
        "MIN_SEG_SEC": env.get("MIN_SEG_SEC"),
        "PAD_SEC": env.get("PAD_SEC"),
        "NOISE_OFFSET_DB": env.get("NOISE_OFFSET_DB"),
        "NOISE_MIN_DB": env.get("NOISE_MIN_DB"),
        "NOISE_MAX_DB": env.get("NOISE_MAX_DB"),
        "GLOBAL_NEAREST_LIMIT": env.get("GLOBAL_NEAREST_LIMIT"),
        # Redis
        "REDIS_URL": env.get("REDIS_URL"),
        "REDIS_HOST": env.get("REDIS_HOST"),
        "REDIS_PORT": env.get("REDIS_PORT"),
        "REDIS_DB": env.get("REDIS_DB"),
        "REDIS_PASSWORD": env.get("REDIS_PASSWORD"),
        "REDIS_ENV": env.get("REDIS_ENV"),
        "REDIS_INGEST_LIST": env.get("REDIS_INGEST_LIST"),
        "REDIS_BRPOP_TIMEOUT": env.get("REDIS_BRPOP_TIMEOUT"),
        "REDIS_EMBED_LIST": env.get("REDIS_EMBED_LIST"),
        "INGEST_USER_ID": env.get("INGEST_USER_ID"),
    }
    return Settings(**kwargs)


try:
    _S = _load_settings()
except Exception as e:
    raise RuntimeError(f"Invalid video-processor configuration: {e}") from e


# Expose validated constants, preserving existing names used by modules
DATA_DIR_PATH = _S.data_dir
DATA_DIR = pathlib.Path(DATA_DIR_PATH)
DATA_DIR.mkdir(parents=True, exist_ok=True)

MAX_TEXT_LENGTH = _S.max_text_length

CHUNK_SECONDS = _S.chunk_seconds
WHISPER_WORKERS = _S.whisper_workers
WHISPER_MODEL = _S.whisper_model
WHISPER_DEVICE = _S.whisper_device
WHISPER_COMPUTE = _S.whisper_compute

WINDOW_SECONDS = _S.window_seconds
SILENCE_MIN_D = _S.silence_min_d
MIN_SEG_SEC = _S.min_seg_sec
PAD_SEC = _S.pad_sec
NOISE_OFFSET_DB = _S.noise_offset_db
NOISE_MIN_DB = _S.noise_min_db
NOISE_MAX_DB = _S.noise_max_db
GLOBAL_NEAREST_LIMIT = _S.global_nearest_limit

REDIS_URL = _S.redis_url
REDIS_HOST = _S.redis_host
REDIS_PORT = _S.redis_port
REDIS_DB = _S.redis_db
REDIS_PASSWORD = _S.redis_password
REDIS_ENV = _S.redis_env
REDIS_INGEST_LIST = _S.redis_ingest_list
REDIS_BRPOP_TIMEOUT = _S.redis_brpop_timeout
REDIS_EMBED_LIST = _S.redis_embed_list
INGEST_USER_ID = _S.ingest_user_id


