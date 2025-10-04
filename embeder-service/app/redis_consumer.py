import asyncio
import json
import logging
from typing import Tuple, Optional, Dict
from uuid import UUID
from datetime import datetime

from .config import (
    REDIS_URL,
    REDIS_EMBED_LIST,
    REDIS_BRPOP_TIMEOUT,
    MAX_TEXT_LENGTH,
)
from .queues import q_ingest
from .model import EmbedModel

logger = logging.getLogger(__name__)

redis = None


async def redis_consumer_loop(model: EmbedModel):
    global redis
    import redis.asyncio as aioredis  # type: ignore

    logger.info(f"Redis consumer loop starting - connecting to {REDIS_URL}, list: {REDIS_EMBED_LIST}")

    try:
        redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        await redis.ping()
        logger.info("Redis connection established successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return

    message_count = 0
    error_count = 0

    try:
        while True:
            try:
                kv: Tuple[str, str] = await redis.brpop(REDIS_EMBED_LIST, timeout=REDIS_BRPOP_TIMEOUT)
                if not kv:
                    await asyncio.sleep(0)
                    continue
                try:
                    _key, payload = kv
                    logger.info(f"Redis message received: {payload}")
                except Exception as e:
                    logger.error(f"Error parsing Redis message: {e}")
                    continue

                try:
                    obj = json.loads(payload)
                    doc_chunk_id = (obj.get("doc_chunk_id") or "").strip()
                    text = (obj.get("text") or "").strip()
                    user_id = (obj.get("user_id") or "").strip()
                    source = (obj.get("source") or "").strip()
                    title = (obj.get("title") or "").strip()
                    lang = (obj.get("lang") or "").strip()
                    created_at_raw = (obj.get("created_at") or "").strip()
                    source_url = (obj.get("source_url") or "").strip()
                    source_id_raw = (obj.get("source_id") or "").strip()
                    video_ts_obj = obj.get("video_timestamp")

                    if not (doc_chunk_id and text and user_id and source and title and lang and created_at_raw and source_url and source_id_raw):
                        logger.warning(f"Invalid ingest message - required fields missing: {obj}")
                        error_count += 1
                        continue
                    if len(text) > MAX_TEXT_LENGTH:
                        logger.warning(f"Ingest message too long: {len(text)} > {MAX_TEXT_LENGTH} (doc_chunk_id={doc_chunk_id})")
                        error_count += 1
                        continue

                    try:
                        created_at_dt = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                    except Exception:
                        logger.warning(f"Invalid created_at format: {created_at_raw}")
                        error_count += 1
                        continue

                    source_id = None
                    try:
                        source_id = UUID(source_id_raw)
                    except Exception:
                        try:
                            source_id = int(source_id_raw)
                        except Exception:
                            try:
                                source_id = float(source_id_raw)
                            except Exception:
                                logger.warning(f"Invalid source_id (not UUID or number): {source_id_raw}")
                                error_count += 1
                                continue

                    try:
                        _user_uuid = UUID(user_id)
                    except Exception:
                        logger.warning(f"Invalid user_id UUID: {user_id}")
                        error_count += 1
                        continue

                    try:
                        _doc_chunk_uuid = UUID(doc_chunk_id)
                    except Exception:
                        logger.warning(f"Invalid doc_chunk_id UUID: {doc_chunk_id}")
                        error_count += 1
                        continue

                    video_timestamp: Optional[Dict[str, float]] = None
                    if isinstance(video_ts_obj, dict):
                        try:
                            tf = float(video_ts_obj.get("time_from"))
                            tt = float(video_ts_obj.get("time_to"))
                            if tf < 0 or tt < 0 or tf > tt:
                                raise ValueError("Invalid video_timestamp range")
                            video_timestamp = {"time_from": tf, "time_to": tt}
                        except Exception:
                            logger.warning(f"Invalid video_timestamp: {video_ts_obj}")
                            error_count += 1
                            continue

                    metadata = {
                        "user_id": user_id,
                        "source": source,
                        "title": title,
                        "lang": lang,
                        "model": model.model_name,
                        "created_at": created_at_dt,
                        "source_url": source_url,
                        "source_id": source_id,
                    }
                    if video_timestamp is not None:
                        metadata["video_timestamp"] = video_timestamp

                    await q_ingest.put((doc_chunk_id, text, metadata))
                    message_count += 1
                    if message_count % 100 == 0:
                        logger.info(f"Processed {message_count} messages from Redis, {error_count} errors")

                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode error: {e}, payload: {payload[:100]}...")
                    error_count += 1
                    continue

            except Exception as e:
                logger.error(f"Redis consumer error: {e}")
                error_count += 1
                await asyncio.sleep(0.1)

    finally:
        logger.info(f"Redis consumer stopping - processed {message_count} messages, {error_count} errors")
        try:
            if redis:
                await redis.close()
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")


