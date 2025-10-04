import json
import asyncio
import time
from typing import List

from .config import (
    REDIS_URL,
    REDIS_UPSERT_LIST,
    REDIS_BRPOP_TIMEOUT,
    QDRANT_VECTORS_MODE,
    QDRANT_VECTOR_NAME,
    QDRANT_COLLECTION,
    logger,
)
from .qdrant import qdrant_upsert_points


redis = None


async def embed_consumer_loop():
    global redis
    import redis.asyncio as aioredis

    logger.info(
        f"Upsert consumer connecting to {REDIS_URL}, list={REDIS_UPSERT_LIST}, "
        f"collection={QDRANT_COLLECTION}, vectors_mode={QDRANT_VECTORS_MODE}, vector_name={QDRANT_VECTOR_NAME}"
    )
    try:
        redis = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        await redis.ping()
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return

    processed = 0
    failed = 0
    started_at = time.monotonic()
    try:
        while True:
            kv = await redis.brpop(REDIS_UPSERT_LIST, timeout=REDIS_BRPOP_TIMEOUT)
            if not kv:
                await asyncio.sleep(0)
                continue
            _, payload = kv
            try:
                obj = json.loads(payload)
            except Exception as e:
                logger.warning(f"Invalid JSON on upsert queue: {e}")
                continue
            # Expect: {"id": str, "vector": [float], "payload": {...}}
            vec = obj.get("vector")
            meta = obj.get("payload") or {}
            pid = meta.get("doc_chunk_id") or obj.get("id") or ""
            if not isinstance(vec, list) or not vec:
                logger.warning(f"Invalid embed item (no vector): {obj}")
                continue
            logger.debug(f"Embed item received: id={pid} vector_len={len(vec)}")
            point = {
                "id": pid,
                "payload": meta,
                "vector": {QDRANT_VECTOR_NAME: vec} if QDRANT_VECTORS_MODE == "named" else vec,
            }
            try:
                t0 = time.monotonic()
                await qdrant_upsert_points([point])
                took = (time.monotonic() - t0) * 1000.0
                processed += 1
                if processed % 50 == 0:
                    rate = processed / max(0.001, (time.monotonic() - started_at))
                    logger.info(
                        f"Upserted #{processed} (last id={pid}) took={took:.2f}ms, rate={rate:.2f} msg/s, failed={failed}"
                    )
            except Exception as e:
                failed += 1
                logger.error(f"Upsert failed for {pid}: {e}")
                await asyncio.sleep(0.05)
                continue
    finally:
        try:
            if redis:
                await redis.close()
        except Exception:
            pass
