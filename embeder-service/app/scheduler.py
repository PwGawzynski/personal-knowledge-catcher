import asyncio
import json
import logging
from typing import List, Dict, Tuple, Optional

from .config import (
    BATCH_SIZE,
    MAX_WAIT_MS,
    POLL_SLEEP_MS,
    MAX_SEARCH_PER_BATCH_IF_INGEST,
    MAX_SEARCH_PER_BATCH_IF_EMPTY,
    INSERTER_BATCH_UPSERT,
    INSERTER_FLUSH_MS,
    REDIS_URL,
    REDIS_UPSERT_LIST,
)
from .logging_conf import logger
from .model import EmbedModel
from .utils import now_ms, preprocess_batch, to_jsonable
from .queues import q_search, q_ingest, _embed_pub
from .qdrant import build_qdrant_points, qdrant_upsert_points


_shed_stop = asyncio.Event()
_scheduler_task: Optional[asyncio.Task] = None
_inserter_task: Optional[asyncio.Task] = None


async def embed_batch(model: EmbedModel, texts: List[str]) -> List[List[float]]:
    batch_size = len(texts)
    start_time = now_ms()
    prepped = preprocess_batch(texts)
    vectors = await model.embed(prepped)
    duration_ms = now_ms() - start_time
    logger.info(
        f"Completed embedding batch: {batch_size} texts in {duration_ms:.2f}ms ({duration_ms/batch_size:.2f}ms per text)"
    )
    return vectors


async def scheduler_loop(model: EmbedModel):
    logger.info("Scheduler loop started")
    batch_counter = 0

    while not _shed_stop.is_set():
        ingest_present = not q_ingest.empty()
        target_search = MAX_SEARCH_PER_BATCH_IF_INGEST if ingest_present else MAX_SEARCH_PER_BATCH_IF_EMPTY

        batch_types: List[str] = []
        batch_ids: List[str] = []
        batch_texts: List[str] = []
        batch_futs: List[asyncio.Future] = []
        batch_ingest_metas: List[Dict[str, object]] = []
        t_start = now_ms()

        while len(batch_texts) < min(target_search, BATCH_SIZE) and (now_ms() - t_start) < MAX_WAIT_MS:
            timeout_left = MAX_WAIT_MS - (now_ms() - t_start)
            if timeout_left <= 0:
                break
            try:
                item = await asyncio.wait_for(q_search.get(), timeout=timeout_left / 1000.0)
                req_id, text, fut = item
                batch_types.append("search")
                batch_ids.append(req_id)
                batch_texts.append(text)
                batch_futs.append(fut)
            except asyncio.TimeoutError:
                await asyncio.sleep(POLL_SLEEP_MS / 1000.0)

        while len(batch_texts) < BATCH_SIZE:
            if not q_ingest.empty():
                doc_chunk_id, text, metadata = q_ingest.get_nowait()
                batch_types.append("ingest")
                batch_ids.append(doc_chunk_id)
                batch_texts.append(text)
                batch_ingest_metas.append(metadata)
            elif not q_search.empty():
                req_id, text, fut = q_search.get_nowait()
                batch_types.append("search")
                batch_ids.append(req_id)
                batch_texts.append(text)
                batch_futs.append(fut)
            else:
                break

        if not batch_texts:
            await asyncio.sleep(POLL_SLEEP_MS / 1000.0)
            continue

        search_count = batch_types.count("search")
        ingest_count = batch_types.count("ingest")
        collection_time = now_ms() - t_start
        batch_counter += 1
        logger.info(
            f"Batch #{batch_counter}: {len(batch_texts)} items (search: {search_count}, ingest: {ingest_count}) collected in {collection_time:.2f}ms, remaining search: {q_search.qsize()}, remaining ingest: {q_ingest.qsize()}"
        )

        try:
            vectors = await embed_batch(model, batch_texts)
        except Exception as e:
            logger.error(f"Error in embed_batch for batch #{batch_counter}: {e}")
            for t, _id in zip(batch_types, batch_ids):
                if t == "search":
                    fut = batch_futs.pop(0)
                    if not fut.done():
                        fut.set_exception(RuntimeError(f"Embedding failed: {e}"))
            continue

        vi = 0
        mi = 0
        search_completed = 0
        ingest_queued = 0
        for t, _id in zip(batch_types, batch_ids):
            if t == "search":
                fut = batch_futs.pop(0)
                if not fut.done():
                    fut.set_result(vectors[vi])
                    search_completed += 1
            else:
                try:
                    metadata = batch_ingest_metas[mi] if mi < len(batch_ingest_metas) else {}
                    mi += 1
                    safe_payload = to_jsonable(metadata)
                    item = {"id": _id, "vector": vectors[vi], "payload": safe_payload}
                    global _embed_pub  # from queues module
                    if _embed_pub is None:
                        import redis.asyncio as aioredis  # type: ignore
                        from .config import REDIS_URL as _RU

                        from .queues import _embed_pub as _pub_ref
                        _pub_ref = aioredis.from_url(_RU, encoding="utf-8", decode_responses=True)
                        # assign back to module-level reference
                        import importlib
                        mod = importlib.import_module("app.queues")
                        setattr(mod, "_embed_pub", _pub_ref)
                        _embed_pub = _pub_ref
                        logger.info(f"ITEM PUBLISHED: {item}")
                    await _embed_pub.rpush(REDIS_UPSERT_LIST, json.dumps(item))
                    ingest_queued += 1
                except Exception as e:
                    logger.error(f"Failed to publish embed for {_id}: {e}")
            vi += 1
        logger.info(
            f"Batch #{batch_counter} completed: {search_completed} search responses, {ingest_queued} vectors queued for insertion"
        )





def request_stop():
    _shed_stop.set()


