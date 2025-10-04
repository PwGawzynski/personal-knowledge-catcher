import asyncio
import logging
from fastapi import FastAPI

from .logging_conf import logger
from .api import router as api_router
from .model import EmbedModel, log_gpu_info
from .config import REDIS_URL, REDIS_EMBED_LIST, REDIS_UPSERT_LIST
from .scheduler import scheduler_loop, request_stop
from .redis_consumer import redis_consumer_loop


app = FastAPI(title="Embedder Service (search-first, mixed-batching, Redis ingest)")

model: EmbedModel | None = None
_scheduler_task: asyncio.Task | None = None
_redis_consumer_task: asyncio.Task | None = None


@app.on_event("startup")
async def _startup():
    global model, _scheduler_task, _inserter_task, _redis_consumer_task
    logger.info("Starting Embedder Service...")
    log_gpu_info()
    logger.info(f"Redis config: url={REDIS_URL}, embed_list={REDIS_EMBED_LIST}, upsert_list={REDIS_UPSERT_LIST}")
    model = EmbedModel(model_name="BAAI/bge-m3")
    _scheduler_task = asyncio.create_task(scheduler_loop(model), name="scheduler")
    _redis_consumer_task = asyncio.create_task(redis_consumer_loop(model), name="redis-consumer")


@app.on_event("shutdown")
async def _shutdown():
    request_stop()
    tasks = [
        ("scheduler", _scheduler_task),
        ("redis-consumer", _redis_consumer_task),
    ]
    for name, task in tasks:
        if task:
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logging.getLogger(__name__).error(f"Error stopping {name} task: {e}")


app.include_router(api_router)


