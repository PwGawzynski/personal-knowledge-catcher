import asyncio
import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from .schemas import EmbedSearchRequest, EmbedSearchResponse, IngestItem
from .config import MAX_TEXT_LENGTH, MAX_CONCURRENT_SEARCH
from .queues import q_search, q_ingest
from .utils import now_ms

router = APIRouter()
logger = logging.getLogger(__name__)

_active_search = asyncio.Semaphore(MAX_CONCURRENT_SEARCH)
_request_ctr = 0
_req_lock = asyncio.Lock()


@router.post("/embedsearch", response_model=EmbedSearchResponse)
async def embed_search(req: EmbedSearchRequest):
    start_time = now_ms()
    text = (req.text or "").strip()

    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    if len(text) > MAX_TEXT_LENGTH:
        raise HTTPException(status_code=400, detail=f"text too long (max {MAX_TEXT_LENGTH} chars)")

    global _request_ctr
    async with _req_lock:
        _request_ctr += 1
        request_id = f"rq_{_request_ctr}"

    fut = asyncio.get_running_loop().create_future()
    async with _active_search:
        await q_search.put((request_id, text, fut))
        try:
            vec = await asyncio.wait_for(fut, timeout=10.0)
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="search timeout")
    return EmbedSearchResponse(vector=vec)


@router.post("/ingest")
async def ingest_item(item: IngestItem):
    text = (item.text or "").strip()
    if not item.doc_chunk_id or not text:
        raise HTTPException(status_code=400, detail="doc_chunk_id and text are required")
    if len(text) > MAX_TEXT_LENGTH:
        raise HTTPException(status_code=400, detail=f"text too long (max {MAX_TEXT_LENGTH} chars)")

    # Pydantic has validated UUIDs and HttpUrl
    video_timestamp = None
    if item.video_timestamp is not None:
        tf = float(item.video_timestamp.time_from)
        tt = float(item.video_timestamp.time_to)
        if tf < 0 or tt < 0 or tf > tt:
            raise HTTPException(status_code=400, detail="Invalid video_timestamp range")
        video_timestamp = {"time_from": tf, "time_to": tt}

    metadata = {
        "user_id": item.user_id,
        "source": item.source,
        "title": item.title,
        "lang": item.lang,
        "model": "BAAI/bge-m3",
        "created_at": item.created_at,
        "source_url": str(item.source_url),
        "source_id": item.source_id,
    }
    if video_timestamp is not None:
        metadata["video_timestamp"] = video_timestamp

    await q_ingest.put((item.doc_chunk_id, text, metadata))
    return JSONResponse({"status": "queued", "doc_chunk_id": item.doc_chunk_id})


@router.get("/healthz")
async def healthz():
    return {"status": "ok"}


