import time
from typing import List

from fastapi import FastAPI, HTTPException

from .config import logger
from .models import SearchRequest, SearchResponse, QdrantHit
from .qdrant import qdrant_search_points, qdrant_connection_test
from .consumer import embed_consumer_loop
import asyncio


app = FastAPI(title="Inserter Service (upsert + search)")


@app.on_event("startup")
async def _startup():
    logger.info("Inserter Service startup: spawning embed consumer loop")
    asyncio.create_task(embed_consumer_loop())
    # Fire-and-forget Qdrant connectivity test
    asyncio.create_task(qdrant_connection_test())


@app.post("/search", response_model=SearchResponse)
async def search(req: SearchRequest):
    try:
        logger.info(f"/search called: vector_len={len(req.vector)} limit={req.limit} thr={req.score_threshold}")
        t0 = time.monotonic()
        hits = await qdrant_search_points(req.vector, req.limit, req.score_threshold)
        out: List[QdrantHit] = []
        for h in hits:
            hid = h.get("id")
            if isinstance(hid, (int, float)):
                hid = str(hid)
            elif isinstance(hid, dict) and "uuid" in hid:
                hid = str(hid["uuid"])
            else:
                hid = str(hid)
            out.append(QdrantHit(id=hid, score=float(h.get("score", 0.0)), payload=h.get("payload", {}) or {}))
        took = (time.monotonic() - t0) * 1000.0
        logger.info(f"/search results: {len(out)} took={took:.2f}ms")
        return SearchResponse(results=out)
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=502, detail="search failed")
