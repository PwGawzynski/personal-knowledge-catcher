import json
import asyncio
import time
import urllib.request
import urllib.error
from typing import List, Dict, Any, Optional

from .config import (
    QDRANT_HOST,
    QDRANT_COLLECTION,
    QDRANT_VECTORS_MODE,
    QDRANT_VECTOR_NAME,
    QDRANT_API_KEY,
    logger,
)


def _qdrant_request(method: str, path: str, payload: dict, timeout: float = 10.0) -> dict:
    url = f"{QDRANT_HOST}{path}"
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if QDRANT_API_KEY:
        headers["api-key"] = QDRANT_API_KEY
    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        try:
            return json.loads(body)
        except Exception:
            return {"status": "unknown", "raw": body}


def _qdrant_get(path: str, timeout: float = 5.0) -> dict:
    url = f"{QDRANT_HOST}{path}"
    headers = {"Content-Type": "application/json"}
    if QDRANT_API_KEY:
        headers["Api-Key"] = QDRANT_API_KEY
    req = urllib.request.Request(url=url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            try:
                return json.loads(body)
            except Exception:
                return {"status": "unknown", "raw": body}
    except urllib.error.HTTPError as e:
        # Try to parse error body for more context
        try:
            raw = e.read().decode("utf-8")
        except Exception:
            raw = str(e)
        try:
            return json.loads(raw)
        except Exception:
            return {"status": "http_error", "code": e.code, "reason": e.reason, "raw": raw}


async def qdrant_upsert_points(points: List[dict]) -> dict:
    payload = {"points": points}
    path = f"/collections/{QDRANT_COLLECTION}/points?wait=true"
    loop = asyncio.get_running_loop()
    t0 = time.monotonic()
    resp = await loop.run_in_executor(None, lambda: _qdrant_request("PUT", path, payload))
    dt = (time.monotonic() - t0) * 1000.0
    status = resp.get("status") if isinstance(resp, dict) else "unknown"
    logger.info(f"Qdrant upsert: points={len(points)} status={status} took={dt:.2f}ms")
    return resp


async def qdrant_search_points(vector: List[float], limit: int, score_threshold: Optional[float]) -> List[dict]:
    payload: Dict[str, Any] = {
        "limit": limit,
        "with_payload": True,
        "with_vector": False,
    }
    if QDRANT_VECTORS_MODE == "named":
        payload["using"] = QDRANT_VECTOR_NAME
    payload["vector"] = vector
    if score_threshold is not None:
        payload["score_threshold"] = score_threshold
    loop = asyncio.get_running_loop()
    path = f"/collections/{QDRANT_COLLECTION}/points/search"
    t0 = time.monotonic()
    resp = await loop.run_in_executor(None, lambda: _qdrant_request("POST", path, payload))
    dt = (time.monotonic() - t0) * 1000.0
    results = resp.get("result", []) if isinstance(resp, dict) else []
    logger.info(f"Qdrant search: results={len(results)} limit={limit} took={dt:.2f}ms")
    return results


async def qdrant_connection_test() -> None:
    """Test connection to Qdrant and log the outcome."""
    loop = asyncio.get_running_loop()
    if not QDRANT_API_KEY:
        logger.error("Qdrant API key not found")
        raise ValueError("Qdrant API key not found")
    try:
        t0 = time.monotonic()
        # Try to fetch collection info (works whether or not it exists; we log both cases)
        info = await loop.run_in_executor(None, lambda: _qdrant_get(f"/collections/{QDRANT_COLLECTION}"))
        took_ms = (time.monotonic() - t0) * 1000.0
        status = info.get("status", "unknown") if isinstance(info, dict) else "unknown"
        exists = False
        if isinstance(info, dict):
            result = info.get("result")
            # When collection exists, result is an object; otherwise status may be error
            exists = bool(result) and isinstance(result, dict)
        logger.info(
            f"Qdrant connection test: host={QDRANT_HOST} collection={QDRANT_COLLECTION} status={status} exists={exists} took={took_ms:.2f}ms"
        )
    except Exception as e:
        logger.error(f"Qdrant connection test failed: {e}")
