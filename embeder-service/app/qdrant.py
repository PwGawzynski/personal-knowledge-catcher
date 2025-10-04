import asyncio
import json
import logging
import urllib.request
import urllib.error
from typing import List, Dict, Any, Optional, Tuple
from uuid import UUID
from datetime import datetime

from .config import QDRANT_HOST, QDRANT_API_KEY, QDRANT_COLLECTION, QDRANT_VECTORS_MODE, QDRANT_VECTOR_NAME

logger = logging.getLogger(__name__)


def _to_jsonable(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(v) for v in value]
    return value


def build_qdrant_points(batch: List[Tuple[str, List[float], Dict[str, object]]]) -> List[dict]:
    points: List[dict] = []
    is_named = QDRANT_VECTORS_MODE == "named"
    for doc_chunk_id, vector, metadata in batch:
        payload: Dict[str, object] = {"doc_chunk_id": doc_chunk_id}
        if metadata:
            for k, v in metadata.items():
                payload[k] = _to_jsonable(v)
        point = {
            "id": doc_chunk_id,
            "payload": payload,
        }
        if is_named:
            point["vector"] = {QDRANT_VECTOR_NAME: vector}
        else:
            point["vector"] = vector
        points.append(point)
    return points


def _qdrant_request(method: str, path: str, payload: dict, timeout: float = 10.0) -> dict:
    url = f"{QDRANT_HOST}{path}"
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if QDRANT_API_KEY:
        headers["Api-Key"] = QDRANT_API_KEY
    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_bytes = resp.read()
            try:
                return json.loads(resp_bytes.decode("utf-8"))
            except Exception:
                return {"status": "unknown", "raw": resp_bytes.decode("utf-8", errors="ignore")}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Qdrant HTTPError {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Qdrant URLError: {e}") from e


async def qdrant_upsert_points(points: List[dict]) -> dict:
    payload = {"points": points}
    path = f"/collections/{QDRANT_COLLECTION}/points?wait=true"
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: _qdrant_request("PUT", path, payload))


