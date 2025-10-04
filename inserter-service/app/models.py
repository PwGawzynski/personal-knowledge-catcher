from typing import List, Optional, Dict, Any
from pydantic import BaseModel


class SearchRequest(BaseModel):
    vector: List[float]
    limit: int = 10
    score_threshold: Optional[float] = None


class QdrantHit(BaseModel):
    id: str
    score: float
    payload: Dict[str, Any]


class SearchResponse(BaseModel):
    results: List[QdrantHit]
