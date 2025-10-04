from typing import List, Optional
from uuid import UUID
from datetime import datetime

from pydantic import BaseModel, HttpUrl


class EmbedSearchRequest(BaseModel):
    text: str


class EmbedSearchResponse(BaseModel):
    vector: List[float]


class VideoTimestamp(BaseModel):
    time_from: float
    time_to: float


class IngestItem(BaseModel):
    doc_chunk_id: str
    text: str
    user_id: str
    source: str  # youtube|tiktok|article|other
    title: str
    lang: str    # pl|en|…
    created_at: datetime
    source_url: HttpUrl
    source_id: UUID
    video_timestamp: Optional[VideoTimestamp] = None


