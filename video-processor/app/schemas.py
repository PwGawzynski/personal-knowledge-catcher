from pydantic import BaseModel, HttpUrl, Field
from typing import Optional, Literal, List, Dict, Any

class ExtractRequest(BaseModel):
    url: HttpUrl
    preferred_sub_formats: Optional[List[Literal["vtt","ttml","srv3","srv2","srv1","json3","srt"]]] = None
    languages: Optional[List[str]] = None   # np. ["pl","en","en-orig"]

class ExtractResponse(BaseModel):
    job_id: str
    message: str = "accepted"

class JobStatus(BaseModel):
    job_id: str
    state: Literal["queued","running","done","error"]
    detail: Optional[str] = None
    artifacts: Optional[Dict[str, Any]] = None  # np. listy plików

class Event(BaseModel):
    type: Literal["log","progress","state","artifact"]
    job_id: str
    data: Dict[str, Any] = Field(default_factory=dict)