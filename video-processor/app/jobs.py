import asyncio
import uuid
from typing import Dict, Any, List, Callable, Optional
from collections import defaultdict

class JobBus:
    """
    Prosty pubsub per job_id — do /monitor (WebSocket).
    """
    def __init__(self) -> None:
        self._subs: Dict[str, List[asyncio.Queue]] = defaultdict(list)

    def subscribe(self, job_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subs[job_id].append(q)
        return q

    def unsubscribe(self, job_id: str, q: asyncio.Queue) -> None:
        lst = self._subs.get(job_id, [])
        if q in lst:
            lst.remove(q)

    async def publish(self, job_id: str, event: Dict[str, Any]) -> None:
        for q in self._subs.get(job_id, []):
            await q.put(event)

bus = JobBus()

class JobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, Dict[str, Any]] = {}

    def new_job(self) -> str:
        jid = uuid.uuid4().hex
        self._jobs[jid] = {"state": "queued", "artifacts": {}, "error": None}
        return jid

    def set_state(self, jid: str, state: str, detail: Optional[str]=None) -> None:
        if jid in self._jobs:
            self._jobs[jid]["state"] = state
            if detail:
                self._jobs[jid]["detail"] = detail

    def add_artifact(self, jid: str, key: str, value: Any) -> None:
        if jid in self._jobs:
            self._jobs[jid]["artifacts"].setdefault(key, [])
            self._jobs[jid]["artifacts"][key].append(value)

    def set_error(self, jid: str, msg: str) -> None:
        if jid in self._jobs:
            self._jobs[jid]["state"] = "error"
            self._jobs[jid]["error"] = msg

    def get(self, jid: str) -> Optional[Dict[str, Any]]:
        return self._jobs.get(jid)

store = JobStore()