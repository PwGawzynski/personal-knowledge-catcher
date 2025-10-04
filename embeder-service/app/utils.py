from time import monotonic
from datetime import datetime
from uuid import UUID


def to_jsonable(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    return value


def now_ms() -> float:
    return monotonic() * 1000.0


def preprocess_batch(texts):
    stripped_texts = [t.strip() for t in texts]
    return stripped_texts


