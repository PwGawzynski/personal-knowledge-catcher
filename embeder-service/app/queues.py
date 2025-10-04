import asyncio
from typing import Tuple, List, Dict


q_search: "asyncio.Queue[Tuple[str, str, asyncio.Future]]" = asyncio.Queue()
q_ingest: "asyncio.Queue[Tuple[str, str, Dict[str, object]]]" = asyncio.Queue()


_embed_pub = None  # lazy redis client for publishing vectors


