import os, json, uuid, pathlib, re, html
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional


from .config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PASSWORD,
    REDIS_EMBED_LIST,
    MAX_TEXT_LENGTH,
    INGEST_USER_ID,
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_timecode_to_seconds(tc: str) -> float:
    # Supports "HH:MM:SS,mmm" and "HH:MM:SS.mmm"
    m = re.match(r"^(\d{2}):(\d{2}):(\d{2})[\.,](\d{3})$", tc.strip())
    if not m:
        return 0.0
    h, mnt, s, ms = [int(x) for x in m.groups()]
    return h * 3600 + mnt * 60 + s + ms / 1000.0


def _parse_any_time(value: str) -> float:
    """Parse various time formats: HH:MM:SS.mmm, HH:MM:SS,mmm, seconds float, or '12.34s'."""
    v = (value or "").strip()
    if not v:
        return 0.0
    # trailing 's'
    if v.endswith("s") and re.match(r"^[0-9]+(\.[0-9]+)?s$", v):
        try:
            return float(v[:-1])
        except Exception:
            return 0.0
    # plain float seconds
    if re.match(r"^[0-9]+(\.[0-9]+)?$", v):
        try:
            return float(v)
        except Exception:
            return 0.0
    # formatted time
    return _parse_timecode_to_seconds(v)


def _normalize_text_basic(text: str) -> str:
    """Normalize subtitle text: unescape, drop simple tags, collapse whitespace, collapse repeats."""
    if not text:
        return ""
    t = text.replace("\\N", " ")
    t = html.unescape(t)
    # drop simple HTML-like tags
    t = re.sub(r"<[^>]+>", "", t)
    # collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    # collapse immediate repeats: 3-gram, 2-gram, 1-gram
    for pat in (
        r"\b(\S+\s+\S+\s+\S+)(?:\s+\1)+\b",
        r"\b(\S+\s+\S+)(?:\s+\1)+\b",
        r"\b(\S+)(?:\s+\1)+\b",
    ):
        prev = None
        # iterate until stable or small number of iterations
        for _ in range(3):
            if prev == t:
                break
            prev = t
            t = re.sub(pat, r"\1", t, flags=re.IGNORECASE)
    return t


def _remove_overlap(prev_text: str, new_text: str, min_tokens: int = 3, max_tokens: int = 30) -> str:
    """Remove overlapping prefix in new_text that duplicates the suffix of prev_text (token-based)."""
    if not prev_text or not new_text:
        return new_text
    prev_tokens = prev_text.split()
    new_tokens = new_text.split()
    limit = min(len(prev_tokens), len(new_tokens), max_tokens)
    for l in range(limit, min_tokens - 1, -1):
        if prev_tokens[-l:] == new_tokens[:l]:
            return " ".join(new_tokens[l:])
    # try a lighter overlap for 2 tokens if nothing found
    if len(prev_tokens) >= 2 and len(new_tokens) >= 2 and min_tokens > 2:
        if prev_tokens[-2:] == new_tokens[:2]:
            return " ".join(new_tokens[2:])
    return new_text


def _parse_srt(content: str) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    lines = [ln.rstrip("\n") for ln in content.splitlines()]
    i = 0
    while i < len(lines):
        # Skip index line if present
        if re.match(r"^\d+$", lines[i].strip()):
            i += 1
        if i >= len(lines):
            break
        m = re.match(r"^(\d{2}:\d{2}:\d{2},\d{3})\s+-->\s+(\d{2}:\d{2}:\d{2},\d{3})", lines[i])
        if not m:
            i += 1
            continue
        start = _parse_timecode_to_seconds(m.group(1))
        end = _parse_timecode_to_seconds(m.group(2))
        i += 1
        texts: List[str] = []
        while i < len(lines) and lines[i].strip() != "":
            texts.append(lines[i].strip())
            i += 1
        # Skip blank separator
        while i < len(lines) and lines[i].strip() == "":
            i += 1
        if texts:
            segments.append({"start": float(start), "end": float(end), "text": " ".join(texts).strip()})
    return segments


def _parse_vtt(content: str) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    lines = [ln.rstrip("\n") for ln in content.splitlines()]
    i = 0
    # Skip optional WEBVTT header and any metadata lines until empty line or timecode
    while i < len(lines) and (not re.match(r"^\d{2}:\d{2}:\d{2}[\.,]\d{3}\s+-->\s+", lines[i])):
        i += 1
    while i < len(lines):
        m = re.match(r"^(\d{2}:\d{2}:\d{2}[\.,]\d{3})\s+-->\s+(\d{2}:\d{2}:\d{2}[\.,]\d{3})", lines[i])
        if not m:
            i += 1
            continue
        start = _parse_timecode_to_seconds(m.group(1))
        end = _parse_timecode_to_seconds(m.group(2))
        i += 1
        texts: List[str] = []
        while i < len(lines) and lines[i].strip() != "":
            # Strip styling tags common in VTT
            txt = _normalize_text_basic(lines[i])
            if txt:
                # avoid immediate duplicate lines within the same cue
                if not texts or txt != texts[-1]:
                    texts.append(txt)
            i += 1
        while i < len(lines) and lines[i].strip() == "":
            i += 1
        if texts:
            segments.append({"start": float(start), "end": float(end), "text": " ".join(texts).strip()})
    return segments


def _parse_ttml(content: str) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(content)
    except Exception:
        return segments
    # TTML namespaces vary; search for 'p' elements anywhere
    for p in root.iter():
        if p.tag.endswith('p'):
            text = ''.join(p.itertext()).strip()
            if not text:
                continue
            begin = p.attrib.get('begin') or p.attrib.get('{http://www.w3.org/ns/ttml#parameter}begin')
            end = p.attrib.get('end') or p.attrib.get('{http://www.w3.org/ns/ttml#parameter}end')
            dur = p.attrib.get('dur') or p.attrib.get('{http://www.w3.org/ns/ttml#parameter}dur')
            start_s = _parse_any_time(begin or '0')
            if end:
                end_s = _parse_any_time(end)
            elif dur:
                end_s = start_s + _parse_any_time(dur)
            else:
                end_s = start_s
            segments.append({"start": float(start_s), "end": float(end_s), "text": text})
    return segments


def _parse_srv_xml(content: str) -> List[Dict[str, Any]]:
    """Parse YouTube XML captions (srv1/srv2/srv3)."""
    segments: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(content)
    except Exception:
        return segments
    for node in root.iter():
        if node.tag.endswith('text'):
            try:
                start = node.attrib.get('start') or node.attrib.get('t')
                dur = node.attrib.get('dur') or node.attrib.get('d')
                if start is None:
                    continue
                start_s = _parse_any_time(start)
                end_s = start_s + _parse_any_time(dur or '0')
                txt = html.unescape(''.join(node.itertext()).replace('\n', ' ').strip())
                if txt:
                    segments.append({"start": float(start_s), "end": float(end_s), "text": txt})
            except Exception:
                continue
    return segments


def _parse_json3(content: str) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    try:
        data = json.loads(content)
    except Exception:
        return segments
    events = []
    if isinstance(data, dict) and isinstance(data.get('events'), list):
        events = data['events']
    elif isinstance(data, list):
        events = data
    for ev in events:
        try:
            t0 = ev.get('tStartMs')
            d = ev.get('dDurationMs') or ev.get('dDuration')
            segs = ev.get('segs') or []
            text_parts: List[str] = []
            for s in segs:
                t = (s.get('utf8') or '').replace('\n', ' ').strip()
                if t:
                    text_parts.append(t)
            if not text_parts or t0 is None:
                continue
            start_s = float(t0) / 1000.0
            end_s = start_s + (float(d) / 1000.0 if d is not None else 0.0)
            segments.append({"start": start_s, "end": end_s, "text": ' '.join(text_parts)})
        except Exception:
            continue
    return segments


def _parse_ass(content: str) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    for line in content.splitlines():
        if not line.lstrip().startswith('Dialogue:'):
            continue
        try:
            body = line.split(':', 1)[1].strip()
            parts = body.split(',', 9)
            if len(parts) < 10:
                continue
            start_str = parts[1].strip()
            end_str = parts[2].strip()
            text = parts[9].strip()
            m = re.match(r"^(\d+):(\d{2}):(\d{2})\.(\d{2})$", start_str)
            if not m:
                continue
            h, mnt, s, cs = [int(x) for x in m.groups()]
            start_s = h * 3600 + mnt * 60 + s + cs / 100.0
            m2 = re.match(r"^(\d+):(\d{2}):(\d{2})\.(\d{2})$", end_str)
            if not m2:
                continue
            h2, mnt2, s2, cs2 = [int(x) for x in m2.groups()]
            end_s = h2 * 3600 + mnt2 * 60 + s2 + cs2 / 100.0
            txt = re.sub(r"{[^}]*}", "", text)  # strip ASS style overrides
            txt = txt.replace("\\N", " ").strip()
            if txt:
                segments.append({"start": float(start_s), "end": float(end_s), "text": txt})
        except Exception:
            continue
    return segments


def parse_subtitles_file(path: str) -> List[Dict[str, Any]]:
    """
    Parse subtitles using pysubs2; on failure, fallback to parsers based on extension
    for common formats: VTT/SRT/TTML/XML (srv), JSON3, ASS/SSA.
    Returns segments: {start, end, text} with times in seconds.
    """
    ext = pathlib.Path(path).suffix.lower()
    # First try pysubs2 for best compatibility
    try:
        import pysubs2  # type: ignore
        subs = None
        for enc in ("utf-8", "utf-8-sig", None):
            try:
                subs = pysubs2.load(path, encoding=enc)
                break
            except Exception:
                continue
        if subs is not None:
            segments: List[Dict[str, Any]] = []
            for ev in subs:
                try:
                    text = _normalize_text_basic((ev.text or "").replace("\n", " "))
                    if not text:
                        continue
                    start_s = float(ev.start) / 1000.0
                    end_s = float(ev.end) / 1000.0
                    segments.append({"start": start_s, "end": end_s, "text": text})
                except Exception:
                    continue
            if segments:
                return segments
    except Exception:
        pass

    # Fallback by extension using simple parsers
    try:
        data = pathlib.Path(path).read_text(encoding="utf-8")
    except Exception:
        try:
            data = pathlib.Path(path).read_text(errors="ignore")
        except Exception:
            data = ""

    if ext == ".vtt":
        return _parse_vtt(data)
    if ext == ".srt":
        return _parse_srt(data)
    if ext in (".ttml", ".dfxp", ".xml"):
        return _parse_ttml(data)
    if ext in (".srv1", ".srv2", ".srv3"):
        return _parse_srv_xml(data)
    if ext == ".json3":
        return _parse_json3(data)
    if ext in (".ass", ".ssa"):
        return _parse_ass(data)

    # Unknown extension: try VTT then SRT heuristics
    segs = _parse_vtt(data)
    if segs:
        return segs
    return _parse_srt(data)


def _pick_captions_file(caps: List[str], lang: Optional[str]) -> Optional[str]:
    # Prefer exact language prefix match if provided
    candidates = caps[:]
    if lang:
        pref = [c for c in candidates if pathlib.Path(c).name.startswith(f"{lang}.")]
        if pref:
            candidates = pref
    # Prefer VTT, then SRT
    for ext in (".vtt", ".srt"):
        for c in candidates:
            if c.lower().endswith(ext):
                return c
    # Fallback to any
    return candidates[0] if candidates else None


def _chunk_segments_into_texts(segments: List[Dict[str, Any]], max_len: int) -> List[Dict[str, Any]]:
    chunks: List[Dict[str, Any]] = []
    cur_text: List[str] = []
    cur_start: Optional[float] = None
    cur_end: Optional[float] = None

    def flush():
        nonlocal cur_text, cur_start, cur_end
        if cur_text and cur_start is not None and cur_end is not None:
            chunks.append({
                "text": " ".join(cur_text).strip(),
                "time_from": float(cur_start),
                "time_to": float(cur_end),
            })
        cur_text = []
        cur_start = None
        cur_end = None

    last_added_text: Optional[str] = None
    for seg in segments:
        text = _normalize_text_basic(seg.get("text") or "")
        if last_added_text:
            text = _remove_overlap(last_added_text, text)
        if not text:
            continue
        seg_len = len(text)
        if not cur_text:
            cur_text = [text]
            cur_start = float(seg.get("start", 0.0))
            cur_end = float(seg.get("end", seg.get("start", 0.0)))
            if seg_len >= max_len:
                flush()
                last_added_text = None
            continue
        # +1 for space
        next_len = len(" ".join(cur_text)) + 1 + seg_len
        if next_len > max_len:
            flush()
            cur_text = [text]
            cur_start = float(seg.get("start", 0.0))
            cur_end = float(seg.get("end", seg.get("start", 0.0)))
            if seg_len >= max_len:
                flush()
                last_added_text = None
        else:
            cur_text.append(text)
            cur_end = float(seg.get("end", seg.get("start", 0.0)))
        last_added_text = text
    flush()
    return chunks


def _build_payload(base: Dict[str, Any], chunk: Dict[str, Any]) -> Dict[str, Any]:
    # Ensure source_id is a UUID; if not, derive uuid5 from source_url
    source_id = base["source_id"]
    try:
        _ = uuid.UUID(str(source_id))
        valid_source_id = str(source_id)
    except Exception:
        ns_uuid = uuid.uuid5(uuid.NAMESPACE_URL, base.get("source_url", ""))
        valid_source_id = ns_uuid.hex
    payload = {
        "doc_chunk_id": str(uuid.uuid4()),
        "text": chunk["text"],
        "user_id": base.get("user_id") or INGEST_USER_ID,
        "source": base["source"],
        "title": base["title"],
        "lang": base.get("lang") or "UNKNOW",
        "created_at": base.get("created_at") or _iso_now(),
        "source_url": base["source_url"],
        "source_id": valid_source_id,
        "video_timestamp": {
            "time_from": float(chunk["time_from"]),
            "time_to": float(chunk["time_to"]),
        },
    }
    return payload


def _redis_client():
    try:
        import redis  # type: ignore
    except Exception as e:
        raise RuntimeError("redis-py is required to enqueue ingest payloads") from e
    url = os.getenv("REDIS_URL")
    if url:
        return redis.from_url(url, decode_responses=True)
    # tolerate accidental schema in REDIS_HOST
    host = REDIS_HOST
    if isinstance(host, str) and host.startswith("redis://"):
        try:
            return redis.from_url(host, decode_responses=True)
        except Exception:
            host = host.replace("redis://", "")
    return redis.StrictRedis(host=host, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD, decode_responses=True)


def enqueue_payloads(payloads: List[Dict[str, Any]]) -> int:
    if not payloads:
        return 0
    r = _redis_client()
    # Use pipeline for efficiency
    pipe = r.pipeline()
    for p in payloads:
        print(f"[av-worker] pushing payload: {p}")
        try:
            pipe.rpush(REDIS_EMBED_LIST, json.dumps(p, ensure_ascii=False))
        except Exception as e:
            print(f"[av-worker] error pushing payload: {e}")            
    res = pipe.execute()
    # Return number of successful pushes
    return sum(1 for x in res if isinstance(x, int) and x >= 0)


def ingest_from_captions(
    *,
    job_dir: str,
    captions: List[str],
    selected_lang: Optional[str],
    source_url: str,
    video_id: str,
    video_title: Optional[str],
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    cap_file = _pick_captions_file(captions, selected_lang)
    if not cap_file:
        return {"enqueued": 0, "reason": "no_captions_found"}
    segments = parse_subtitles_file(cap_file)
    chunks = _chunk_segments_into_texts(segments, MAX_TEXT_LENGTH)
    base = {
        "user_id": user_id,
        "source": source_url,
        "title": video_title or video_id,
        "lang": selected_lang or "UNKNOWN",
        "created_at": _iso_now(),
        "source_url": source_url,
        "source_id": video_id,
    }
    payloads = [_build_payload(base, ch) for ch in chunks]
    count = enqueue_payloads(payloads)
    return {"enqueued": count, "chunks": len(chunks), "file": cap_file}


def ingest_from_whisper(
    *,
    job_dir: str,
    srt_path: str,
    source_url: str,
    video_id: str,
    video_title: Optional[str],
    lang: Optional[str],
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    segments = parse_subtitles_file(srt_path)
    chunks = _chunk_segments_into_texts(segments, MAX_TEXT_LENGTH)
    base = {
        "user_id": user_id,
        "source": source_url,
        "title": video_title or video_id,
        "lang": lang or "UNKNOWN",
        "created_at": _iso_now(),
        "source_url": source_url,
        "source_id": video_id,
    }
    payloads = [_build_payload(base, ch) for ch in chunks]
    count = enqueue_payloads(payloads)
    return {"enqueued": count, "chunks": len(chunks), "file": srt_path}


