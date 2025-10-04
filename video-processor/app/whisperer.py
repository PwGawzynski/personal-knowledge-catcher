import os, pathlib, asyncio, json, math, subprocess, shlex, time
from .config import (
    DATA_DIR,
    CHUNK_SECONDS,
    WHISPER_WORKERS,
    WHISPER_MODEL,
    WHISPER_DEVICE,
    WHISPER_COMPUTE,
)
from multiprocessing import Process, Queue
from typing import List, Dict, Any, Tuple
from faster_whisper import WhisperModel
from .jobs import bus, store
from .splitter import split_audio_by_silence


def ffmpeg_segment(wav_path: str, out_dir: pathlib.Path, chunk_sec: int) -> List[str]:
    """Legacy uniform segmentation (kept for fallback/testing)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = (
        f'ffmpeg -y -i {shlex.quote(wav_path)} -f segment -segment_time {chunk_sec} '
        f'-c copy {shlex.quote(str(out_dir / "chunk_%06d.wav"))}'
    )
    subprocess.run(cmd, shell=True, check=True)
    return [str(p) for p in sorted(out_dir.glob("chunk_*.wav"))]

def worker_proc(model_name: str, device: str, compute: str, in_q: Queue, out_q: Queue):
    model = WhisperModel(model_name, device=device, compute_type=compute)
    while True:
        item = in_q.get()
        if item is None:
            break
        job_id, idx, chunk_path = item
        try:
            segments, info = model.transcribe(chunk_path, vad_filter=True)
            out = []
            for s in segments:
                out.append({
                    "start": float(s.start or 0.0),
                    "end": float(s.end or 0.0),
                    "text": s.text.strip()
                })
            lang = None
            try:
                lang = getattr(info, "language", None)
            except Exception:
                lang = None
            out_q.put((job_id, idx, out, lang))
        except Exception as e:
            out_q.put((job_id, idx, {"error": str(e)}, None))

async def transcribe_chunks(job_id: str, wav_path: str, vid: str) -> Dict[str, Any]:
    t0 = time.time()
    # 1) pocięcie na segmenty
    # Chunks already named chunk_%06d.wav; keep directory name generic
    chunk_dir = pathlib.Path(wav_path).with_suffix("")
    chunk_dir = chunk_dir.parent / (chunk_dir.name + "_chunks")
    t_seg0 = time.time()
    split_info = split_audio_by_silence(job_id, wav_path, chunk_dir, CHUNK_SECONDS)
    chunks = split_info["chunks"]
    chunk_offsets = split_info.get("offsets", [i * CHUNK_SECONDS for i in range(len(chunks))])
    t_seg1 = time.time()

    await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"stage": "segmentation", "chunks": len(chunks), "method": split_info.get("stats", {}).get("segmentation_method")}})

    # 2) uruchom procesy
    in_q: Queue = Queue(maxsize=len(chunks))
    out_q: Queue = Queue()

    workers: List[Process] = []
    for _ in range(max(1, WHISPER_WORKERS)):
        p = Process(target=worker_proc, args=(WHISPER_MODEL, WHISPER_DEVICE, WHISPER_COMPUTE, in_q, out_q))
        p.start()
        workers.append(p)

    for idx, ch in enumerate(chunks):
        in_q.put((job_id, idx, ch))
    for _ in workers:
        in_q.put(None)

    # 3) zbieraj wyniki
    results: Dict[int, Any] = {}
    lang_counts: Dict[str, int] = {}
    received = 0
    total = len(chunks)
    while received < total:
        item = out_q.get()
        # Support tuples of len 3 (legacy) or 4 (with lang)
        if len(item) == 3:
            jid, idx, data = item
            lang = None
        else:
            jid, idx, data, lang = item
        results[idx] = {"data": data, "lang": lang}
        if isinstance(lang, str) and lang:
            lang_counts[lang] = lang_counts.get(lang, 0) + 1
        received += 1
        await bus.publish(job_id, {"type": "progress", "job_id": job_id, "data": {"received": received, "total": total}})

    # 4) dołącz segmenty w kolejności, z offsetem chunku
    merged: List[Dict[str, Any]] = []
    for idx in range(total):
        item = results[idx]
        data = item["data"]
        if isinstance(data, dict) and "error" in data:
            await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"chunk_error": data["error"], "chunk_idx": idx}})
            continue
        offset = float(chunk_offsets[idx]) if idx < len(chunk_offsets) else float(idx * CHUNK_SECONDS)
        for seg in data:
            merged.append({
                "start": seg["start"] + offset,
                "end": seg["end"] + offset,
                "text": seg["text"]
            })

    # 5) zapisz VTT i SRT
    out_dir = pathlib.Path(wav_path).parent
    # Remove videoid from output subtitles; use raw_video.whisper.{vtt,srt}
    vtt = out_dir / f"raw_video.whisper.vtt"
    srt = out_dir / f"raw_video.whisper.srt"

    def fmt_ts(t: float, srt_mode=False):
        ms = int(round((t - int(t)) * 1000))
        s = int(t) % 60
        m = (int(t) // 60) % 60
        h = int(t) // 3600
        if srt_mode:
            return f"{h:02}:{m:02}:{s:02},{ms:03}"
        return f"{h:02}:{m:02}:{s:02}.{ms:03}"

    # VTT
    with open(vtt, "w", encoding="utf-8") as f:
        f.write("WEBVTT\n\n")
        for i, seg in enumerate(merged, 1):
            f.write(f"{fmt_ts(seg['start'])} --> {fmt_ts(seg['end'])}\n{seg['text']}\n\n")

    # SRT
    with open(srt, "w", encoding="utf-8") as f:
        for i, seg in enumerate(merged, 1):
            f.write(f"{i}\n{fmt_ts(seg['start'], True)} --> {fmt_ts(seg['end'], True)}\n{seg['text']}\n\n")

    t1 = time.time()
    stats = {
        "segmentation_seconds": t_seg1 - t_seg0,
        "processing_seconds": t1 - t_seg1,
        "total_seconds": t1 - t0,
        "num_workers": max(1, WHISPER_WORKERS),
        "num_chunks": len(chunks),
        "chunk_seconds": CHUNK_SECONDS,
        "model": WHISPER_MODEL,
        "device": WHISPER_DEVICE,
        "compute": WHISPER_COMPUTE
    }
    # merge splitter stats if available
    if isinstance(split_info.get("stats"), dict):
        stats.update({f"split_{k}": v for k, v in split_info["stats"].items()})

    return {
        "vtt": str(vtt),
        "srt": str(srt),
        "chunks": chunks,
        "chunk_dir": str(chunk_dir),
        "wav_path": str(wav_path),
        "stats": stats,
        "lang": (sorted(lang_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0] if lang_counts else None),
    }