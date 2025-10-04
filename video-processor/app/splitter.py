import os, re, json, shlex, pathlib, subprocess, time
from .config import (
    WINDOW_SECONDS,
    SILENCE_MIN_D,
    MIN_SEG_SEC,
    PAD_SEC,
    NOISE_OFFSET_DB,
    NOISE_MIN_DB,
    NOISE_MAX_DB,
    GLOBAL_NEAREST_LIMIT,
)
from typing import List, Dict, Any, Optional, Tuple

from .jobs import bus


def _run(cmd: str) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


def _probe_duration(wav_path: str) -> float:
    res = _run(f'ffprobe -v error -show_entries format=duration -of json {shlex.quote(wav_path)}')
    data = json.loads(res.stdout or '{}')
    return float(data.get('format', {}).get('duration', 0.0))


def _compute_noise_floor_db(wav_path: str,
                            noise_offset_db: float,
                            noise_min_db: float,
                            noise_max_db: float) -> Tuple[float, float]:
    cmd_stats = (
        f'ffmpeg -hide_banner -nostats -i {shlex.quote(wav_path)} '
        f'-af "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level" '
        f'-f null -'
    )
    res = _run(cmd_stats)
    rms: List[float] = []
    for line in (res.stderr or '').splitlines():
        m = re.search(r'RMS_level=([\-\d\.]+)', line)
        if m:
            try:
                rms.append(float(m.group(1)))
            except ValueError:
                pass
    if not rms:
        raise RuntimeError("No RMS_level data; input not readable")
    rms_sorted = sorted(rms)
    p10 = rms_sorted[max(0, int(0.10 * (len(rms_sorted) - 1)))]
    noise = max(noise_min_db, min(noise_max_db, p10 - noise_offset_db))
    return p10, noise


def _detect_silences(wav_path: str, noise_db: float, min_silence_d: float) -> List[Dict[str, float]]:
    cmd_sil = (
        f'ffmpeg -hide_banner -nostats -i {shlex.quote(wav_path)} '
        f'-af "silencedetect=noise={noise_db:.1f}dB:d={min_silence_d}" -f null -'
    )
    res = _run(cmd_sil)
    silences: List[Dict[str, float]] = []
    cur: Dict[str, float] = {}
    for line in (res.stderr or '').splitlines():
        ms = re.search(r'silence_start:\s*([0-9\.]+)', line)
        me = re.search(r'silence_end:\s*([0-9\.]+)\s*\|\s*silence_duration:\s*([0-9\.]+)', line)
        if ms:
            cur = {"start": float(ms.group(1))}
        if me and cur:
            end = float(me.group(1))
            dur = float(me.group(2))
            cur["end"] = end
            cur["dur"] = dur
            cur["mid"] = cur["start"] + dur / 2.0
            silences.append(cur)
            cur = {}
    return silences


def _nearest_silence(points: List[float], t: float) -> Tuple[Optional[float], float]:
    if not points:
        return None, float("inf")
    best = None
    bestd = float("inf")
    for p in points:
        d = abs(p - t)
        if d < bestd:
            bestd = d
            best = p
    return best, bestd


def _dedup_sorted(seq: List[float], eps: float = 0.25) -> List[float]:
    out: List[float] = []
    for x in sorted(seq):
        if not out or abs(x - out[-1]) > eps:
            out.append(x)
    return out


def split_audio_by_silence(
    job_id: Optional[str],
    wav_path: str,
    out_dir: pathlib.Path,
    chunk_seconds: int,
) -> Dict[str, Any]:
    """
    Split audio by aligning target cut points (~chunk_seconds) to detected silences.
    Returns: {"chunks": [paths], "stats": {...}}
    """

    # Params from validated config
    window_seconds = float(WINDOW_SECONDS)
    min_silence_d = float(SILENCE_MIN_D)
    min_seg_sec = float(MIN_SEG_SEC)
    pad_sec = float(PAD_SEC)
    noise_offset_db = float(NOISE_OFFSET_DB)
    noise_min_db = float(NOISE_MIN_DB)
    noise_max_db = float(NOISE_MAX_DB)
    global_nearest_limit = float(GLOBAL_NEAREST_LIMIT)

    t0 = time.time()
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Duration and noise floor
    duration = _probe_duration(wav_path)
    p10_db, noise_db = _compute_noise_floor_db(wav_path, noise_offset_db, noise_min_db, noise_max_db)
    if job_id:
        try:
            # Inform about computed thresholds
            msg = {
                "stage": "segmentation",
                "info": {
                    "duration": duration,
                    "p10_db": round(p10_db, 2),
                    "noise_db": round(noise_db, 2),
                    "chunk_seconds": chunk_seconds,
                    "window_seconds": window_seconds,
                    "min_silence_d": min_silence_d,
                    "min_seg_sec": min_seg_sec,
                    "pad_sec": pad_sec,
                    "global_nearest_limit": global_nearest_limit,
                }
            }
            # Fire-and-forget
            import asyncio
            asyncio.create_task(bus.publish(job_id, {"type": "log", "job_id": job_id, "data": msg}))
        except Exception:
            pass

    # 2) Detect silences
    silences = _detect_silences(wav_path, noise_db, min_silence_d)
    noise_db_used = noise_db
    silence_passes = 1
    if len(silences) == 0:
        # Adaptive: relax threshold progressively to find low-energy gaps
        for relax_db in (5.0, 10.0, 15.0):
            trial_noise = max(noise_min_db, noise_db - relax_db)
            trial_min_d = max(0.05, min_silence_d * 0.5)
            silences = _detect_silences(wav_path, trial_noise, trial_min_d)
            silence_passes += 1
            if silences:
                noise_db_used = trial_noise
                min_silence_d = trial_min_d
                break
    silence_points = [s["mid"] for s in silences]

    # 3) Compute target cuts and snap to silence
    cuts: List[float] = [0.0]
    n_targets = max(0, int(duration // float(chunk_seconds)))
    for n in range(1, n_targets + 1):
        target = n * float(chunk_seconds)
        if target >= duration:
            break
        p, d = _nearest_silence(silence_points, target)
        chosen = None
        if p is not None and d <= window_seconds:
            chosen = p
        else:
            if p is not None and d <= global_nearest_limit:
                chosen = p
        if chosen is not None:
            if chosen - cuts[-1] >= min_seg_sec:
                cuts.append(chosen)

    if duration - cuts[-1] >= min_seg_sec:
        cuts.append(duration)

    cuts = _dedup_sorted(cuts)

    # 4) Build segments and cut
    def sec(x: float) -> float:
        return max(0.0, min(duration, x))

    segments: List[Tuple[float, float]] = []
    for i in range(len(cuts) - 1):
        a, b = cuts[i], cuts[i + 1]
        if b - a >= min_seg_sec:
            segments.append((sec(a + pad_sec), sec(b - pad_sec)))

    # Fallback to uniform segmentation if the above produced no or only one large segment
    used_method = "silence"
    if len(segments) <= 1 and duration > float(chunk_seconds) * 1.25:
        used_method = "uniform-fallback"
        # Optional: inform via bus
        if job_id:
            try:
                import asyncio
                asyncio.create_task(bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"stage": "segmentation", "fallback": used_method}}))
            except Exception:
                pass
        # Cut equal parts of chunk_seconds
        segments = []
        start = 0.0
        while start < duration:
            end = min(duration, start + float(chunk_seconds))
            if end - start >= max(5.0, pad_sec * 2):
                segments.append((sec(start + pad_sec), sec(end - pad_sec)))
            start = end

    # Perform cuts
    made_files: List[str] = []
    offsets: List[float] = []
    for i, (start, end) in enumerate(segments, 1):
        out = out_dir / f"chunk_{i:06d}.wav"
        cmd_cut = (
            f'ffmpeg -y -i {shlex.quote(wav_path)} '
            f'-ss {start:.3f} -to {end:.3f} -c copy {shlex.quote(str(out))}'
        )
        _run(cmd_cut)
        made_files.append(str(out))
        offsets.append(float(start))

    t1 = time.time()
    stats: Dict[str, Any] = {
        "segmentation_method": used_method,
        "duration_seconds": duration,
        "p10_db": p10_db,
        "noise_db": noise_db,
        "noise_db_used": noise_db_used,
        "silences_detected": len(silences),
        "selected_cuts": len(cuts),
        "segments_made": len(made_files),
        "silence_passes": silence_passes,
        "window_seconds": window_seconds,
        "min_silence_d": min_silence_d,
        "min_seg_sec": min_seg_sec,
        "pad_sec": pad_sec,
        "global_nearest_limit": global_nearest_limit,
        "segmentation_seconds": t1 - t0,
    }

    return {"chunks": made_files, "offsets": offsets, "stats": stats}


