import os, shlex, subprocess, pathlib, asyncio, re, json, time, uuid
from .config import DATA_DIR
from typing import List, Dict, Any, Optional, Tuple
from .jobs import bus, store

DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---- helpers ---------------------------------------------------------------

def run(cmd: str, capture: bool=False) -> str:
    proc = subprocess.run(
        cmd, shell=True, text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        check=False
    )
    return (proc.stdout or "") if capture else ""

def get_video_id(url: str) -> str:
    out = run(f'yt-dlp --get-id {shlex.quote(url)}', capture=True).strip()
    # If yt-dlp failed or returned an error blob, fall back to deterministic UUID from URL
    if not out:
        return uuid.uuid5(uuid.NAMESPACE_URL, url).hex
    if "ERROR:" in out or "Cannot parse" in out or "\n" in out or "\r" in out:
        return uuid.uuid5(uuid.NAMESPACE_URL, url).hex
    # Some providers may return composite IDs; normalize whitespace
    token = out.split()[0].strip()
    if not token or token.upper().startswith("ERROR"):
        return uuid.uuid5(uuid.NAMESPACE_URL, url).hex
    return token

def get_video_info(url: str) -> Optional[Dict[str, Any]]:
    """
    Zwraca metadane wideo z yt-dlp (-J / --dump-json)
    """
    raw = run(f'yt-dlp -J {shlex.quote(url)}', capture=True).strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None

def parse_list_subs(output: str) -> Dict[str, List[str]]:
    """
    Parsuje wynik `yt-dlp --list-subs URL`.
    Zwraca mapę: lang_code -> [formats...]
    Łączy sekcje "Available automatic captions" i "Available subtitles".
    """
    lines = output.splitlines()
    subs: Dict[str, List[str]] = {}

    # Szukamy wierszy typu:
    # en-orig  English (Original)    vtt, ttml, srv3, srv2, srv1, json3
    # Kolumny są rozdzielone >=2 spacjami.
    row_re = re.compile(r"^([A-Za-z0-9\-]+)\s{2,}.*?\s{2,}([A-Za-z0-9,\.\s]+)$")

    capture = False
    for line in lines:
        if "Available automatic captions" in line or "Available subtitles" in line:
            capture = True
            continue
        if capture:
            line = line.strip()
            if not line:
                # pusta linia zwykle kończy sekcję, ale na wszelki wypadek nie wyłączamy capture
                continue
            m = row_re.match(line)
            if not m:
                # linie typu nagłówków "Language Name  Formats" pomijamy
                continue
            lang, formats_str = m.groups()
            formats = [f.strip() for f in formats_str.split(",") if f.strip()]
            if formats:
                subs.setdefault(lang, [])
                # scal z unikaniem duplikatów
                for f in formats:
                    if f not in subs[lang]:
                        subs[lang].append(f)
    return subs

def choose_lang_and_format(
    subs: Dict[str, List[str]],
    preferred_langs: Optional[List[str]],
    preferred_formats: Optional[List[str]]
) -> Optional[Tuple[str, str]]:
    """
    Zwraca (lang, fmt) wg priorytetów:
    1) języki: preferred_langs -> '*-orig' -> 'en-orig' -> 'en' -> dowolny dostępny
    2) formaty: preferred_formats -> 'vtt' -> pierwszy dostępny
    """
    if not subs:
        return None

    # --- 1) Kolejność języków ---
    lang_order: List[str] = []

    # preferencje użytkownika
    if preferred_langs:
        for l in preferred_langs:
            if l and l not in lang_order:
                lang_order.append(l)

    # zawsze preferuj oryginalne napisy, jeśli są
    for l in subs.keys():
        if l.endswith("-orig") and l not in lang_order:
            lang_order.append(l)

    # fallback: angielski oryginalny -> angielski
    for l in ["en-orig", "en"]:
        if l not in lang_order:
            lang_order.append(l)

    # dowolny dostępny
    for l in subs.keys():
        if l not in lang_order:
            lang_order.append(l)

    # --- 2) Kolejność formatów ---
    fmt_order: List[str] = []
    if preferred_formats:
        for f in preferred_formats:
            if f and f not in fmt_order:
                fmt_order.append(f)

    if "vtt" not in fmt_order:
        fmt_order.append("vtt")  # domyślny yt-dlp

    # --- 3) Wybór najlepszego języka i formatu ---
    for lang in lang_order:
        if lang not in subs:
            continue
        avail = subs[lang]  # np. ["vtt", "srv3", "json3"]
        for f in fmt_order:
            if f in avail:
                return (lang, f)
        if avail:
            return (lang, avail[0])

    return None

    
def sub_format_chain(target_fmt: str) -> str:
    """
    Buduje łańcuch priorytetów dla --sub-format.
    Najpierw target, potem sensowne fallbacki.
    Uwaga: yt-dlp używa separatora '/'.
    """
    chain = [target_fmt]
    for f in ["json3", "srv3", "vtt", "ttml", "srt", "best"]:
        if f not in chain:
            chain.append(f)
    return "/".join(chain)

# ---- public API ------------------------------------------------------------

async def download_captions(
    job_id: str,
    url: str,
    languages: Optional[List[str]],
    preferred_sub_formats: Optional[List[str]]
) -> Dict[str, Any]:
    """
    Nowa logika:
    1) list-subs -> parsuj języki i formaty
    2) wybierz najlepsze dopasowanie (język, format)
    3) pobierz jednoznacznie (write-auto-subs + write-subs), --sub-langs=<lang>, --sub-format=<chain>
    4) zwróć ścieżkę do pobranych plików (zawiera lang i ext)
    """
    vid = get_video_id(url) or "unknown"
    outdir = DATA_DIR / job_id
    outdir.mkdir(parents=True, exist_ok=True)

    # 1) list-subs
    list_cmd = f'yt-dlp --list-subs {shlex.quote(url)}'
    await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"cmd": list_cmd}})
    listing = run(list_cmd, capture=True)
    await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"list_subs": listing}})

    subs_map = parse_list_subs(listing)
    # metadata o video
    video_info = get_video_info(url)

    # 2) wybór języka i formatu wg preferencji
    choice = choose_lang_and_format(subs_map, languages, preferred_sub_formats)
    if not choice:
        # brak jakichkolwiek napisów → caller (main) zrobi fallback do Whisper
        return {"video_id": vid, "dir": str(outdir), "captions": []}

    target_lang, target_fmt = choice
    chain = sub_format_chain(target_fmt)

    # 3) pobranie z wybranym lang i formatem (auto+author włączone)
    flags = "--write-auto-subs --write-subs"
    # Captions: stop using video_id in filenames; keep only language + ext
    dl_cmd = (
        f'yt-dlp {shlex.quote(url)} '
        f'--skip-download {flags} '
        f'--sub-langs {shlex.quote(target_lang)} '
        f'--sub-format {shlex.quote(chain)} '
        f'-o "{outdir}/%(language)s.%(ext)s"'
    )
    await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"cmd": dl_cmd}})
    out = run(dl_cmd, capture=True)
    await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"yt_dlp": out}})

    # 4) zebrane pliki
    files = [str(p) for p in outdir.glob(f"{target_lang}.*")]
    caps = [f for f in files if any(f.endswith(ext) for ext in (".vtt",".ttml",".srv3",".srv2",".srv1",".json3",".srt",".ass"))]

    # jeśli z jakiegoś powodu nic nie wpadło (throttling/nsig)
    dl_cmd2 = None
    if not caps:
        # ostatnia próba: bez ograniczania do konkretnego formatu (pozwól YT wybrać najlepszy)
        dl_cmd2 = (
            f'yt-dlp {shlex.quote(url)} '
            f'--skip-download {flags} '
            f'--sub-langs {shlex.quote(target_lang)} '
            f'-o "{outdir}/%(language)s.%(ext)s"'
        )
        await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"cmd": dl_cmd2}})
        out2 = run(dl_cmd2, capture=True)
        await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"yt_dlp": out2}})
        all_caps_exts = (".vtt", ".ttml", ".srv3", ".srv2", ".srv1", ".json3", ".srt", ".ass")
        files = [str(p) for p in outdir.glob(f"*.*")]
        caps = [f for f in files if f.endswith(all_caps_exts)]  

    # raportuj artefakty
    for f in caps:
        store.add_artifact(job_id, "captions", f)
        await bus.publish(job_id, {"type": "artifact", "job_id": job_id, "data": {"captions": f}})

    return {
        "video_id": vid,
        "dir": str(outdir),
        "job_id": job_id,
        "captions": caps,
        "subs_map": subs_map,
        "selected": {"lang": target_lang, "format": target_fmt, "chain": chain},
        "commands": {"list": list_cmd, "download": dl_cmd, "download_fallback": dl_cmd2},
        "video_info": video_info,
        "source_url": url
    }

async def download_wav(job_id: str, url: str) -> Dict[str, Any]:
    vid = get_video_id(url) or "unknown"
    outdir = DATA_DIR / job_id
    outdir.mkdir(parents=True, exist_ok=True)
    # Audio: store as raw_video.wav
    wav_path = outdir / f"raw_video.wav"
    cmd = (
      f'yt-dlp {shlex.quote(url)} -x --audio-format wav '
      f'-o "{outdir}/raw_video.%(ext)s"'
    )
    await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"cmd": cmd}})
    t0 = time.time()
    out = run(cmd, capture=True)
    t1 = time.time()
    await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"yt_dlp": out}})
    # Zidentyfikuj ewentualny oryginalny plik pobrany przez yt-dlp (webm/m4a)
    original_media: Optional[str] = None
    for ext in (".webm", ".m4a", ".mp4", ".mkv"):
        cand = outdir / f"raw_video{ext}"
        if cand.exists():
            original_media = str(cand)
            break
    return {
        "video_id": vid,
        "wav_path": str(wav_path),
        "dir": str(outdir),
        "job_id": job_id,
        "commands": {"download_audio": cmd},
        "download_seconds": t1 - t0,
        "original_media": original_media,
        "source_url": url
    }