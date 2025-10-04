from typing import List
import pathlib, shutil

def list_by_ext(dir_path: str, vid: str, exts: List[str]) -> List[str]:
    # Naming no longer uses video_id in filenames. Match by extension only.
    p = pathlib.Path(dir_path)
    found: List[str] = []
    for ext in exts:
        found.extend([str(x) for x in p.glob(f"*.{ext}")])
    return sorted(found)

# Na razie: dla Whisper mamy VTT/SRT (względnie prosto).
# Dla YT: zwracamy pliki w oryginalnych formatach (vtt/ttml/srv*/json3) pobrane przez yt-dlp.