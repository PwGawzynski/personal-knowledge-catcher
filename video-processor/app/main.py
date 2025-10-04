import os, pathlib, asyncio, json, time, shutil
from .config import (
    REDIS_URL,
    REDIS_PASSWORD,
    REDIS_INGEST_LIST,
    REDIS_BRPOP_TIMEOUT,
    DATA_DIR,
    MAX_TEXT_LENGTH,
)
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, List
from .schemas import ExtractRequest, ExtractResponse, JobStatus
from .jobs import bus, store
from .yt import download_captions, download_wav, get_video_id
from .whisperer import transcribe_chunks
from .subs_formats import list_by_ext
from .ingester import ingest_from_captions, ingest_from_whisper
from contextlib import asynccontextmanager


"""Configuration is loaded from .config (validated)."""


async def _consume_ingest_queue():
    try:
        import redis.asyncio as aioredis  # type: ignore
    except Exception:
        print("[av-worker] redis.asyncio not available; skipping ingest consumer")
        return

    try:
        client = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True, password=REDIS_PASSWORD or None)
        await client.ping()
    except Exception as e:
        print(f"[av-worker] Failed to connect to Redis at {REDIS_URL}: {e}")
        return

    print(f"[av-worker] Ingest consumer started: list={REDIS_INGEST_LIST}")
    try:
        while True:
            try:
                item = await client.brpop(REDIS_INGEST_LIST, timeout=REDIS_BRPOP_TIMEOUT)
                if not item:
                    await asyncio.sleep(0)
                    continue
                _key, payload = item
                print(f"[av-worker] item: {item}")
                try:
                    obj = json.loads(payload)
                except Exception:
                    print(f"[av-worker] invalid JSON on {REDIS_INGEST_LIST}: {payload[:200]}")
                    continue

                # Validate as ExtractRequest-like payload
                try:
                    req = ExtractRequest(**obj)
                except Exception as e:
                    print(f"[av-worker] invalid ExtractRequest payload: {e}; obj={obj}")
                    continue

                job_id = store.new_job()
                asyncio.create_task(_run_job(job_id, req))
            except Exception as e:
                print(f"[av-worker] consumer error: {e}")
                await asyncio.sleep(0.1)
    finally:
        try:
            await client.close()
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_consume_ingest_queue())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except Exception:
            pass

app = FastAPI(title="AV Worker (YouTube/TikTok)", lifespan=lifespan)



@app.post("/extract", response_model=ExtractResponse)
async def extract(req: ExtractRequest):
    """
    Priorytet:
      1) spróbuj pobrać napisy z YouTube (auto+author) w oryginalnych formatach (vtt,ttml,srv*,json3)
      2) jeżeli brak — pobierz audio i transkrybuj (Whisper, chunkowanie, N workerów)
    """
    job_id = store.new_job()
    asyncio.create_task(_run_job(job_id, req))
    return ExtractResponse(job_id=job_id)

async def _run_job(job_id: str, req: ExtractRequest):
    url = str(req.url)
    store.set_state(job_id, "running", "starting")
    await bus.publish(job_id, {"type": "state", "job_id": job_id, "data": {"state": "running", "stage": "yt-captions"}})

    try:
        t_job0 = time.time()
        captions_stage = {"start": time.time()}
        # 1) spróbuj napisy (YT)
        cap_info = await download_captions(job_id, url, req.languages, req.preferred_sub_formats)
        captions_stage["end"] = time.time()
        captions_stage["seconds"] = captions_stage["end"] - captions_stage["start"]
        vid = cap_info["video_id"]
        captions = cap_info["captions"]
        job_dir = pathlib.Path(cap_info["dir"]) if cap_info.get("dir") else (DATA_DIR / job_id)
        job_dir.mkdir(parents=True, exist_ok=True)

        # meta.json
        meta = {
            "job_id": job_id,
            "video_id": vid,
            "source_url": cap_info.get("source_url") or url,
            "subs_map": cap_info.get("subs_map"),
            "selected": cap_info.get("selected"),
            "commands": cap_info.get("commands"),
            "video_info": cap_info.get("video_info"),
        }
        with open(job_dir / "meta.json", "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        if captions:
            # enqueue chunks from captions and record text processing metadata
            text_proc = None
            try:
                selected_lang = (cap_info.get("selected") or {}).get("lang")
                video_title = None
                vi = cap_info.get("video_info") or {}
                if isinstance(vi, dict):
                    video_title = vi.get("title")
                ing = ingest_from_captions(
                    job_dir=str(job_dir),
                    captions=captions,
                    selected_lang=selected_lang,
                    source_url=cap_info.get("source_url") or url,
                    video_id=vid,
                    video_title=video_title,
                )
                text_proc = {
                    "method": "captions",
                    "selected_lang": selected_lang,
                    "captions_file": ing.get("file"),
                    "chunks": ing.get("chunks"),
                    "enqueued": ing.get("enqueued"),
                    "queue": REDIS_INGEST_LIST,
                    "max_text_length": MAX_TEXT_LENGTH,
                    "errors": [],
                }
                await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"stage": "ingest", "captions": ing}})
            except Exception as e:
                text_proc = {
                    "method": "captions",
                    "errors": [str(e)],
                    "queue": REDIS_INGEST_LIST,
                    "max_text_length": MAX_TEXT_LENGTH,
                }
                await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"stage": "ingest", "error": str(e)}})

            # processing.meta.json
            proc = {
                "job_id": job_id,
                "video_id": vid,
                "source_url": url,
                "stages": {
                    "captions": {"success": True, "seconds": captions_stage["seconds"], "selected": cap_info.get("selected")},
                    "whisper": None
                },
                "text_processing": text_proc,
                "success": True,
                "total_seconds": time.time() - t_job0
            }
            with open(job_dir / "processing.meta.json", "w", encoding="utf-8") as f:
                json.dump(proc, f, ensure_ascii=False, indent=2)

            store.set_state(job_id, "done", "captions-downloaded")
            await bus.publish(job_id, {"type": "state", "job_id": job_id, "data": {"state": "done", "captions": captions}})
            return

        # 2) brak napisów -> transkrypcja
        await bus.publish(job_id, {"type": "state", "job_id": job_id, "data": {"stage": "whisper-fallback"}})
        whisper_stage = {"start": time.time()}
        wav_info = await download_wav(job_id, url)
        artifacts = await transcribe_chunks(job_id, wav_info["wav_path"], wav_info["video_id"])
        whisper_stage["end"] = time.time()
        whisper_stage["seconds"] = whisper_stage["end"] - whisper_stage["start"]
        store.add_artifact(job_id, "whisper", artifacts)
        store.set_state(job_id, "done", "whisper-finished")
        await bus.publish(job_id, {"type": "state", "job_id": job_id, "data": {"state": "done", "whisper": artifacts}})

        # Enqueue chunks from Whisper SRT and record text processing metadata
        text_proc = None
        try:
            video_title = None
            vi = cap_info.get("video_info") if 'cap_info' in locals() else None
            if isinstance(vi, dict):
                video_title = vi.get("title")
            whisper_lang = artifacts.get("lang")
            ing = ingest_from_whisper(
                job_dir=str(job_dir),
                srt_path=artifacts.get("srt") or "",
                source_url=wav_info.get("source_url") or url,
                video_id=wav_info.get("video_id", vid),
                video_title=video_title,
                lang=whisper_lang,
            )
            text_proc = {
                "method": "whisper",
                "lang": whisper_lang,
                "srt_file": ing.get("file"),
                "chunks": ing.get("chunks"),
                "enqueued": ing.get("enqueued"),
                "queue": REDIS_INGEST_LIST,
                "max_text_length": MAX_TEXT_LENGTH,
                "errors": [],
            }
            await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"stage": "ingest", "whisper": ing}})
        except Exception as e:
            text_proc = {
                "method": "whisper",
                "errors": [str(e)],
                "queue": REDIS_INGEST_LIST,
                "max_text_length": MAX_TEXT_LENGTH,
            }
            await bus.publish(job_id, {"type": "log", "job_id": job_id, "data": {"stage": "ingest", "error": str(e)}})

        # processing.meta.json (whisper)
        proc = {
            "job_id": job_id,
            "video_id": wav_info.get("video_id", vid),
            "source_url": url,
            "stages": {
                "captions": {"success": False, "seconds": captions_stage["seconds"]},
                "whisper": {
                    "success": True,
                    "seconds": whisper_stage["seconds"],
                    "stats": artifacts.get("stats"),
                    "lang": artifacts.get("lang"),
                }
            },
            "text_processing": text_proc,
            "success": True,
            "total_seconds": time.time() - t_job0
        }
        with open(job_dir / "processing.meta.json", "w", encoding="utf-8") as f:
            json.dump(proc, f, ensure_ascii=False, indent=2)

        # Cleanup: original media, wav, chunks dir
        try:
            if wav_info.get("original_media") and os.path.exists(wav_info["original_media"]):
                os.remove(wav_info["original_media"])
        except Exception:
            pass
        try:
            if artifacts.get("wav_path") and os.path.exists(artifacts["wav_path"]):
                os.remove(artifacts["wav_path"])
        except Exception:
            pass
        try:
            if artifacts.get("chunk_dir") and os.path.isdir(artifacts["chunk_dir"]):
                shutil.rmtree(artifacts["chunk_dir"], ignore_errors=True)
        except Exception:
            pass

    except Exception as e:
        store.set_error(job_id, str(e))
        await bus.publish(job_id, {"type": "state", "job_id": job_id, "data": {"state": "error", "error": str(e)}})
        # attempt to write processing.meta.json with error
        try:
            job_dir = DATA_DIR / job_id
            job_dir.mkdir(parents=True, exist_ok=True)
            err_meta = {
                "job_id": job_id,
                "source_url": url,
                "success": False,
                "error": str(e)
            }
            with open(job_dir / "processing.meta.json", "w", encoding="utf-8") as f:
                json.dump(err_meta, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

@app.websocket("/monitor")
async def monitor(ws: WebSocket):
    """
    WebSocket: klient łączy się z parametrem ?job_id=...
    Strumień Event (JSON): {type, job_id, data:{...}}
    """
    await ws.accept()
    try:
        query = ws.query_params
        job_id = query.get("job_id")
        if not job_id:
            await ws.send_json({"error": "missing job_id"})
            await ws.close()
            return

        q = bus.subscribe(job_id)
        # wyślij startowy status
        cur = store.get(job_id)
        if cur:
            await ws.send_json({"type":"state","job_id":job_id,"data":cur})

        while True:
            event = await q.get()
            await ws.send_json(event)

    except WebSocketDisconnect:
        pass
    finally:
        if 'q' in locals():
            bus.unsubscribe(job_id, q)

@app.get("/status/{job_id}", response_model=JobStatus)
async def status(job_id: str):
    cur = store.get(job_id)
    if not cur:
        raise HTTPException(status_code=404, detail="unknown job")
    return JobStatus(
        job_id=job_id,
        state=cur["state"],
        detail=cur.get("detail"),
        artifacts=cur.get("artifacts", {})
    )

@app.get("/ping")
async def ping():
    return {"status": "ok"}