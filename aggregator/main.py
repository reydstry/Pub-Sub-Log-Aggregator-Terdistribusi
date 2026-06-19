"""
main.py — FastAPI application untuk Pub-Sub Log Aggregator.

Endpoint:
  POST /publish  → Terima Event / List[Event], RPUSH ke Redis, return {"queued": N}
  GET  /events   → Daftar event dari database (filter topic opsional)
  GET  /stats    → Counter received/unique/duplicate + uptime + topics
  GET  /health   → Health check

Lifecycle:
  startup  → init_db() + koneksi Redis + start_workers(3)
  shutdown → cancel workers + tutup Redis & DB pool

Middleware:
  Log setiap request: method, path, status code, durasi (ms)
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Union

import redis.asyncio as aioredis
from fastapi import FastAPI, Body, Query, Request
from fastapi.responses import JSONResponse

from models import EventSchema, PublishResponse, StatsResponse
from database import init_db, get_events, get_stats
from consumer import start_workers

# ── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("aggregator.main")

# ── Konfigurasi ─────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
QUEUE_KEY = os.getenv("QUEUE_KEY", "event_queue")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", "3"))


# ── Lifespan: startup & shutdown ────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info("  Pub-Sub Log Aggregator — Starting up")
    logger.info("=" * 60)

    # 1. Inisialisasi PostgreSQL (retry otomatis)
    app.state.pool = await init_db()

    # 2. Koneksi Redis untuk push queue
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    await app.state.redis.ping()
    logger.info("Redis koneksi siap.")

    # 3. Spawn consumer workers
    await start_workers(app, n_workers=NUM_WORKERS)

    yield  # ── Aplikasi berjalan ──

    # ── Shutdown ──
    logger.info("Shutting down...")

    # Cancel semua consumer worker tasks
    for task in getattr(app.state, "worker_tasks", []):
        task.cancel()
    await asyncio.gather(
        *getattr(app.state, "worker_tasks", []), return_exceptions=True
    )

    # Tutup koneksi Redis
    if hasattr(app.state, "redis") and app.state.redis:
        await app.state.redis.close()

    # Tutup pool PostgreSQL
    if hasattr(app.state, "pool") and app.state.pool:
        await app.state.pool.close()

    logger.info("Shutdown selesai.")


# ── FastAPI App ─────────────────────────────────────────────────
app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description=(
        "Sistem agregasi log terdistribusi dengan deduplication persisten, "
        "Redis sebagai message broker, PostgreSQL sebagai storage."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ── Middleware: log setiap request (method, path, status, durasi) ──
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "%s %s → %d (%.1f ms)",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


# ================================================================
# POST /publish — Terima Event atau List[Event], push ke Redis
# ================================================================
@app.post("/publish")
async def publish(
    body: Union[EventSchema, List[EventSchema]] = Body(...),
):
    """
    Terima single event (JSON object) atau batch events (JSON array).
    Setiap event di-push ke Redis queue via RPUSH untuk diproses consumer.
    """
    # Normalisasi ke list
    if isinstance(body, list):
        events = body
    else:
        events = [body]

    # RPUSH setiap event ke Redis queue
    redis_client = app.state.redis
    for ev in events:
        await redis_client.rpush(QUEUE_KEY, ev.model_dump_json())

    logger.info("Diterima %d event, di-push ke Redis queue '%s'.", len(events), QUEUE_KEY)
    return {"queued": len(events)}


# ================================================================
# GET /events — Query event dari database
# ================================================================
@app.get("/events")
async def list_events(
    topic: str = Query(default=None, description="Filter berdasarkan topic"),
):
    """Ambil daftar event unik dari PostgreSQL, opsional filter by topic."""
    events = await get_events(app.state.pool, topic=topic)
    return {"count": len(events), "events": events}


# ================================================================
# GET /stats — Statistik aggregator
# ================================================================
@app.get("/stats")
async def statistics():
    """
    Statistik sistem:
      - received:          total event diterima consumer
      - unique_processed:  event unik tersimpan di DB
      - duplicate_dropped: event duplikat yang dilewati
      - topics:            daftar topic unik
      - uptime_seconds:    waktu sejak started_at di tabel stats
    """
    data = await get_stats(app.state.pool)

    # Hitung uptime dari started_at di tabel stats
    started_at = data.get("started_at")
    if started_at:
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        uptime = (datetime.now(timezone.utc) - started_at).total_seconds()
    else:
        uptime = 0.0

    return {
        "received": data["received"],
        "unique_processed": data["unique_processed"],
        "duplicate_dropped": data["duplicate_dropped"],
        "topics": data.get("topics", []),
        "uptime_seconds": round(uptime, 2),
    }


# ================================================================
# GET /health — Health check
# ================================================================
@app.get("/health")
async def health_check():
    """Health check sederhana untuk Docker dan monitoring."""
    return {"status": "healthy"}
