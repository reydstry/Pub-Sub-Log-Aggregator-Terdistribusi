"""
consumer.py — Asyncio consumer workers untuk memproses event dari Redis.

Setiap worker:
  1. BLPOP dari Redis key "event_queue" (blocking pop, timeout 1 detik)
  2. Parse JSON → validasi via Pydantic EventSchema
  3. Panggil insert_event() untuk dedup + simpan ke PostgreSQL
  4. Log hasil: [NEW] atau [DEDUP]

Graceful shutdown: worker menangkap asyncio.CancelledError dan berhenti.
"""

import asyncio
import json
import logging

from models import EventSchema
from database import insert_event

logger = logging.getLogger("aggregator.consumer")


async def _worker(worker_id: int, redis_client, pool):
    """
    Loop utama satu consumer worker.
    Membaca dari Redis queue, validasi, lalu simpan ke database.
    """
    logger.info("Worker-%d dimulai, mendengarkan 'event_queue'...", worker_id)

    while True:
        try:
            # BLPOP: blocking pop dari kiri queue, timeout 1 detik
            # Jika queue kosong, worker tidur (hemat CPU)
            result = await redis_client.blpop("event_queue", timeout=1)

            if result is None:
                # Timeout — queue kosong, lanjut loop
                continue

            # result = (key, value) → ambil value saja
            _, raw_data = result
            event_dict = json.loads(raw_data)

            # Validasi schema via Pydantic — reject data yang tidak valid
            event = EventSchema(**event_dict)
            event_data = event.model_dump()

            # Simpan ke database (dedup terjadi di sini)
            is_new = await insert_event(pool, event_data)

            if is_new:
                logger.info(
                    "[NEW] event_id=%s topic=%s diproses oleh Worker-%d",
                    event.event_id, event.topic, worker_id,
                )
            else:
                logger.info(
                    "[DEDUP] event_id=%s topic=%s sudah diproses — Worker-%d",
                    event.event_id, event.topic, worker_id,
                )

        except asyncio.CancelledError:
            # Graceful shutdown — keluar dari loop
            logger.info("Worker-%d dihentikan (shutdown).", worker_id)
            break

        except json.JSONDecodeError as e:
            logger.error("Worker-%d: JSON tidak valid: %s", worker_id, e)

        except Exception as e:
            logger.error("Worker-%d: error — %s: %s", worker_id, type(e).__name__, e)
            # Tunggu sebentar agar tidak spin-loop saat error berulang
            await asyncio.sleep(0.5)


async def start_workers(app, n_workers: int = 3):
    """
    Spawn N asyncio tasks sebagai consumer workers.
    Tasks disimpan di app.state agar bisa di-cancel saat shutdown.
    """
    redis_client = app.state.redis
    pool = app.state.pool

    tasks = []
    for i in range(n_workers):
        task = asyncio.create_task(
            _worker(i, redis_client, pool),
            name=f"consumer-worker-{i}",
        )
        tasks.append(task)

    app.state.worker_tasks = tasks
    logger.info("%d consumer worker berhasil di-spawn.", n_workers)
