"""
database.py — Koneksi PostgreSQL (asyncpg) dan operasi database.

Isolation Level: READ COMMITTED (default PostgreSQL).
─────────────────────────────────────────────────────────
Mengapa READ COMMITTED, bukan SERIALIZABLE?
1. Setiap operasi INSERT bersifat atomik — satu statement = satu transaksi.
2. UNIQUE constraint pada (topic, event_id) di-enforce oleh PostgreSQL
   menggunakan exclusive lock pada index entry, BUKAN table-level lock.
3. Phantom read tidak menjadi masalah karena kita tidak melakukan range query
   di dalam transaksi tulis. Operasi dedup adalah point lookup pada constraint.
4. READ COMMITTED memberikan throughput lebih tinggi karena reader tidak
   memblokir writer dan sebaliknya.

Tabel:
  - events: menyimpan event dengan UNIQUE(topic, event_id) untuk dedup
  - stats: singleton row (id=1) untuk tracking counter secara atomik
"""

import asyncpg
import json
import logging
import os
import asyncio
from datetime import datetime, timezone

logger = logging.getLogger("aggregator.database")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://aggregator:aggregator_pass@storage:5432/logdb",
)

# ── DDL: Tabel events dengan UNIQUE constraint untuk dedup ──────
CREATE_EVENTS_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id          SERIAL PRIMARY KEY,
    topic       TEXT          NOT NULL,
    event_id    TEXT          NOT NULL,
    timestamp   TIMESTAMPTZ   NOT NULL,
    source      TEXT          NOT NULL,
    payload     JSONB         NOT NULL DEFAULT '{}',
    received_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    -- Dedup: kombinasi topic + event_id harus unik
    CONSTRAINT uq_topic_event_id UNIQUE (topic, event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_topic ON events (topic);
"""

# ── DDL: Tabel stats — singleton row untuk counter ──────────────
CREATE_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS stats (
    id                  INT PRIMARY KEY DEFAULT 1,
    received            BIGINT      NOT NULL DEFAULT 0,
    unique_processed    BIGINT      NOT NULL DEFAULT 0,
    duplicate_dropped   BIGINT      NOT NULL DEFAULT 0,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Pastikan satu baris singleton selalu ada
INSERT INTO stats (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
"""


async def init_db(max_retries: int = 15, retry_delay: float = 2.0) -> asyncpg.Pool:
    """
    Buat connection pool dan inisialisasi tabel.
    Retry otomatis saat PostgreSQL belum siap (cold start Docker Compose).
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                "Koneksi ke PostgreSQL (percobaan %d/%d)...", attempt, max_retries
            )
            pool = await asyncpg.create_pool(
                DATABASE_URL, min_size=2, max_size=10, command_timeout=30
            )

            # Buat tabel jika belum ada
            async with pool.acquire() as conn:
                await conn.execute(CREATE_EVENTS_TABLE)
                await conn.execute(CREATE_STATS_TABLE)

            logger.info("PostgreSQL siap — tabel events & stats terinisialisasi.")
            return pool

        except (
            asyncpg.CannotConnectNowError,
            asyncpg.ConnectionDoesNotExistError,
            ConnectionRefusedError,
            OSError,
        ) as e:
            logger.warning("PostgreSQL belum siap: %s. Retry %ds...", e, retry_delay)
            if attempt == max_retries:
                raise RuntimeError(
                    f"Gagal konek PostgreSQL setelah {max_retries} percobaan"
                ) from e
            await asyncio.sleep(retry_delay)

    raise RuntimeError("Loop retry selesai tanpa koneksi")


async def insert_event(pool, event: dict) -> bool:
    """
    Simpan event ke database dengan dedup + update stats secara transaksional.

    Alur dalam SATU transaksi:
      1. INSERT INTO events ... ON CONFLICT (topic, event_id) DO NOTHING
      2. Jika inserted (rowcount=1):
         UPDATE stats SET received+1, unique_processed+1
      3. Jika conflict (rowcount=0):
         UPDATE stats SET received+1, duplicate_dropped+1

    Return True jika event unik (baru), False jika duplikat.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Konversi timestamp string → datetime object (asyncpg butuh native datetime)
            ts = event["timestamp"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))

            # Step 1: INSERT dengan dedup — ON CONFLICT DO NOTHING
            status = await conn.execute(
                """
                INSERT INTO events (topic, event_id, timestamp, source, payload)
                VALUES ($1, $2, $3, $4, $5::jsonb)
                ON CONFLICT (topic, event_id) DO NOTHING
                """,
                event["topic"],
                event["event_id"],
                ts,
                event["source"],
                json.dumps(event["payload"]) if isinstance(event["payload"], dict) else event["payload"],
            )

            # asyncpg.execute() return status string: "INSERT 0 1" atau "INSERT 0 0"
            inserted = status == "INSERT 0 1"

            if inserted:
                # Step 2a: Event baru — increment received + unique_processed
                await conn.execute(
                    """
                    UPDATE stats
                    SET received = received + 1,
                        unique_processed = unique_processed + 1
                    WHERE id = 1
                    """
                )
            else:
                # Step 2b: Duplikat — increment received + duplicate_dropped
                await conn.execute(
                    """
                    UPDATE stats
                    SET received = received + 1,
                        duplicate_dropped = duplicate_dropped + 1
                    WHERE id = 1
                    """
                )

            return inserted


async def get_events(pool, topic: str = None) -> list:
    """
    Ambil event dari database, opsional filter by topic.
    Return list of dict.
    """
    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                """
                SELECT topic, event_id, timestamp, source, payload
                FROM events
                WHERE topic = $1
                ORDER BY received_at DESC
                """,
                topic,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT topic, event_id, timestamp, source, payload
                FROM events
                ORDER BY received_at DESC
                """
            )

    results = []
    for r in rows:
        results.append({
            "topic": r["topic"],
            "event_id": r["event_id"],
            "timestamp": r["timestamp"].isoformat() if hasattr(r["timestamp"], "isoformat") else str(r["timestamp"]),
            "source": r["source"],
            "payload": r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"]),
        })
    return results


async def get_stats(pool) -> dict:
    """
    Ambil counter stats + daftar topic unik dari database.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM stats WHERE id = 1")
        topic_rows = await conn.fetch(
            "SELECT DISTINCT topic FROM events ORDER BY topic"
        )

    if row is None:
        return {
            "received": 0,
            "unique_processed": 0,
            "duplicate_dropped": 0,
            "started_at": datetime.now(timezone.utc),
            "topics": [],
        }

    return {
        "received": row["received"],
        "unique_processed": row["unique_processed"],
        "duplicate_dropped": row["duplicate_dropped"],
        "started_at": row["started_at"],
        "topics": [r["topic"] for r in topic_rows],
    }
