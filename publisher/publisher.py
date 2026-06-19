"""
publisher.py — Simulator event publisher dengan duplikasi intentional.

Fitur:
  - Generate N event (default 25.000), ~35% duplikat
  - Kirim via httpx.AsyncClient dalam batch (default 50 per request)
  - Concurrent requests via asyncio.Semaphore (default max 5)
  - Retry dengan exponential backoff (max 3 retry per batch)
  - CLI arguments: --count, --dup-rate, --batch, --concurrency

Distribusi topic:
  user.login, order.created, payment.processed, error.critical, audit.access
"""

import asyncio
import argparse
import random
import sys
import time
import uuid
import os
from datetime import datetime, timezone

import httpx

# ── Konfigurasi ─────────────────────────────────────────────────
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8080")
PUBLISH_URL = f"{AGGREGATOR_URL}/publish"

TOPICS = [
    "user.login",
    "order.created",
    "payment.processed",
    "error.critical",
    "audit.access",
]

# Payload template per topic — minimal 2 field per topic
PAYLOAD_TEMPLATES = {
    "user.login": lambda: {
        "username": f"user_{random.randint(1, 500)}",
        "ip_address": f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
        "success": random.choice([True, True, True, False]),
    },
    "order.created": lambda: {
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "total_amount": round(random.uniform(10.0, 5000.0), 2),
        "item_count": random.randint(1, 20),
    },
    "payment.processed": lambda: {
        "transaction_id": f"TXN-{random.randint(10000, 99999)}",
        "amount": round(random.uniform(5.0, 3000.0), 2),
        "method": random.choice(["credit_card", "debit", "e-wallet", "bank_transfer"]),
    },
    "error.critical": lambda: {
        "error_code": random.choice(["E500", "E502", "E503", "E429", "E408"]),
        "service": random.choice(["auth-svc", "payment-svc", "order-svc", "gateway"]),
        "stack_trace": f"at module.handler:L{random.randint(10, 500)}",
    },
    "audit.access": lambda: {
        "resource": random.choice(["/api/users", "/api/orders", "/admin/config", "/api/reports"]),
        "action": random.choice(["READ", "WRITE", "DELETE", "UPDATE"]),
        "actor": f"admin_{random.randint(1, 50)}",
    },
}


def generate_event(topic: str = None) -> dict:
    """Buat satu event JSON dengan payload sesuai topic."""
    t = topic or random.choice(TOPICS)
    payload_fn = PAYLOAD_TEMPLATES.get(t, lambda: {"data": "generic"})
    return {
        "topic": t,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": f"publisher-sim",
        "payload": payload_fn(),
    }


def generate_events(count: int, dup_rate: float) -> tuple[list[dict], int]:
    """
    Generate daftar event dengan persentase duplikat.
    Return (events_list, jumlah_duplikat_yang_disisipkan).
    """
    events = []
    unique_pool = []  # Pool event unik untuk dipilih sebagai duplikat
    dup_count = 0

    for i in range(count):
        # Jika sudah ada event unik dan random < dup_rate → buat duplikat
        if unique_pool and random.random() < dup_rate:
            # Pilih event acak dari pool untuk diduplikasi
            original = random.choice(unique_pool)
            dup_event = {
                "topic": original["topic"],
                "event_id": original["event_id"],  # ID sama = duplikat
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": original["source"],
                "payload": original["payload"],
            }
            events.append(dup_event)
            dup_count += 1
        else:
            # Buat event unik baru
            ev = generate_event()
            events.append(ev)
            unique_pool.append(ev)

    return events, dup_count


async def send_batch_with_retry(
    client: httpx.AsyncClient,
    batch: list[dict],
    max_retries: int = 3,
) -> dict:
    """
    Kirim batch event ke aggregator dengan retry + exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            resp = await client.post(PUBLISH_URL, json=batch, timeout=15.0)
            resp.raise_for_status()
            return resp.json()
        except (httpx.HTTPStatusError, httpx.ConnectError, httpx.TimeoutException) as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s — exponential backoff
                print(
                    f"  [RETRY] Batch gagal (percobaan {attempt+1}): {e}. "
                    f"Retry dalam {wait}s..."
                )
                await asyncio.sleep(wait)
            else:
                print(f"  [ERROR] Batch gagal setelah {max_retries} percobaan: {e}")
                raise


async def wait_for_aggregator(max_retries: int = 30, delay: float = 2.0):
    """Tunggu aggregator service siap sebelum mulai mengirim."""
    print(f"[PUBLISHER] Menunggu aggregator di {AGGREGATOR_URL}...")
    async with httpx.AsyncClient() as client:
        for attempt in range(1, max_retries + 1):
            try:
                resp = await client.get(f"{AGGREGATOR_URL}/health", timeout=5.0)
                if resp.status_code == 200:
                    print(f"[PUBLISHER] Aggregator siap! (percobaan {attempt})")
                    return
            except (httpx.ConnectError, httpx.TimeoutException):
                pass
            print(f"[PUBLISHER] Percobaan {attempt}/{max_retries}...")
            await asyncio.sleep(delay)

    print("[PUBLISHER] GAGAL: aggregator tidak merespon.")
    sys.exit(1)


async def run(args):
    """Eksekusi utama publisher."""
    print("\n" + "=" * 64)
    print("  PUB-SUB LOG AGGREGATOR — Publisher Simulator")
    print("=" * 64)
    print(f"  Event count  : {args.count}")
    print(f"  Dup rate     : {args.dup_rate:.0%}")
    print(f"  Batch size   : {args.batch}")
    print(f"  Concurrency  : {args.concurrency}")
    print("=" * 64)

    # Tunggu aggregator siap
    await wait_for_aggregator()
    await asyncio.sleep(2)  # Buffer agar consumer workers aktif

    # Generate semua event
    print(f"\n[GENERATE] Membuat {args.count} event ({args.dup_rate:.0%} duplikat)...")
    events, dup_count = generate_events(args.count, args.dup_rate)
    unique_count = args.count - dup_count
    print(f"  → {unique_count} unik + {dup_count} duplikat = {len(events)} total")

    # Pecah jadi batch
    batches = [events[i : i + args.batch] for i in range(0, len(events), args.batch)]
    print(f"  → {len(batches)} batch (@ max {args.batch} event)")

    # Kirim semua batch secara concurrent
    sem = asyncio.Semaphore(args.concurrency)
    sent_ok = 0
    sent_fail = 0

    async def send_with_semaphore(client, batch):
        nonlocal sent_ok, sent_fail
        async with sem:
            try:
                result = await send_batch_with_retry(client, batch)
                sent_ok += result.get("queued", len(batch))
            except Exception:
                sent_fail += len(batch)

    print(f"\n[SEND] Mengirim {len(batches)} batch (max {args.concurrency} concurrent)...")
    start_time = time.perf_counter()

    async with httpx.AsyncClient() as client:
        tasks = [send_with_semaphore(client, b) for b in batches]
        await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start_time
    throughput = len(events) / elapsed if elapsed > 0 else 0

    # Ringkasan hasil
    print("\n" + "=" * 64)
    print("  RINGKASAN PENGIRIMAN")
    print("=" * 64)
    print(f"  Total event dikirim      : {len(events)}")
    print(f"  Estimasi unik            : {unique_count}")
    print(f"  Estimasi duplikat dikirim : {dup_count}")
    print(f"  Berhasil di-queue        : {sent_ok}")
    print(f"  Gagal                    : {sent_fail}")
    print(f"  Durasi total             : {elapsed:.2f} detik")
    print(f"  Throughput               : {throughput:.0f} event/detik")
    print("=" * 64)

    # Tunggu consumer selesai memproses
    print("\n[WAIT] Menunggu consumer memproses semua event...")
    await asyncio.sleep(10)

    # Ambil statistik akhir dari aggregator
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{AGGREGATOR_URL}/stats", timeout=10.0)
            stats = resp.json()
            print("\n" + "=" * 64)
            print("  STATISTIK AGGREGATOR")
            print("=" * 64)
            print(f"  Received           : {stats['received']}")
            print(f"  Unique processed   : {stats['unique_processed']}")
            print(f"  Duplicate dropped  : {stats['duplicate_dropped']}")
            print(f"  Topics             : {stats['topics']}")
            print(f"  Uptime (detik)     : {stats['uptime_seconds']}")
            print("=" * 64)
    except Exception as e:
        print(f"[WARN] Gagal ambil stats: {e}")

    print("\n[PUBLISHER] Selesai.\n")


def parse_args():
    """Parse argumen CLI."""
    parser = argparse.ArgumentParser(
        description="Pub-Sub Log Aggregator — Event Publisher Simulator"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=int(os.getenv("EVENT_COUNT", "25000")),
        help="Jumlah total event yang akan di-generate (default: 25000)",
    )
    parser.add_argument(
        "--dup-rate",
        type=float,
        default=float(os.getenv("DUP_RATE", "0.35")),
        help="Persentase duplikat 0.0–1.0 (default: 0.35)",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=int(os.getenv("BATCH_SIZE", "50")),
        help="Ukuran batch per request (default: 50)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("CONCURRENCY", "5")),
        help="Max concurrent requests (default: 5)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(args))
