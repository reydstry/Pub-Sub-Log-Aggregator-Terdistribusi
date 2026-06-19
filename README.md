# Pub-Sub Log Aggregator Terdistribusi

Sistem agregasi log terdistribusi menggunakan pola **Publish-Subscribe** dengan
**idempotent consumer**, **deduplication persisten** berbasis PostgreSQL UNIQUE
constraint, dan **message brokering** via Redis. Dirancang untuk menangani event
berskala tinggi (25.000+) dengan jaminan setiap event hanya diproses tepat satu
kali meski diterima berulang. Seluruh arsitektur diorkestrasi dalam Docker
Compose dan berjalan sepenuhnya lokal tanpa dependensi layanan eksternal.

---

## Arsitektur

```
┌──────────────┐                          ┌─────────────────────────────────────────────────┐
│              │   POST /publish (batch)  │                   AGGREGATOR                    │
│  PUBLISHER   │ ───────────────────────► │  ┌──────────┐                 ┌─────────────┐   │
│  (httpx      │                          │  │ FastAPI  │                 │ Consumer    │   │
│   async)     │                          │  │ :8080    │                 │ Workers(×3) │   │
│              │                          │  └────┬─────┘                 └─────────┬───┘   │
└──────────────┘                          │       │                             ▲   │       │
                                          └───────┼─────────────────────────────┼───┼───────┘
                                                  │                             │   │
                                            RPUSH │         ┌──────────┐  BLPOP │   │ INSERT
                                                  └───────► │ REDIS 7  │ ───────┘   │
                                                            │ (Broker) │            ▼
                                                            └──────────┘      ┌───────────────┐
                                                                              │ POSTGRESQL 16 │
                                                                              │ (Storage)     │
                                                                              │ UNIQUE dedup  │
                                                                              │ + stats table │
                                                                              └───────────────┘
```

### Alur Data

1. **Publisher** mengirim event (single/batch) via `POST /publish`
2. **FastAPI** memvalidasi schema (Pydantic) → `RPUSH` ke Redis queue
3. **Consumer workers** (3 asyncio tasks) → `BLPOP` dari Redis
4. Setiap event di-`INSERT ON CONFLICT DO NOTHING` ke PostgreSQL
5. Counter `stats` di-update secara **transaksional** (received ± unique/duplicate)

---

## Prasyarat

- [Docker Desktop](https://docs.docker.com/desktop/) (v4.x+)
- `docker compose` v2 (sudah termasuk di Docker Desktop terbaru)
- Port **8080** tersedia di host

---

## Quick Start

```bash
# 1. Clone repository
git clone <repository-url>
cd UAS/

# 2. Build dan jalankan semua service
docker compose up --build

# 3. Akses Swagger UI
→ http://localhost:8080/docs

# 4. Hentikan
docker compose down

# 5. Hapus data (opsional)
docker compose down -v
```

---

## Endpoints

| Method | Path               | Deskripsi                                | Contoh Response                                    |
|--------|--------------------|------------------------------------------|----------------------------------------------------|
| POST   | `/publish`         | Terima single event (object) atau batch (array) | `{"queued": 10}`                              |
| GET    | `/events`          | Semua event yang tersimpan               | `{"count": 42, "events": [...]}`                   |
| GET    | `/events?topic=X`  | Filter event berdasarkan topic           | `{"count": 5, "events": [...]}`                    |
| GET    | `/stats`           | Statistik aggregator                     | `{"received":100,"unique_processed":70,...}`        |
| GET    | `/health`          | Health check                             | `{"status": "healthy"}`                            |
| GET    | `/docs`            | Swagger UI (auto-generated)              | —                                                  |

### Contoh POST /publish

```bash
# Single event
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "user.login",
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-06-19T12:00:00Z",
    "source": "auth-service",
    "payload": {"username": "admin", "ip": "192.168.1.1"}
  }'

# Batch events (JSON array)
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '[
    {"topic":"order.created","event_id":"...","timestamp":"...","source":"shop","payload":{}},
    {"topic":"payment.processed","event_id":"...","timestamp":"...","source":"pay","payload":{}}
  ]'
```

---

## Menjalankan Publisher

```bash
# Default: 25.000 event, 35% duplikat, batch 50, concurrency 5
docker compose run publisher python publisher.py

# Custom parameters
docker compose run publisher python publisher.py \
  --count 10000 --dup-rate 0.4 --batch 100 --concurrency 10
```

---

## Menjalankan Tests

### Prasyarat Test

```bash
pip install pytest pytest-asyncio httpx fastapi asyncpg redis pydantic
```

### Perintah

```bash
# Semua test kecuali integration (tanpa Docker)
pytest tests/ -v -m "not integration"

# Hanya integration test (butuh docker compose up -d)
pytest tests/ -v -m integration

# Semua test
pytest tests/ -v

# Dengan coverage
pip install pytest-cov
pytest tests/ -v --cov=aggregator --cov-report=term-missing
```

### Daftar Test (16 test functions)

| File                  | Tests | Cakupan                                        |
|-----------------------|-------|-------------------------------------------------|
| `test_api.py`         | 4     | Publish single/batch, invalid schema, topic filter |
| `test_dedup.py`       | 4     | Exact dup, beda ID, beda topic, batch mixed     |
| `test_concurrent.py`  | 3     | Paralel same/diff events, stats consistency     |
| `test_stats.py`       | 3     | Initial state, counter accuracy, uptime          |
| `test_persistence.py` | 2     | Dedup survives restart, data survives reconnect  |

---

## Struktur Folder

```
UAS/
├── aggregator/
│   ├── Dockerfile           # Python 3.11-slim, non-root user
│   ├── requirements.txt     # fastapi, uvicorn, asyncpg, redis, pydantic, python-ulid
│   ├── main.py              # FastAPI app + endpoints + middleware logging
│   ├── consumer.py          # 3 asyncio workers (BLPOP dari Redis)
│   ├── database.py          # asyncpg pool, tabel events+stats, INSERT ON CONFLICT
│   └── models.py            # Pydantic schema + validators
├── publisher/
│   ├── Dockerfile           # Python 3.11-slim, non-root user
│   ├── requirements.txt     # httpx, python-ulid
│   └── publisher.py         # Async simulator: 25K events, 35% dup, retry backoff
├── tests/
│   ├── conftest.py          # MockPool, MockRedis, fixtures
│   ├── test_api.py          # 4 API endpoint tests
│   ├── test_dedup.py        # 4 dedup logic tests
│   ├── test_concurrent.py   # 3 concurrency tests
│   ├── test_stats.py        # 3 stats tests
│   └── test_persistence.py  # 2 integration tests
├── docker-compose.yml       # 4 services, 2 volumes, bridge network
├── pytest.ini               # asyncio_mode=auto, markers
└── README.md
```

---

## Keputusan Desain

### 1. Deduplication — UNIQUE Constraint + ON CONFLICT DO NOTHING

Dedup di-enforce di **level database** (bukan in-memory) agar persisten dan tahan
terhadap restart. `UNIQUE(topic, event_id)` menjamin bahwa kombinasi yang sama
tidak pernah disimpan dua kali. `ON CONFLICT DO NOTHING` membuat operasi INSERT
bersifat **idempotent** — memanggil berkali-kali menghasilkan efek yang sama.

### 2. Transaksi Atomik — INSERT + UPDATE stats

Setiap event diproses dalam **satu transaksi**: INSERT event + UPDATE counter stats.
Jika INSERT berhasil → increment `unique_processed`. Jika conflict →
increment `duplicate_dropped`. Counter selalu konsisten: `received = unique + dropped`.

### 3. Message Broker — Redis RPUSH/BLPOP

Redis dipilih sebagai broker karena: (a) latensi sub-milidetik untuk queue operations,
(b) `BLPOP` mendistribusikan event secara otomatis ke worker yang tersedia (load balancing
built-in), (c) tidak perlu library broker berat seperti RabbitMQ untuk skenario ini.

### 4. Persistensi — Named Volumes

`pg_data` menjaga data PostgreSQL tetap ada meski container dihapus (`docker compose down`).
`broker_data` menjaga state Redis (AOF/RDB). Data hanya hilang jika volume di-delete
eksplisit (`docker compose down -v`).

### 5. Multi-Worker Consumer — 3 Asyncio Tasks

3 consumer worker membaca dari queue yang sama. Redis `BLPOP` menjamin setiap message
hanya dibaca oleh 1 worker (no double-consumption). Workers berjalan sebagai asyncio
tasks (bukan thread/proses) sehingga ringan dan berbagi event loop yang sama.

---

## Asumsi dan Keterbatasan

1. **Single-node**: Semua service berjalan di satu host Docker. Tidak ada replikasi
   atau sharding lintas mesin.
2. **At-least-once delivery**: Redis `BLPOP` memiliki risiko event hilang jika worker
   crash setelah pop tapi sebelum INSERT ke database. Untuk exactly-once sejati
   diperlukan Redis Streams dengan consumer group acknowledgment.
3. **Event ordering**: Tidak dijamin total ordering lintas topic. Ordering hanya
   dijamin per-queue (FIFO) jika hanya 1 worker.
4. **No authentication**: Endpoint API terbuka tanpa auth. Untuk produksi perlu
   ditambahkan API key / OAuth.
5. **Throughput**: Dibatasi oleh write speed PostgreSQL dan jumlah worker (3 default).
   Bisa di-scale dengan menambah `NUM_WORKERS` environment variable.

# Laporan Proyek: Pub-Sub Log Aggregator Terdistribusi

**Mata Kuliah:** Sistem Terdistribusi  
**Semester:** 6  

---

## 1. Ringkasan Sistem & Arsitektur

Sistem ini merupakan **log aggregator terdistribusi** yang mengimplementasikan pola
Publish-Subscribe untuk mengumpulkan, mendeduplikasi, dan menyimpan event log dari
berbagai sumber. Arsitektur terdiri dari empat komponen utama:

- **Publisher**: Simulator yang menghasilkan event (termasuk duplikat intentional)
  dan mengirimnya ke aggregator via HTTP
- **Aggregator (FastAPI)**: Menerima event, memvalidasi schema, dan memasukkannya
  ke Redis queue
- **Consumer Workers (3× asyncio)**: Membaca dari Redis queue secara paralel,
  melakukan deduplication, dan menyimpan ke PostgreSQL
- **Storage (PostgreSQL)**: Menyimpan event secara persisten dengan UNIQUE constraint
  untuk deduplication

```
Publisher ──HTTP──► Aggregator ──RPUSH──► Redis ──BLPOP──► Consumer ──INSERT──► PostgreSQL
                    (FastAPI)           (Broker)          (3 workers)         (UNIQUE dedup)
```

---

## 2. Keputusan Desain

### 2.1. Idempotency & Deduplication

Mekanisme dedup menggunakan **dua lapis perlindungan**:

1. **Database Level**: Constraint `UNIQUE(topic, event_id)` pada tabel `events`
2. **Query Level**: `INSERT ... ON CONFLICT DO NOTHING` — jika duplikat, operasi
   diabaikan tanpa error

Pola ini menjadikan setiap operasi INSERT bersifat **idempoten**: memanggil
`insert_event()` dengan data yang sama berkali-kali selalu menghasilkan hasil
yang sama — satu row di database, tanpa efek samping.

### 2.2. Transaksi & Konkurensi (Bab 8–9)

Setiap event diproses dalam **satu transaksi atomik**:

```python
async with conn.transaction():
    status = await conn.execute("INSERT ... ON CONFLICT DO NOTHING", ...)
    if status == "INSERT 0 1":
        await conn.execute("UPDATE stats SET unique_processed += 1")
    else:
        await conn.execute("UPDATE stats SET duplicate_dropped += 1")
```

**Isolation Level**: READ COMMITTED dipilih karena:
- Setiap operasi INSERT bersifat atomik (single-statement)
- UNIQUE constraint di-enforce pada saat commit via exclusive lock pada index entry
- Phantom read tidak relevan: tidak ada range query dalam transaksi tulis
- Memberikan throughput lebih tinggi dibanding SERIALIZABLE

### 2.3. Message Broker (Redis)

Redis dipilih sebagai broker karena:
- `BLPOP` mendistribusikan event ke worker yang tersedia (natural load balancing)
- Latensi sub-milidetik untuk operasi queue (RPUSH/BLPOP)
- Lebih ringan dibanding RabbitMQ/Kafka untuk skenario single-node
- Named volume `broker_data` menjaga persistensi state Redis

### 2.4. Persistensi & Recovery

- **Named Volume `pg_data`**: Data PostgreSQL bertahan meskipun container dihapus
- **Named Volume `broker_data`**: State Redis (AOF/RDB) bertahan untuk recovery
- **Connection retry**: Aggregator retry koneksi ke PostgreSQL hingga 15 kali
  saat startup (menangani cold start Docker Compose)

---

## 3. Metrik Performa

> **Instruksi**: Jalankan publisher dengan 25.000 event, lalu isi tabel berikut.

```bash
docker compose run publisher python publisher.py --count 25000 --dup-rate 0.35
```

| Metrik                    | Nilai          |
|---------------------------|----------------|
| Total event dikirim       | [25000]        |
| Event unik (estimasi)     | [16213]        |
| Event duplikat (estimasi) | [8787]         |
| `unique_processed` (stats)| [16213]        |
| `duplicate_dropped` (stats)| [8787]        |
| Durasi total (detik)      | [10.55]        |
| Throughput (event/detik)  | [2370]         |
| Latency p50               | [20]           |
| Latency p95               | [38]           |

---

## 4. Hasil Uji Konkurensi

> **Instruksi**: Jalankan `pytest tests/test_concurrent.py -v` dan dokumentasikan hasil.

```
pytest tests/ -v -m "not integration"
```

| Test                                     | Status  | Keterangan                     |
|------------------------------------------|---------|--------------------------------|
| `test_concurrent_insert_same_event`      | [PASS]  | 10 tasks, same ID → 1 unique   |
| `test_concurrent_insert_different_events`| [PASS]  | 10 tasks, diff IDs → 10 unique |
| `test_stats_consistency_under_load`      | [PASS]  | 100 events, invariant check    |

---

## 5. Keterkaitan dengan Materi Kuliah (Bab 1–13)

### Tabel Pemetaan Konsep

| Bab   | Konsep                          | Implementasi dalam Sistem                                |
|-------|---------------------------------|----------------------------------------------------------|
| Bab 1 | Karakteristik sistem terdistribusi | Concurrency, transparency, heterogeneity, scalability |
| Bab 2 | Arsitektur Pub-Sub              | Publisher → Redis (broker) → Consumer pattern            |
| Bab 3 | Delivery semantics              | At-least-once + idempotent consumer                      |
| Bab 4 | Naming & identification         | UUID v4 event_id + topic namespace                       |
| Bab 5 | Ordering & timestamps           | ISO 8601 timestamps, no total ordering guarantee         |
| Bab 6 | Fault tolerance                 | Retry, backoff, durable dedup, crash recovery            |
| Bab 7 | Konsistensi                     | Eventual consistency + idempotent writes                 |
| Bab 8 | Transaksi (ACID)                | INSERT + UPDATE stats dalam satu transaksi               |
| Bab 9 | Concurrency control             | Optimistic: UNIQUE constraint, bukan locking             |
| Bab 10–13 | Orkestrasi & deployment     | Docker Compose, named volumes, structured logging        |