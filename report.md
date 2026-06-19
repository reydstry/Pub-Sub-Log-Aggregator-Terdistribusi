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