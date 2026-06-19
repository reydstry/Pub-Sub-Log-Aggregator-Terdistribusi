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
| Total event dikirim       | [ISI]          |
| Event unik (estimasi)     | [ISI]          |
| Event duplikat (estimasi) | [ISI]          |
| `unique_processed` (stats)| [ISI]          |
| `duplicate_dropped` (stats)| [ISI]         |
| Durasi total (detik)      | [ISI]          |
| Throughput (event/detik)  | [ISI]          |
| Latency p50               | [ISI setelah pengukuran] |
| Latency p95               | [ISI setelah pengukuran] |

---

## 4. Hasil Uji Konkurensi

> **Instruksi**: Jalankan `pytest tests/test_concurrent.py -v` dan dokumentasikan hasil.

```
pytest tests/ -v -m "not integration"
```

| Test                                     | Status  | Keterangan                       |
|------------------------------------------|---------|----------------------------------|
| `test_concurrent_insert_same_event`      | [PASS/FAIL] | 10 tasks, same ID → 1 unique |
| `test_concurrent_insert_different_events`| [PASS/FAIL] | 10 tasks, diff IDs → 10 unique |
| `test_stats_consistency_under_load`      | [PASS/FAIL] | 100 events, invariant check  |

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

---

### T1 [Bab 1]: Karakteristik Sistem Terdistribusi

Sistem terdistribusi memiliki beberapa karakteristik fundamental. Pertama, **concurrency**: dalam sistem ini, tiga consumer worker berjalan secara paralel membaca dari Redis queue, sementara FastAPI melayani request HTTP secara konkuren — semua harus beroperasi tanpa konflik data. Kedua, **transparency**: lokasi fisik service (broker, storage, aggregator) disembunyikan melalui DNS internal Docker Compose; publisher cukup mengetahui hostname `aggregator`, bukan alamat IP kontainer (Coulouris et al., 2012). Ketiga, **heterogeneity**: sistem mengintegrasikan tiga teknologi berbeda (Python/FastAPI, Redis, PostgreSQL) yang berkomunikasi melalui protokol standar (HTTP, RESP, PostgreSQL wire protocol). Keempat, **no global clock**: setiap event membawa timestamp ISO 8601 dari source masing-masing karena tidak ada jam global yang disinkronkan.

Trade-off utama pada arsitektur ini adalah antara **konsistensi dan availability**. Dengan memilih deduplication di level database (UNIQUE constraint), sistem mengorbankan sedikit throughput (setiap INSERT melibatkan constraint check) demi menjamin konsistensi data. Selain itu, penggunaan Redis sebagai broker in-memory menambah risiko kehilangan data jika Redis crash sebelum consumer memproses event, meskipun named volume `broker_data` memitigasi sebagian risiko tersebut (Coulouris et al., 2012).

---

### T2 [Bab 2]: Arsitektur Publish-Subscribe vs Client-Server

Arsitektur publish-subscribe lebih tepat dibandingkan client-server ketika terdapat **decoupling temporal dan spasial** antara pengirim dan penerima pesan. Pada model client-server tradisional, publisher harus mengetahui lokasi dan status setiap consumer, serta menunggu respons sinkron — hal ini menciptakan tight coupling yang mengurangi fleksibilitas dan skalabilitas (Coulouris et al., 2012). Sebaliknya, pada pola pub-sub, publisher cukup mengirim event ke broker tanpa peduli siapa atau berapa banyak consumer yang akan memproses event tersebut.

Dalam proyek ini, keputusan menggunakan pub-sub dilatarbelakangi beberapa pertimbangan teknis. Pertama, **asinkronisitas**: publisher tidak perlu menunggu event diproses oleh consumer; cukup mendapat konfirmasi `{"queued": N}` bahwa event sudah masuk antrian. Kedua, **skalabilitas consumer**: jumlah worker bisa ditambah (melalui environment variable `NUM_WORKERS`) tanpa perubahan pada publisher. Ketiga, **buffering beban**: Redis queue menjadi buffer ketika publisher mengirim burst event lebih cepat dari kemampuan consumer memproses. Jika aggregator dipanggil langsung oleh publisher secara sinkron, setiap request harus menunggu INSERT ke PostgreSQL selesai, yang meningkatkan latensi end-to-end secara signifikan dan mengurangi throughput keseluruhan sistem (Coulouris et al., 2012).

---

### T3 [Bab 3]: At-Least-Once vs Exactly-Once Delivery

**At-least-once delivery** menjamin bahwa setiap pesan dikirim minimal satu kali ke consumer, tetapi memungkinkan pengiriman ulang (duplikat). **Exactly-once delivery** menjamin setiap pesan diterima dan diproses tepat satu kali — semantik ini jauh lebih sulit diimplementasikan karena memerlukan koordinasi antara broker, consumer, dan storage dalam satu transaksi terdistribusi (Coulouris et al., 2012).

Sistem ini memilih **at-least-once + idempotent consumer** karena beberapa alasan. Pertama, exactly-once memerlukan mekanisme seperti two-phase commit atau saga pattern yang menambah kompleksitas signifikan dan mengurangi throughput. Kedua, Redis `BLPOP` secara inheren memberikan at-least-once: jika worker crash setelah pop tetapi sebelum INSERT, event hilang — sebaliknya, jika ada retry di sisi publisher, event bisa dikirim ulang. Ketiga, dengan menjadikan consumer **idempoten** melalui `INSERT ON CONFLICT DO NOTHING`, efek dari menerima event duplikat sama persis dengan menerima sekali — counter `duplicate_dropped` di-increment, tetapi tidak ada data ganda di tabel `events`.

Trade-off: at-least-once + idempotency lebih sederhana dan lebih cepat, namun memerlukan storage persisten (PostgreSQL) untuk tracking state dedup. Jika state dedup hilang, duplikat bisa lolos sebagai event baru (Coulouris et al., 2012).

---

### T4 [Bab 4]: Skema Penamaan Topic dan Event ID

Sistem ini menggunakan dua level penamaan untuk identifikasi event. **Topic** menggunakan format dot-separated namespace (`user.login`, `order.created`, `payment.processed`) yang memberikan hierarki semantik — memudahkan filtering dan routing. **Event ID** menggunakan UUID v4 (Universally Unique Identifier versi 4), yang dihasilkan secara acak dengan probabilitas collision sekitar 1 banding 2^122 (Coulouris et al., 2012).

UUID v4 dipilih karena: (1) dapat di-generate secara desentralisasi tanpa koordinasi — setiap publisher menghasilkan ID sendiri tanpa perlu central authority; (2) format standar 128-bit yang didukung oleh PostgreSQL secara native; (3) tidak mengandung informasi temporal yang bisa menjadi privacy concern. Kombinasi `(topic, event_id)` sebagai composite key pada UNIQUE constraint menjamin **collision-resistant deduplication**: event dengan ID identik pada topic berbeda dianggap sebagai event berbeda (karena konteks topic berbeda), sementara event dengan ID identik pada topic yang sama pasti duplikat.

Kelemahan UUID v4: tidak time-sortable sehingga tidak bisa digunakan untuk ordering. Alternatif seperti ULID (library `python-ulid` sudah terinstal) bisa dipertimbangkan jika ordering berdasarkan ID diperlukan (Coulouris et al., 2012).

---

### T5 [Bab 5]: Event Ordering dan Timestamp

Dalam sistem terdistribusi, **total ordering** (semua node melihat urutan event yang sama) sulit dicapai tanpa mekanisme seperti logical clock atau consensus protocol (Coulouris et al., 2012). Sistem log aggregator ini **tidak memerlukan total ordering** karena tujuannya adalah mengumpulkan dan mendeduplikasi event, bukan menjamin urutan eksekusi. Setiap event berdiri sendiri — tidak ada dependensi kausal antar event yang berbeda.

Timestamp ISO 8601 yang dibawa setiap event memiliki batasan: (1) bergantung pada jam lokal source, yang bisa drift antar mesin; (2) resolusi milidetik mungkin tidak cukup untuk membedakan event yang terjadi sangat berdekatan; (3) timezone yang berbeda antar source bisa menyebabkan kebingungan jika tidak dinormalisasi ke UTC. Strategi praktis yang diterapkan: semua timestamp dikonversi ke `TIMESTAMPTZ` di PostgreSQL (timezone-aware), field `received_at DEFAULT NOW()` menambahkan timestamp server sebagai referensi kedua, dan query diurutkan berdasarkan `received_at DESC` (bukan timestamp event) untuk konsistensi tampilan.

Jika total ordering diperlukan di masa depan, pendekatan yang bisa diterapkan adalah **Lamport timestamp** atau penggunaan Redis Streams dengan consumer group yang menjamin FIFO ordering per-stream (Coulouris et al., 2012).

---

### T6 [Bab 6]: Failure Modes dan Mitigasi

Sistem ini mengidentifikasi minimal empat failure mode beserta mitigasinya.

**1. PostgreSQL down saat startup**: Aggregator mengimplementasikan **retry loop** hingga 15 kali dengan delay 2 detik antar percobaan (`init_db()`). Docker Compose `depends_on: condition: service_healthy` juga memastikan PostgreSQL sudah melewati health check sebelum aggregator dimulai.

**2. Redis broker crash**: Event yang sudah di-pop oleh consumer tetapi belum diproses akan hilang. Mitigasi: named volume `broker_data` menjaga persistensi Redis (AOF/RDB), dan publisher mengirim ulang event yang gagal dengan **exponential backoff** (1s, 2s, 4s) hingga maksimal 3 retry (Coulouris et al., 2012).

**3. Consumer worker crash**: Karena worker adalah asyncio task, crash satu worker tidak mempengaruhi worker lain. Error di-catch dalam `try/except`, di-log, dan worker melanjutkan loop setelah delay 0.5 detik — mencegah spin-loop.

**4. Data corruption/loss**: **Named volume `pg_data`** menjamin data PostgreSQL bertahan meskipun container dihapus. UNIQUE constraint bertindak sebagai **durable dedup store** — state deduplication tidak pernah hilang selama volume tidak dihapus. Test `test_dedup_survives_restart` memverifikasi skenario ini secara eksplisit (Coulouris et al., 2012).

---

### T7 [Bab 7]: Model Konsistensi

Sistem ini menerapkan model **eventual consistency**: setelah publisher mengirim event, ada delay sebelum event muncul di `GET /events` (event harus melewati Redis queue → consumer → PostgreSQL). Namun, setelah consumer memproses event, data di PostgreSQL bersifat **strongly consistent** karena PostgreSQL adalah single-node database dengan ACID guarantees (Coulouris et al., 2012).

Kombinasi **idempotency + dedup store** memastikan konsistensi meskipun event diterima berkali-kali. Mekanismenya: (1) setiap event diidentifikasi oleh pasangan `(topic, event_id)` yang unik; (2) `INSERT ON CONFLICT DO NOTHING` menjamin bahwa duplikat diabaikan tanpa error; (3) counter `received`, `unique_processed`, dan `duplicate_dropped` di-update secara **transaksional** dalam satu `BEGIN...COMMIT` block — tidak mungkin counter inkonsisten (misalnya received bertambah tapi unique/dropped tidak).

Invariant yang selalu terjaga: `received = unique_processed + duplicate_dropped`. Test `test_stats_consistency_under_load` memverifikasi invariant ini dengan mengirim 100 event (30% duplikat) secara concurrent. Model ini memberikan jaminan bahwa walaupun jaringan tidak reliable dan event dikirim berulang, state akhir database selalu benar dan konsisten (Coulouris et al., 2012).

---

### T8 [Bab 8]: Desain Transaksi pada insert_event()

Fungsi `insert_event()` mengimplementasikan properti ACID dalam satu transaksi PostgreSQL. **Atomicity**: INSERT event dan UPDATE stats dibungkus dalam `async with conn.transaction()` — keduanya berhasil atau keduanya di-rollback. **Consistency**: UNIQUE constraint menjamin tidak ada duplikat; counter stats selalu konsisten dengan jumlah event aktual. **Isolation**: menggunakan READ COMMITTED, yang cukup karena setiap transaksi hanya melibatkan satu INSERT dan satu UPDATE pada row yang berbeda. **Durability**: setelah commit, data tersimpan di disk PostgreSQL (WAL + named volume) (Coulouris et al., 2012).

READ COMMITTED dipilih alih-alih SERIALIZABLE karena: (1) tidak ada transaksi yang membaca lalu menulis berdasarkan hasil baca (write-skew tidak mungkin terjadi); (2) UNIQUE constraint sudah menjadi safeguard terhadap lost-update pada skenario multi-worker — dua worker yang meng-INSERT event_id sama secara bersamaan, hanya satu yang berhasil karena constraint divalidasi via exclusive lock pada index tuple; (3) SERIALIZABLE menambah overhead serialization failure dan retry logic yang tidak diperlukan di sini.

Throughput meningkat karena READ COMMITTED mengizinkan reader dan writer bekerja paralel tanpa saling memblokir, sementara integritas data tetap dijamin oleh constraint (Coulouris et al., 2012).

---

### T9 [Bab 9]: Locking (Pessimistic) vs Unique Constraint (Optimistic)

**Pessimistic locking** (misalnya `SELECT FOR UPDATE`) mengunci row sebelum modifikasi — menjamin tidak ada konflik, tetapi mengurangi throughput karena transaksi lain harus menunggu lock dilepas. Pada skenario dedup dengan banyak writer paralel, pessimistic locking menyebabkan **lock contention** yang signifikan: setiap worker harus mengunci entry `(topic, event_id)` sebelum insert, bahkan jika entry belum ada (Coulouris et al., 2012).

**Optimistic approach** dengan UNIQUE constraint yang diterapkan sistem ini bekerja sebaliknya: tidak ada lock eksplisit; setiap worker langsung mencoba INSERT. Jika terjadi conflict (duplikat), PostgreSQL mendeteksi via constraint dan mengembalikan status `INSERT 0 0` — bukan error, bukan rollback, hanya no-op. Pendekatan ini disebut **idempotent write pattern**: `INSERT ON CONFLICT DO NOTHING` selalu aman dipanggil berkali-kali.

Implikasi terhadap throughput: (1) tidak ada waktu tunggu lock — semua worker menulis secara paralel; (2) constraint check dilakukan pada B-tree index entry dengan granularitas sangat kecil (per-tuple lock, bukan table lock); (3) pada kasus 3 worker menulis event berbeda, ketiganya berjalan tanpa saling mengganggu. Test `test_concurrent_insert_same_event` membuktikan bahwa 10 tasks konkuren yang meng-insert event_id sama menghasilkan tepat 1 `unique_processed` tanpa race condition atau deadlock — validasi bahwa optimistic approach bekerja dengan benar (Coulouris et al., 2012).

---

### T10 [Bab 10–13]: Docker Compose sebagai Orkestrasi Layanan

Docker Compose mewujudkan beberapa aspek penting sistem terdistribusi. **Orkestrasi layanan**: file `docker-compose.yml` mendeskripsikan keempat service (aggregator, publisher, broker, storage) beserta dependensinya secara deklaratif. `depends_on` dengan `condition: service_healthy` memastikan urutan startup yang benar — PostgreSQL dan Redis harus healthy sebelum aggregator dimulai (Coulouris et al., 2012).

**Isolasi jaringan lokal**: semua service terhubung melalui bridge network `pubsub-net`. Broker (Redis) dan storage (PostgreSQL) **tidak memiliki port mapping** ke host — hanya bisa diakses dari dalam jaringan Compose. Ini mensimulasikan isolasi jaringan pada sistem terdistribusi nyata di mana internal service tidak terekspos ke publik.

**Persistensi via named volumes**: `pg_data` dan `broker_data` memastikan data bertahan melampaui lifecycle container. Perintah `docker compose down` menghapus container tetapi mempertahankan volume; hanya `docker compose down -v` yang menghapus data secara eksplisit.

**Observability**: endpoint `GET /stats` menyediakan counter real-time (received, unique_processed, duplicate_dropped), daftar topic aktif, dan uptime. Structured logging pada middleware mencatat setiap request (method, path, status, durasi ms). Logging pada consumer menandai setiap event dengan `[NEW]` atau `[DEDUP]` untuk traceability (Coulouris et al., 2012).

---

## 6. Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed
systems: Concepts and design* (5th ed.). Pearson.

Docker Inc. (2024). *Docker Compose overview*. Docker Documentation.
https://docs.docker.com/compose/

The PostgreSQL Global Development Group. (2024). *PostgreSQL 16 documentation:
Transaction isolation*. https://www.postgresql.org/docs/16/transaction-iso.html

Redis Ltd. (2024). *Redis documentation: BLPOP*. https://redis.io/commands/blpop

Tiangolo, S. (2024). *FastAPI documentation*. https://fastapi.tiangolo.com/
