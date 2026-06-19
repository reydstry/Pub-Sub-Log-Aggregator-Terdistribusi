"""
Cara Menjalankan:
1. Pastikan docker compose sudah running (aggregator up di http://localhost:8080)
2. Install requirement: pip install httpx colorama
3. Jalankan skrip: python demo_concurrent_proof.py
"""

import asyncio
import httpx
import uuid
from datetime import datetime, timezone
from colorama import init, Fore, Style

# Inisialisasi colorama untuk warna di terminal Windows
init(autoreset=True)

BASE_URL = "http://localhost:8080"

def print_step(msg):
    print(f"\n{Fore.CYAN}{Style.BRIGHT}[STEP] {msg}{Style.RESET_ALL}")

def print_info(msg):
    print(f"{Fore.BLUE}[INFO] {msg}{Style.RESET_ALL}")

def print_result(msg):
    print(f"{Fore.GREEN}{Style.BRIGHT}[RESULT] {msg}{Style.RESET_ALL}")

def create_event(topic, event_id, worker_id):
    """Membuat payload event JSON."""
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": f"worker-{worker_id}",
        "payload": {"concurrency_test": True}
    }

async def send_event(client, event):
    """Mengirim event ke API dan mengembalikan statusnya."""
    try:
        response = await client.post(f"{BASE_URL}/publish", json=event, timeout=10.0)
        return {"worker": event["source"], "status_code": response.status_code, "response": response.json()}
    except Exception as e:
        return {"worker": event["source"], "status_code": 500, "error": str(e)}

async def get_stats(client):
    """Mengambil statistik dari API."""
    response = await client.get(f"{BASE_URL}/stats")
    return response.json()

async def run_same_id_test():
    print_step("Uji 1: Mengirim event_id yang SAMA dari 10 task secara BERSAMAAN")
    
    event_id = str(uuid.uuid4())
    topic = "demo.concurrent.same_id"
    
    print_info(f"Target event_id: {event_id}")
    
    async with httpx.AsyncClient() as client:
        # Catat stats awal
        stats_awal = await get_stats(client)
        print_info(f"Stats Awal -> received: {stats_awal['received']}, unique: {stats_awal['unique_processed']}, duplicate: {stats_awal['duplicate_dropped']}")
        
        # Siapkan 10 event identik (id sama)
        tasks = []
        for i in range(1, 11):
            event = create_event(topic, event_id, i)
            tasks.append(send_event(client, event))
            
        print_info("Menjalankan 10 request secara paralel...")
        results = await asyncio.gather(*tasks)
        
        # Tampilkan hasil per task
        for r in results:
            print(f" - {r['worker']} | HTTP {r['status_code']} | Response: {r.get('response', r.get('error'))}")
            
        # Beri waktu sebentar untuk PostgreSQL memproses background task (kalau ada),
        # Tapi karena arsitektur kita sinkron via consumer, tunggu 2 detik
        await asyncio.sleep(2)
        
        # Cek stats akhir
        stats_akhir = await get_stats(client)
        
        diff_received = stats_akhir['received'] - stats_awal['received']
        diff_unique = stats_akhir['unique_processed'] - stats_awal['unique_processed']
        diff_dropped = stats_akhir['duplicate_dropped'] - stats_awal['duplicate_dropped']
        
        print_result("SUMMARY UJI 1")
        print(f"Total task dikirim       : 10")
        print(f"Tambahan Received        : {diff_received}")
        print(f"Tambahan Unique          : {Fore.GREEN}{diff_unique}{Style.RESET_ALL} (Harus 1)")
        print(f"Tambahan Duplicate       : {Fore.YELLOW}{diff_dropped}{Style.RESET_ALL} (Harus 9)")
        
        # Buktikan received == unique + dropped
        is_consistent = (diff_received == diff_unique + diff_dropped)
        if is_consistent:
            print_result("KONSISTENSI TERJAGA: received == unique_processed + duplicate_dropped")
        else:
            print(f"{Fore.RED}[FAIL] Konsistensi rusak!{Style.RESET_ALL}")

async def run_multiple_ids_test():
    print_step("Uji 2: 3 event_id BERBEDA masing-masing dikirim 5x secara BERSAMAAN (15 task)")
    
    event_ids = [str(uuid.uuid4()) for _ in range(3)]
    topic = "demo.concurrent.mixed"
    
    async with httpx.AsyncClient() as client:
        stats_awal = await get_stats(client)
        
        tasks = []
        worker_id = 1
        for eid in event_ids:
            for _ in range(5):
                event = create_event(topic, eid, worker_id)
                tasks.append(send_event(client, event))
                worker_id += 1
                
        print_info("Menjalankan 15 request secara paralel...")
        results = await asyncio.gather(*tasks)
        
        # Tampilkan beberapa hasil
        print_info("Hasil eksekusi (menampilkan 5 sampel):")
        for r in results[:5]:
            print(f" - {r['worker']} | HTTP {r['status_code']} | {r.get('response')}")
            
        await asyncio.sleep(2)
        
        stats_akhir = await get_stats(client)
        
        diff_received = stats_akhir['received'] - stats_awal['received']
        diff_unique = stats_akhir['unique_processed'] - stats_awal['unique_processed']
        diff_dropped = stats_akhir['duplicate_dropped'] - stats_awal['duplicate_dropped']
        
        print_result("SUMMARY UJI 2")
        print(f"Total task dikirim       : 15 (3 ID unik, @5x duplikat)")
        print(f"Tambahan Received        : {diff_received}")
        print(f"Tambahan Unique          : {Fore.GREEN}{diff_unique}{Style.RESET_ALL} (Harus 3)")
        print(f"Tambahan Duplicate       : {Fore.YELLOW}{diff_dropped}{Style.RESET_ALL} (Harus 12)")
        
        print_step("TABEL HASIL AKHIR SERVER")
        print(f"--------------------------------------------------")
        print(f"{'METRIK':<20} | {'NILAI':<15}")
        print(f"--------------------------------------------------")
        print(f"{'Total Received':<20} | {stats_akhir['received']:<15}")
        print(f"{'Unique Processed':<20} | {Fore.GREEN}{stats_akhir['unique_processed']:<15}{Style.RESET_ALL}")
        print(f"{'Duplicate Dropped':<20} | {Fore.YELLOW}{stats_akhir['duplicate_dropped']:<15}{Style.RESET_ALL}")
        print(f"{'Topics Tracked':<20} | {len(stats_akhir['topics']):<15}")
        print(f"--------------------------------------------------")

async def main():
    print(f"{Fore.MAGENTA}{Style.BRIGHT}=====================================================")
    print(f"  PEMBUKTIAN KONKURENSI & TRANSAKSI (MULTI-WORKER)  ")
    print(f"====================================================={Style.RESET_ALL}")
    
    await run_same_id_test()
    await run_multiple_ids_test()
    
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nDihentikan oleh pengguna.")
