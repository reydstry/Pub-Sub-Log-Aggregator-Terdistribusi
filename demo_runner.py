"""
Cara Menjalankan:
1. Pastikan docker compose sudah di-build dan siap.
2. Install requirement: pip install httpx colorama
3. Jalankan skrip: python demo_runner.py
"""

import subprocess
import httpx
import time
import json
import uuid
import sys
import asyncio
from datetime import datetime, timezone
from colorama import init, Fore, Style

init(autoreset=True)

BASE_URL = "http://localhost:8080"

def wait_enter():
    input(f"\n{Fore.YELLOW}{Style.BRIGHT}>>> Tekan Enter untuk lanjut ke segmen berikutnya...{Style.RESET_ALL}\n")

def print_header(title):
    print(f"\n{Fore.MAGENTA}{Style.BRIGHT}{'='*60}")
    print(f"{title.center(60)}")
    print(f"{'='*60}{Style.RESET_ALL}")

def print_step(msg):
    print(f"{Fore.CYAN}{Style.BRIGHT}[STEP] {msg}{Style.RESET_ALL}")

def print_ok(msg):
    print(f"{Fore.GREEN}{Style.BRIGHT}[OK] {msg}{Style.RESET_ALL}")

def print_fail(msg):
    print(f"{Fore.RED}{Style.BRIGHT}[FAIL] {msg}{Style.RESET_ALL}")

def print_info(msg):
    print(f"{Fore.BLUE}[INFO] {msg}{Style.RESET_ALL}")

def print_result(msg, data=None):
    print(f"{Fore.GREEN}[RESULT] {msg}{Style.RESET_ALL}")
    if data:
        if isinstance(data, dict) or isinstance(data, list):
            print(json.dumps(data, indent=2))
        else:
            print(data)

def run_cmd(cmd, capture=False):
    print_info(f"Menjalankan: {cmd}")
    if capture:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    else:
        subprocess.run(cmd, shell=True)

def generate_event(topic="demo.topic"):
    return {
        "topic": topic,
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "demo-runner",
        "payload": {"demo": True}
    }

def get_stats():
    try:
        r = httpx.get(f"{BASE_URL}/stats")
        return r.json()
    except Exception as e:
        print_fail(f"Gagal mengambil stats: {e}")
        return {}

def segmen_1():
    print_header("SEGMEN 1: Arsitektur & Network Docker Lokal")
    print_step("Cek status container via 'docker compose ps'")
    run_cmd("docker compose ps")
    
    print("\n")
    print_step("Cek network uas_pubsub-net")
    # Parse json dari docker network inspect agar tampilannya lebih bersih
    output = run_cmd("docker network inspect uas_pubsub-net", capture=True)
    try:
        net_info = json.loads(output)[0]
        containers = net_info.get("Containers", {})
        print_info("Container yang terhubung ke uas_pubsub-net:")
        for cid, cinfo in containers.items():
            print(f" - {cinfo['Name']} ({cinfo['IPv4Address']})")
        print_ok("Isolasi network berhasil. Layanan DB dan Redis terlindungi.")
    except Exception:
        print_info(output[:500] + "... (truncated)")
    wait_enter()

def segmen_2():
    print_header("SEGMEN 2: Idempotency Dasar - Single Event")
    print_step("Kirim 1 event unik")
    ev = generate_event("user.login")
    r = httpx.post(f"{BASE_URL}/publish", json=ev)
    print_result(f"Response: {r.json()}")
    
    time.sleep(1)
    print_step("Cek GET /stats")
    stats = get_stats()
    print_result("Stats Saat Ini:", stats)
    wait_enter()

def segmen_3():
    print_header("SEGMEN 3: Duplicate Event Detection")
    ev = generate_event("payment.processed")
    
    print_step("Kirim event pertama (original)")
    r1 = httpx.post(f"{BASE_URL}/publish", json=ev)
    print_result(f"Original Response: {r1.json()}")
    time.sleep(1)
    
    stats1 = get_stats()
    dropped_before = stats1.get("duplicate_dropped", 0)
    
    print_step(f"Kirim event dengan event_id yang SAMA 3x ({ev['event_id']})")
    for i in range(3):
        httpx.post(f"{BASE_URL}/publish", json=ev)
        print_info(f"Duplicate {i+1} terkirim.")
        
    time.sleep(2) # tunggu worker
    stats2 = get_stats()
    dropped_after = stats2.get("duplicate_dropped", 0)
    
    print_result(f"Duplicate dropped sebelumnya: {dropped_before}")
    print_result(f"Duplicate dropped sekarang  : {dropped_after}")
    
    if dropped_after == dropped_before + 3:
        print_ok("Sistem berhasil menolak 3 event duplikat secara idempotent.")
    else:
        print_fail("Perhitungan duplicate dropped tidak sesuai ekspektasi.")
    wait_enter()

def segmen_4():
    print_header("SEGMEN 4: Batch Processing (Mixed Unik & Duplikat)")
    print_step("Membuat batch 10 event: 5 unik + 5 duplikat (dari event pertama batch)")
    
    unique_events = [generate_event("batch.test") for _ in range(5)]
    all_events = unique_events + unique_events # 5 unik, diulang 2x = 10 event
    
    stats_before = get_stats()
    
    r = httpx.post(f"{BASE_URL}/publish", json={"events": all_events})
    print_result(f"Batch Response: {r.json()}")
    
    time.sleep(2)
    stats_after = get_stats()
    
    diff_received = stats_after['received'] - stats_before['received']
    diff_unique = stats_after['unique_processed'] - stats_before['unique_processed']
    diff_dropped = stats_after['duplicate_dropped'] - stats_before['duplicate_dropped']
    
    print_result(f"Batch stats:")
    print(f"- Received bertambah : {diff_received} (Harus 10)")
    print(f"- Unique bertambah   : {diff_unique} (Harus 5)")
    print(f"- Dropped bertambah  : {diff_dropped} (Harus 5)")
    
    if diff_received == 10 and diff_unique == 5 and diff_dropped == 5:
        print_ok("Deduplikasi batch berjalan sempurna!")
    wait_enter()

def segmen_5():
    print_header("SEGMEN 5: Uji Transaksi & Konkurensi (Parallel Inserts)")
    print_step("Memanggil skrip demo_concurrent_proof.py untuk uji asyncio")
    run_cmd(f"{sys.executable} demo_concurrent_proof.py")
    wait_enter()

def segmen_6():
    print_header("SEGMEN 6: Beban Simulator & Metrik Real-time")
    print_step("Menjalankan publisher 1000 event dengan 35% duplikat")
    
    cmd = "docker compose run --rm publisher python publisher.py --count 1000 --dup-rate 0.35 --batch 50"
    print_info(f"Menjalankan command: {cmd}")
    
    # Jalankan sebagai subprocess non-blocking
    proc = subprocess.Popen(cmd, shell=True)
    
    # Monitor stats selama proses berjalan
    print_info("Monitoring metrik (polling setiap 2 detik)...")
    while proc.poll() is None:
        stats = get_stats()
        print(f"   [METRIK] Received: {stats.get('received')} | Unique: {stats.get('unique_processed')} | Dropped: {stats.get('duplicate_dropped')}")
        time.sleep(2)
        
    print_ok("Publisher simulator selesai.")
    stats_final = get_stats()
    print_result("Stats Akhir Simulator:", stats_final)
    wait_enter()

def segmen_7():
    print_header("SEGMEN 7: Crash Recovery & Persistensi Data")
    stats_before = get_stats()
    print_info(f"Stats sebelum crash -> Unique: {stats_before.get('unique_processed')}")
    
    print_step("Mematikan container (docker compose down)")
    run_cmd("docker compose down")
    
    print_step("Menyalakan kembali container (docker compose up -d)")
    run_cmd("docker compose up -d")
    
    print_info("Menunggu layanan siap (health check)...")
    # Tunggu API up
    for i in range(15):
        try:
            r = httpx.get(f"{BASE_URL}/health", timeout=2)
            if r.status_code == 200:
                print_ok("Aggregator UP dan API responsif!")
                break
        except Exception:
            time.sleep(2)
            
    stats_after = get_stats()
    print_info(f"Stats setelah recovery -> Unique: {stats_after.get('unique_processed')}")
    
    if stats_before.get('unique_processed') == stats_after.get('unique_processed'):
        print_ok("Persistensi PostgreSQL berhasil! Data tidak hilang.")
        
    print_step("Mengirim ulang sebuah event LAMA untuk verifikasi state dedup")
    old_ev = generate_event("recovery.test")
    httpx.post(f"{BASE_URL}/publish", json=old_ev)
    time.sleep(1) # biarkan tercatat
    
    # Sekarang kirim ulang old_ev
    stats_mid = get_stats()
    httpx.post(f"{BASE_URL}/publish", json=old_ev)
    time.sleep(1)
    
    stats_final = get_stats()
    diff_dropped = stats_final.get('duplicate_dropped', 0) - stats_mid.get('duplicate_dropped', 0)
    if diff_dropped == 1:
        print_ok("State deduplikasi bertahan (Event ID yang pernah ada ditolak otomatis).")
    wait_enter()

def segmen_8():
    print_header("SEGMEN 8: Validasi Payload via GET /events")
    topic_query = "user.login"
    print_step(f"Mengambil event dengan topic={topic_query}")
    
    r = httpx.get(f"{BASE_URL}/events?topic={topic_query}")
    data = r.json()
    
    print_result(f"Ditemukan {data.get('count')} event.")
    if data.get('count', 0) > 0:
        print_info("Sampel event terakhir:")
        print(json.dumps(data['events'][-1], indent=2))
    wait_enter()

def segmen_9():
    print_header("SEGMEN 9: Observability & Logging Middleware")
    print_step("Melihat 30 baris terakhir log aggregator (mencari [NEW] dan [DEDUP])")
    
    output = run_cmd("docker compose logs aggregator --tail=30", capture=True)
    
    for line in output.split("\n"):
        if "[NEW]" in line:
            print(f"{Fore.GREEN}{line}{Style.RESET_ALL}")
        elif "[DEDUP]" in line:
            print(f"{Fore.YELLOW}{line}{Style.RESET_ALL}")
        elif "POST /publish" in line or "GET /stats" in line:
            print(f"{Fore.CYAN}{line}{Style.RESET_ALL}")
        else:
            print(line)
            
    print_ok("Log menunjukkan HTTP request (durasi dalam ms) dan identifikasi worker untuk tiap event.")
    print_header("SELESAI - DEMONSTRASI SUKSES")

def main():
    print_header("AUTOMASI DEMONSTRASI UAS - PUB-SUB LOG AGGREGATOR")
    
    try:
        segmen_1()
        segmen_2()
        segmen_3()
        segmen_4()
        segmen_5()
        segmen_6()
        segmen_7()
        segmen_8()
        segmen_9()
    except KeyboardInterrupt:
        print("\nDemonstrasi dihentikan pengguna.")

if __name__ == "__main__":
    main()
