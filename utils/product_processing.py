from concurrent.futures import ThreadPoolExecutor
import threading
import queue
import json
from collections import defaultdict
from config.db_config import main_collection

# ===== CẤU HÌNH =====
BATCH_SIZE = 5000
THREADS = 4
TEMP_FILE = "product_data_temp.json"
LOCK = threading.Lock()
batch_queue = queue.Queue()
progress_counter = 0
CHECKPOINT_FILE = "checkpoint_product_processing.txt"

# ===== CÁC COLLECTION CẦN LỌC =====
COLLECTIONS = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed"
]

def fetch_batch(skip, limit):
    """Fetch data in batches from MongoDB"""
    cursor = main_collection.find(
        {"collection": {"$in": COLLECTIONS}},
        {"product_id": 1, "viewing_product_id": 1, "current_url": 1, "_id": 0}
    ).sort([("_id", 1)]).skip(skip).limit(limit)
    return list(cursor)

def process_batch(skip):
    """Fetch and store batch data in queue"""
    data = fetch_batch(skip, BATCH_SIZE)
    if data:
        batch_number = (skip // BATCH_SIZE) + 1
        print(f"[THREAD] Fetched Batch {batch_number} (skip={skip})")
        batch_queue.put((batch_number, data))

def save_checkpoint(batch_number):
    with LOCK:
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(batch_number))

def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def writer_thread(total_records):
    """Gộp dữ liệu theo product_id và lưu ra file JSON (loại trùng URL)"""
    merged_data = defaultdict(set)
    expected_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
    global progress_counter

    for _ in range(expected_batches):
        batch_number, data = batch_queue.get()

        with LOCK:
            progress_counter += 1
            percent = (progress_counter / expected_batches) * 100
            print(f"[WRITER] Processing Batch {batch_number} | Progress: {progress_counter}/{expected_batches} ({percent:.2f}%)")
            save_checkpoint(batch_number)

        for item in data:
            pid = item.get("product_id") or item.get("viewing_product_id")
            url = item.get("current_url")
            if pid and url:
                merged_data[pid].add(url)

    # Chuyển thành list và lưu file
    result = [
        {"product_id": pid, "urls": sorted(list(urls))}
        for pid, urls in sorted(merged_data.items())
    ]

    with open(TEMP_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[WRITER] Saved {len(result)} product groups to {TEMP_FILE}")

def get_product_data():
    """Fetch all data and save to JSON"""
    total_records = main_collection.count_documents({
        "collection": {"$in": COLLECTIONS}
    })

    print(f"[INFO] Total records: {total_records}")
    total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"[INFO] Total batches: {total_batches}")

    last_checkpoint = load_checkpoint()
    print(f"[CHECKPOINT] Last completed batch: {last_checkpoint}")

    writer = threading.Thread(target=writer_thread, args=(total_records,))
    writer.start()

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for skip in range(0, total_records, BATCH_SIZE):
            batch_number = (skip // BATCH_SIZE) + 1
            if batch_number > last_checkpoint:
                executor.submit(process_batch, skip)
            else:
                print(f"[SKIP] Batch {batch_number} already processed (checkpoint)")

    writer.join()
    print(f"[DONE] Full data has been saved to {TEMP_FILE}")

    with open(TEMP_FILE, "r", encoding="utf-8") as f:
        return json.load(f)
