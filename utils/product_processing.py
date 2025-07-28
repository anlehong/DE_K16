from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import threading
import queue
import json
from collections import defaultdict
from config.db_config import main_collection

BATCH_SIZE = 5000
THREADS = 4
TEMP_FILE = "product_data_temp.json"
LOCK = threading.Lock()
batch_queue = queue.Queue()

# Tên các collection cần lọc
TARGET_COLLECTIONS = [
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
        {"collection": {"$in": TARGET_COLLECTIONS}},
        {"product_id": 1, "viewing_product_id": 1, "current_url": 1, "_id": 0}
    ).sort([("_id", 1)]).skip(skip).limit(limit)

    data = []
    for doc in cursor:
        pid = doc.get("product_id") or doc.get("viewing_product_id")
        url = doc.get("current_url")
        if pid and url:
            data.append((pid, url))
    return data

def process_batch(skip):
    """Fetch and store batch data in queue"""
    data = fetch_batch(skip, BATCH_SIZE)
    if data:
        batch_queue.put((skip, data))

def writer_thread():
    """Thread để gom và ghi dữ liệu vào file JSON"""
    combined = defaultdict(set)  # sử dụng set để tránh trùng URL

    while True:
        item = batch_queue.get()
        if item == "DONE":
            break
        skip, data = item
        for pid, url in data:
            combined[pid].add(url)
        print(f"Processed batch {skip}")

    # Sau khi gom xong: chuyển set → list rồi ghi file
    result = [{"product_id": pid, "urls": list(urls)} for pid, urls in combined.items()]
    with open(TEMP_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

def get_product_data():
    """Fetch all data and save to a grouped JSON file"""
    total_records = main_collection.count_documents({
        "collection": {"$in": TARGET_COLLECTIONS}
    })
    print(f"Total records: {total_records}")

    # Start writer thread
    writer = threading.Thread(target=writer_thread)
    writer.start()

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for skip in range(0, total_records, BATCH_SIZE):
            executor.submit(process_batch, skip)

    # Gửi tín hiệu dừng writer
    batch_queue.put("DONE")
    writer.join()

    print(f"Data has been saved to {TEMP_FILE}")
    with open(TEMP_FILE, "r", encoding="utf-8") as f:
        return json.load(f)
