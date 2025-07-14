from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import threading
import queue
from config.db_config import main_collection

BATCH_SIZE = 5000  # Number of records per batch
THREADS = 4  # Number of parallel threads
TEMP_FILE = "product_data_temp.csv"  # Temporary file to store batches
LOCK = threading.Lock()
batch_queue = queue.Queue()  # Queue để lưu batch trước khi ghi vào file

def fetch_batch(skip, limit):
    """Fetch data in batches from MongoDB"""
    cursor = main_collection.find(
        {"collection": {"$in": ["view_product_detail", "select_product_option", "select_product_option_quality"]}},
        {"product_id": 1, "current_url": 1, "_id": 0}
    ).sort([("_id", 1)]).skip(skip).limit(limit)  # Đảm bảo lấy theo thứ tự
    
    data = list(cursor)
    return pd.DataFrame(data)

def process_batch(skip):
    """Fetch and store batch data in queue"""
    df = fetch_batch(skip, BATCH_SIZE)
    if not df.empty:
        batch_queue.put((skip, df))  # Đẩy batch vào queue

def writer_thread():
    """Thread để ghi dữ liệu từ queue vào file"""
    with open(TEMP_FILE, "w", encoding="utf-8") as f:
        f.write("product_id,current_url\n")  # Ghi header trước
        expected_skip = 0

        while expected_skip is not None:
            skip, df = batch_queue.get()  # Lấy batch từ queue
            df.to_csv(f, mode="a", header=False, index=False)  # Ghi batch vào file
            print(f"Saved batch {skip} to {TEMP_FILE}")
            
            if skip + BATCH_SIZE >= total_records:
                break  # Dừng nếu đã ghi hết tất cả dữ liệu

def get_product_data():
    """Fetch all data in batches and save to a temporary CSV"""
    global total_records
    total_records = main_collection.count_documents({
        "collection": {"$in": ["view_product_detail", "select_product_option", "select_product_option_quality"]}
    })
    
    print(f"Total records: {total_records}")

    # Khởi động thread ghi dữ liệu
    writer = threading.Thread(target=writer_thread)
    writer.start()

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for skip in range(0, total_records, BATCH_SIZE):
            executor.submit(process_batch, skip)

    writer.join()  # Đợi thread ghi hoàn thành
    print(f"Data has been saved to {TEMP_FILE}")
    return pd.read_csv(TEMP_FILE)