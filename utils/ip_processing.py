from concurrent.futures import ThreadPoolExecutor
from config.iplocation_config import ip2loc
from config.db_config import main_collection, location_collection
from utils.save_failed_ips import save_failed_ips
import time
from pymongo.errors import AutoReconnect

batch_size = 10000  # Number of IPs per batch
num_threads = 4     # Number of threads for parallel processing


def get_location(ip):
    """Get location information for a single IP address."""
    try:
        record = ip2loc.get_all(ip)
        return {
            "ip": ip,
            "country": record.country_long,
            "region": record.region,
            "city": record.city,
        }
    except Exception as e:
        print(f"Error processing IP {ip}: {e}")
        return None


def safe_insert_many(collection, documents, max_retries=5):
    """Insert documents into a MongoDB collection with retry logic."""
    for attempt in range(max_retries):
        try:
            collection.insert_many(documents)
            return
        except AutoReconnect as e:
            wait = 2 ** attempt
            print(f"AutoReconnect error. Retrying in {wait} seconds (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
        except Exception as e:
            print(f"Insert error: {e}")
            return
    print("Insert failed after maximum retries.")


def chunked_insert(collection, data, chunk_size=1000):
    """Insert data into the collection in smaller chunks."""
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        safe_insert_many(collection, chunk)


def process_batch(batch, batch_num):
    """Process a batch of IPs in parallel."""
    results = []
    failed_ips = []

    print(f"Processing batch {batch_num} (size: {len(batch)})")

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_ip = {executor.submit(get_location, ip): ip for ip in batch}

        for future in future_to_ip:
            result = future.result()
            if result:
                results.append(result)
            else:
                failed_ips.append(future_to_ip[future])

    print(f"Batch {batch_num} completed. Success: {len(results)}, Failed: {len(failed_ips)}")
    return results, failed_ips


def process_ips():
    """Main function to process all IP addresses from the main collection."""
    cursor = main_collection.find({}, {"ip": 1})

    batch = []
    batch_num = 0

    for doc in cursor:
        batch.append(doc['ip'])

        if len(batch) >= batch_size:
            batch_num += 1
            results, failed_ips = process_batch(batch, batch_num)

            if results:
                chunked_insert(location_collection, results)

            if failed_ips:
                save_failed_ips(failed_ips)

            batch.clear()

    # Process the final batch
    if batch:
        batch_num += 1
        results, failed_ips = process_batch(batch, batch_num)

        if results:
            chunked_insert(location_collection, results)

        if failed_ips:
            save_failed_ips(failed_ips)

    print("All IP addresses have been processed successfully.")
