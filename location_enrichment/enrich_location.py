from location_enrichment.save_files import save_check_point, save_location, save_error
from location_enrichment.collect_location import collect_ip_location
from multiprocessing import Pool
from tqdm import tqdm
import os
import csv

def load_checkpoint(path):
    if not os.path.exists(path):
        return 1

    with open(path, "r", encoding='utf-8') as rf:
        content = rf.read().strip()
        if content:
            return int(content)
        else:
            return 1

def batch_reader(start_batch, batch_size, input_file):
    start_index = (start_batch-1) * batch_size
    batch_num = start_batch

    with open (input_file, "r", encoding='utf-8') as rf:
        reader = csv.DictReader(rf)
        all_ip = [row["ip_address"] for row in reader if row["ip_address"]]

    for i in range(start_index, len(all_ip), batch_size):
        batch = all_ip[i:i+batch_size]
        yield batch, batch_num
        batch_num+=1

def enrich_location_info(input_file):
    start_batch = load_checkpoint("checkpoint/checkpoint_ip_location.txt")

    for batch, batch_num in batch_reader(start_batch, batch_size=1000, input_file=input_file):
        collected_data = []
        error_data = []

        with Pool(processes=5) as pool:
            for result in tqdm(pool.imap_unordered(collect_ip_location, batch), total=len(batch)):
                if isinstance (result, dict) and "error" in result:
                    error_data.append((result["ip"], result["error"]))
                else:
                    collected_data.append(result)

        save_location(collected_data, batch_num)
        save_check_point(batch_num+1)
        save_error(error_data)