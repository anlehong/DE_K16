import os
import json
import tldextract
import logging
import time
import random
from tqdm import tqdm
from multiprocessing import Pool
from product_enrichment.get_product_data import collect_product_info
from product_enrichment.save_files import save_product_detail,save_check_point, save_pid_error_jsonl, save_pid_error_403, save_pid_error_404, save_pid_error_retries
from product_enrichment.yaml_config import load_config


config = load_config()
log_cf = config.get("LOGGING", {})

logging.basicConfig(
    level=getattr(logging, log_cf.get("level","INFO").upper(), logging.INFO),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_cf["log_file"], encoding='utf-8'),
        logging.StreamHandler() if log_cf.get("to_console", False) else logging.NullHandler()
    ]

)

def extract_tag(url):
    ext = tldextract.extract(url)
    return ext.suffix

def read_file(file):
    product_ids = []
    with open(file, "r", encoding="utf-8") as rf:
        product_ids = json.load(rf)
    logging.info(f"Have read {len(product_ids)} PID from {file}")
    return product_ids


def load_checkpoint(path):
    if not os.path.exists(path):
        logging.info(f"Start batch now is 1 with {path}")
        return 1

    with open(path, "r", encoding='utf-8') as rf:
        content = rf.read().strip()
        if content:
            logging.info(f"Start batch now is {int(content)} with file {path}")
            return int(content)
        else:
            logging.info(f"Start batch now is 1 with {path}")
            return 1


def batch_reader(start_batch, batch_size, input_file):
    start_index = (start_batch-1) * batch_size
    batch_num = start_batch

    pids = read_file(input_file)

    for i in range(start_index, len(pids), batch_size):
        batch = pids[i:i+batch_size]
        yield batch, batch_num
        batch_num+=1


def process_tag_product(pid, tag, url_list):
    for url in url_list:
        logging.info(f"Crawling {pid} with ({tag}) and url {url}")
        try:
            data = collect_product_info(url)
            if isinstance(data, dict):
                if "product_id" in data:
                    return data
                elif "status" in data and data["status"] == 403:
                    save_pid_error_403(pid, tag, url)
                elif "status" in data and data["status"] == 404:
                    save_pid_error_404(pid, tag, url)
                elif "status" in data and data["status"] == "fail many attempt":
                    save_pid_error_retries(pid, tag, url)
                elif "href" in data:
                    save_pid_error_jsonl(pid, tag, url)
                else:
                    logging.warning(f"Unknown data format: {data}")
                    save_pid_error_jsonl(pid, tag, url)
            else:
                logging.warning(f"Data is not a dict: {data}")
                save_pid_error_jsonl(pid, tag, url)

        except Exception as e:
            logging.exception(f"Error with {pid} and domain {tag}: {e}")
            save_pid_error_jsonl(pid, tag, url)

        time.sleep(random.uniform(0.3, 1.5))

    logging.warning(f"All URLs failed for PID {pid} and tag {tag}")
    return None


def process_pid(item):
    pid = item["product_id"]
    urls = item.get("list_url", [])
    result = {"product_id": pid}
    tag_to_urls = {}

    for url in urls:
        tag = extract_tag(url)
        tag_to_urls.setdefault(tag, []).append(url)

    for tag, url_list in tag_to_urls.items():
        data = process_tag_product(pid, tag, url_list)
        if data:
            result[tag] = data

    return result if len(result) > 1 else None


def collect_products_data(file_path):
    start_batch = load_checkpoint(f"product_enrichment/checkpoint/checkpoint_crawl_product_data.txt")

    for batch, batch_num in batch_reader(start_batch, batch_size=config["BATCH_SIZE"], input_file=file_path):
        with Pool(processes=config["PROCESSES"]) as pool:
            parameters = [item for item in batch]
            results = list(tqdm(pool.imap_unordered(process_pid, parameters), total=len(batch), desc=f"Batch {batch_num}"))

        all_data = [res for res in results if res]

        save_product_detail(all_data, batch_num)
        save_check_point(batch_num + 1)