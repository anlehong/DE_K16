import json
import logging
import os

def save_product_detail(data, batch_num):
    os.makedirs("data/product_detail", exist_ok=True)
    file_path = f"data/product_detail/product_data_{batch_num}.json"

    with open(file_path, "w", encoding="utf-8") as wf:
        json.dump(data, wf, ensure_ascii=False, indent=2)
    logging.info(f"Have saved {len(data)} products into data/product_detail/product_data_{batch_num}.jsonl")


def save_pid_error_jsonl(pid, tag, link, path="data/error/pid_error.jsonl"):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    record = {
        "pid": pid,
        "tag": tag,
        "link": link
    }

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logging.info(f"Have saved pid_error with {pid} into data/error/pid_error.jsonl")

def save_pid_error_retries(pid, tag, link, path="data/error/pid_error_attempt.jsonl"):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    record = {
        "pid": pid,
        "tag": tag,
        "link": link
    }

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logging.info(f"Have saved pid error after 3 attempt with {pid} into data/error/pid_error_attempt.jsonl")

def save_pid_error_403(pid, tag, link, path="data/error/pid_error_403.jsonl"):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    record = {
        "pid": pid,
        "tag": tag,
        "link": link
    }

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logging.info(f"Have saved pid_error 403 with {pid} into data/error/pid_error_403.jsonl")

def save_pid_error_404(pid, tag, link, path="data/error/pid_error_404.jsonl"):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    record = {
        "pid": pid,
        "tag": tag,
        "link": link
    }

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logging.info(f"Have saved pid_error 404 with {pid} into data/error/pid_error_404.jsonl")

def save_pid_url_by_collection(data, collection_name):
    os.makedirs("data/pid_by_collection", exist_ok=True)
    file_path = f"data/pid_by_collection/{collection_name}.json"

    with open(file_path, "w", encoding="utf-8") as wf:
        json.dump(data, wf, ensure_ascii=False, indent=2)

    logging.info(f"Have saved {len(data)} PID into {file_path}")

def save_check_point(batch_num):
    os.makedirs("checkpoint", exist_ok=True)
    with open (f"product_enrichment/checkpoint/checkpoint_crawl_product_data.txt", "w", encoding='utf-8') as wf:
        wf.write(str(batch_num))
    logging.info(f"Have saved checkpoint {batch_num} for crawling product data")