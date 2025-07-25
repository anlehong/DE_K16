import json
import os
import logging
import csv

def save_location(data, batch_num):
    os.makedirs("location_enrichment/data/location", exist_ok=True)
    with open (f"location_enrichment/data/location/loc_batch_{batch_num}.json", "w", encoding='utf-8') as wf:
        json.dump(data, wf, ensure_ascii=False, indent=2)
    logging.info(f"Have saved {len(data)} into data/location/loc_batch_{batch_num}.json")

def save_check_point(batch_num):
    os.makedirs("checkpoint", exist_ok=True)
    with open ("location_enrichment/checkpoint/checkpoint_ip_location.txt", "w", encoding='utf-8') as wf:
        wf.write(str(batch_num))
    logging.info(f"Have saved checkpoint {batch_num}")

def save_error(data):
    os.makedirs("location_enrichment/data/error", exist_ok=True)
    check_not_exist = not os.path.exists("location_enrichment/data/error/ip_error.csv")
    with open ("location_enrichment/data/error/ip_error.csv", "a", encoding='utf-8', newline="") as wf:
        writer = csv.writer(wf)
        if check_not_exist:
            writer.writerow(["ip_address", "error_message"])
        for ip, message in data:
            writer.writerow([ip, message])
    logging.info(f"Have saved {len(data)} into location_enrichment/data/error/ip_error.csv")

def save_ip_address(data):
    os.makedirs("location_enrichment/data", exist_ok=True)
    check_not_exist = not os.path.exists("location_enrichment/data/ip_address.csv")
    with open ("location_enrichment/data/ip_address.csv", "w", encoding='utf-8', newline='') as wf:
        writer = csv.writer(wf)
        if check_not_exist:
            writer.writerow(["ip_address"])
        for ip in data:
            writer.writerow([ip])
    logging.info(f"Have saved {len(data)} ip_address into location_enrichment/data/ip_address.csv")
