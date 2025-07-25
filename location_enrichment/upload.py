from pymongo import MongoClient
import os
import json
from tqdm import tqdm
import logging
from config.db_config import MONGO_URI

def upload_ip_location():
    url = MONGO_URI
    client = MongoClient(url, serverSelectionTimeoutMS=30000)
    database = client['glamira']
    collection = database['ip_location']

    folder_path = "location_enrichment/data/location"
    file_list = [f for f in os.listdir(folder_path) if f.endswith(".json")]

    for filename in tqdm(file_list, desc="Đang upload"):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, "r", encoding="utf-8") as rf:
                try:
                    data = json.load(rf)
                    if isinstance(data, list):
                        result = collection.insert_many(data)
                    else:
                        logging.info(f"{filename} does not have JSON")
                except Exception as e:
                    logging.exception(f"Error when reading file {filename}: {e}")

    logging.info(f"Have uploaded ip location into the new collection")
