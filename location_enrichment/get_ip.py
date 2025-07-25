from pymongo import MongoClient
from location_enrichment.save_files import save_ip_address
import os
from config.db_config import MONGO_URI
def get_ip_address():
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "xxxxxxxxxxxxxxx"
    url = MONGO_URI

    client = MongoClient(url, serverSelectionTimeoutMS=5000)
    database = client["glamira"]
    summary = database["summary"]

    pipeline = [
        {"$group": {"_id": "$ip"}},
        {"$project": {"ip": "$_id", "_id": 0}}
    ]
    cursor = summary.aggregate(pipeline, allowDiskUse=True)

    ip_data = [doc["ip"] for doc in cursor if doc.get("ip")]

    save_ip_address(ip_data)
