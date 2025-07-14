import configparser
import urllib.parse
from pymongo import MongoClient

# Đọc file cấu hình database.ini
config = configparser.ConfigParser()
config.read("config/database.ini")

# Lấy thông tin từ MongoDB section
print(config.sections())
MONGO_HOST = config["MongoDB"]["host"]
MONGO_PORT = int(config["MongoDB"]["port"])
MONGO_USER = config["MongoDB"]["username"]
MONGO_PASS = urllib.parse.quote_plus(config["MongoDB"]["password"])
DB_NAME = config["MongoDB"]["database"]
MAIN_COLLECTION = config["MongoDB"]["main_collection"]
LOCATION_COLLECTION = config["MongoDB"]["location_collection"]

# Kết nối MongoDB
MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authMechanism=SCRAM-SHA-1"
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
main_collection = db[MAIN_COLLECTION]
location_collection = db[LOCATION_COLLECTION]
