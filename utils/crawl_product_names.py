import requests
import pandas as pd
import threading
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import cloudscraper
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

BATCH_SIZE = 500  # Kích thước batch khi đọc dữ liệu từ CSV
THREADS = 4  # Số luồng chạy song song
TEMP_INPUT_FILE = "test.csv"  # File chứa dữ liệu sản phẩm cần crawl
OUTPUT_FILE = "crawled_products.csv"  # File lưu kết quả
FAILED_FILE = "failed_crawls.json"  # File lưu các crawl bị lỗi
LOCK = threading.Lock()

def fetch_product_name(url):
    """Crawl tên sản phẩm từ URL bằng cloudscraper"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0'
        }
    
        time.sleep(random.uniform(2, 5))
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url, headers=headers)
        
        soup = BeautifulSoup(response.text, "html.parser")
        product_name = soup.find("h1", class_="page-title")
        
        return product_name.text.strip() if product_name else None
    except Exception as e:
        print(f"Lỗi khi crawl {url}: {e}")
        return None

def process_batch(batch):
    """Xử lý một batch crawl sản phẩm"""
    crawled_data = []
    failed_crawls = []

    for _, row in batch.iterrows():
        product_id = row["product_id"]
        url = row["current_url"]
        product_name = fetch_product_name(url)

        if product_name:
            crawled_data.append({"product_id": product_id, "product_name": product_name})
        else:
            failed_crawls.append({"product_id": product_id, "url": url})

    # Ghi kết quả vào file CSV
    with LOCK:
        pd.DataFrame(crawled_data).to_csv(OUTPUT_FILE, mode="a", header=False, index=False)
        if failed_crawls:
            with open(FAILED_FILE, "a") as f:
                json.dump(failed_crawls, f)
                f.write("\n")

def crawl_products():
    """Chia dữ liệu thành batch và crawl sản phẩm theo batch"""
    # Đọc file CSV chứa dữ liệu sản phẩm cần crawl
    df = pd.read_csv(TEMP_INPUT_FILE)
    
    # Ghi header nếu file output trống
    with LOCK:
        pd.DataFrame(columns=["product_id", "product_name"]).to_csv(OUTPUT_FILE, index=False)

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[i : i + BATCH_SIZE]  # Lấy batch từ dataframe
            executor.submit(process_batch, batch)

    print(f"Crawl hoàn tất! Kết quả lưu vào {OUTPUT_FILE}")
if __name__ == "__main__":

    url = "https://www.glamira.sk/zasnubne-prstene/diaman/"
    print(fetch_product_name(url))
