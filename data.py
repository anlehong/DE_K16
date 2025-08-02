import json

INPUT_FILE = "product_data_temp.json"
OUTPUT_FILE = "product_enrichment/data/product_data_clean_sorted.json"

# Đọc dữ liệu
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

# Xử lý từng product_id
for item in data:
    if "urls" in item and isinstance(item["urls"], list):
        # Khử trùng và sắp xếp
        unique_urls = sorted(set(item["urls"]))
        item["urls"] = unique_urls

# Ghi ra file mới
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"✅ Đã xử lý và lưu vào: {OUTPUT_FILE}")
