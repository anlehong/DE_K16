import json

INPUT_FILE = "product_data_temp.json"

# Đọc dữ liệu từ JSON
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

# Đếm số lượng product_id
num_products = len(data)
print(f"Tổng số product_id: {num_products}")
