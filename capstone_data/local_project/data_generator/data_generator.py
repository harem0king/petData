import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

# --- Cấu hình Đường dẫn ---
HDFS_RAW_DATA_PATH = "/user/ntl/capstone_data/raw_data"
LOCAL_TEMP_PATH = "/home/ntl/capstone_data/local_project/data_generator/temp_generated_data" # Thư mục tạm cục bộ để lưu file trước khi đẩy lên HDFS

# Đảm bảo thư mục tạm thời cục bộ tồn tại
os.makedirs(LOCAL_TEMP_PATH, exist_ok=True)

# Các thông tin thú cưng cố định để sinh dữ liệu
pets_info = [
    {"pet_id": "pet001", "name": "Milo", "species": "Dog", "breed": "Golden Retriever"},
    {"pet_id": "pet002", "name": "Luna", "species": "Cat", "breed": "Bengal"},
    {"pet_id": "pet003", "name": "Buddy", "species": "Dog", "breed": "Poodle"},
    {"pet_id": "pet004", "name": "Cleo", "species": "Cat", "breed": "Siamese"},
    {"pet_id": "pet005", "name": "Max", "species": "Dog", "breed": "Labrador"},
    {"pet_id": "pet006", "name": "Lucy", "species": "Cat", "breed": "Persian"},
    {"pet_id": "pet007", "name": "Rocky", "species": "Dog", "breed": "German Shepherd"},
    {"pet_id": "pet008", "name": "Daisy", "species": "Cat", "breed": "Ragdoll"},
    {"pet_id": "pet009", "name": "Charlie", "species": "Dog", "breed": "Beagle"},
    {"pet_id": "pet010", "name": "Bella", "species": "Cat", "breed": "Sphynx"}
]

# --- Hàm sinh dữ liệu hoạt động hàng ngày ---
def generate_daily_activity(num_days=1, start_date=None):
    if start_date is None:
        start_date = datetime.now() - timedelta(days=num_days)

    all_daily_data = []
    for i in range(num_days):
        current_date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
        for pet in pets_info:
            # Giá trị cơ sở ngẫu nhiên hơn hoặc theo logic nhất định
            food_base = random.uniform(0.4, 0.6) if pet['species'] == 'Cat' else random.uniform(0.7, 1.2)
            sleep_base = random.uniform(13, 16) if pet['species'] == 'Cat' else random.uniform(10, 14)
            weight_base = random.uniform(3, 7) if pet['species'] == 'Cat' else random.uniform(15, 30)
            meow_base = random.randint(15, 30) if pet['species'] == 'Cat' else random.randint(3, 10)

            # Thêm ngẫu nhiên để giả lập biến động hàng ngày
            # Giả lập một số bất thường nhỏ cho mục đích test
            food_amount = max(0.05, food_base + random.uniform(-0.3, 0.3))
            sleep_hours = max(5, sleep_base + random.uniform(-4, 4))
            weight_kg = max(0.5, weight_base + random.uniform(-1.5, 1.5))
            meow_count = max(0, int(meow_base + random.uniform(-10, 20)))

            all_daily_data.append({
                "pet_id": pet['pet_id'],
                "name": pet['name'],
                "species": pet['species'],
                "breed": pet['breed'],
                "date": current_date,
                "food_amount_kg": round(food_amount, 2),
                "sleep_hours": round(sleep_hours, 2),
                "weight_kg": round(weight_kg, 2),
                "meow_count": meow_count
            })
    return pd.DataFrame(all_daily_data)

# --- Hàm sinh dữ liệu ghi chú chủ nuôi (rất đơn giản) ---
def generate_owner_notes(num_days=1, start_date=None):
    if start_date is None:
        start_date = datetime.now() - timedelta(days=num_days)

    all_notes_data = []
    stress_keywords = ["lo lắng", "stress", "sợ hãi", "nhút nhát", "khó ngủ", "căng thẳng"]
    illness_keywords = ["ốm", "bệnh", "tiêu chảy", "nôn", "ho", "sốt", "gãi nhiều", "rụng lông", "đau chân", "mệt mỏi"]
    benign_keywords = ["ăn ngon miệng", "ngủ sâu", "chơi đùa vui vẻ", "hoạt bát", "khỏe mạnh", "vui vẻ"]

    for i in range(num_days):
        current_date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
        for pet in pets_info:
            if random.random() < 0.35: # 35% khả năng có ghi chú
                note = []
                # Tăng cơ hội sinh ghi chú xấu để test cảnh báo
                if random.random() < 0.25: # 25% khả năng có ghi chú stress
                    note.append(random.choice(stress_keywords))
                if random.random() < 0.25: # 25% khả năng có ghi chú bệnh
                    note.append(random.choice(illness_keywords))
                if not note: # Nếu không có stress/bệnh, tạo ghi chú bình thường
                    note.append(random.choice(benign_keywords))

                all_notes_data.append({
                    "pet_id": pet['pet_id'],
                    "date": current_date,
                    "note_text": ", ".join(note)
                })
    return pd.DataFrame(all_notes_data)

# --- Logic chính để sinh và đẩy dữ liệu lên HDFS ---
def main():
    # Sinh dữ liệu cho ngày hôm qua
    print("Generating data for yesterday...")
    yesterday = datetime.now() - timedelta(days=1)
    daily_activity_df = generate_daily_activity(num_days=1, start_date=yesterday)
    owner_notes_df = generate_owner_notes(num_days=1, start_date=yesterday)

    # Lưu tạm ra file CSV cục bộ
    daily_activity_file = os.path.join(LOCAL_TEMP_PATH, "daily_activity.csv")
    owner_notes_file = os.path.join(LOCAL_TEMP_PATH, "owner_notes_generated.csv")

    daily_activity_df.to_csv(daily_activity_file, index=False)
    owner_notes_df.to_csv(owner_notes_file, index=False)

    print(f"Data generated and saved to: {daily_activity_file}, {owner_notes_file}")

    # Đẩy file lên HDFS (GHI ĐÈ file cũ nếu có)
    # LƯU Ý QUAN TRỌNG: Với mục đích demo ban đầu, OVERWRITE toàn bộ data file đầu vào là đơn giản nhất để Spark xử lý lại.
    # Trong hệ thống thực tế, bạn sẽ muốn APPEND dữ liệu mới vào HDFS và Spark sẽ đọc dữ liệu incremental.
    print(f"Uploading {daily_activity_file} to HDFS...")
    os.system(f"hdfs dfs -put -f {daily_activity_file} {HDFS_RAW_DATA_PATH}/daily_activity.csv")
    print(f"Uploading {owner_notes_file} to HDFS...")
    os.system(f"hdfs dfs -put -f {owner_notes_file} {HDFS_RAW_DATA_PATH}/owner_notes_generated.csv")

    print("Data upload to HDFS complete.")
    print(f"Bạn có thể kiểm tra trên HDFS UI: http://localhost:9870/explorer.html#{HDFS_RAW_DATA_PATH}")

if __name__ == "__main__":
    main()
