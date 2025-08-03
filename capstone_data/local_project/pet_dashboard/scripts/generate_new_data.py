# Updated generate_new_data.py (Phiên bản mạnh mẽ hơn)
import pandas as pd
from datetime import datetime, timedelta
import os
import random

# Đã CẬP NHẬT đường dẫn đến file dữ liệu gốc trên hệ thống cục bộ của bạn
LOCAL_DATA_PATH = '/home/ntl/capstone_data/merged_pet_data.csv'

def generate_new_pet_data_for_all_existing_and_some_new(num_new_pets_per_run=2, target_total_pets=50):
    """
    Tạo hoặc cập nhật dữ liệu thú cưng:
    - Đảm bảo tất cả thú cưng hiện có đều có một bản ghi cho ngày hiện tại.
    - Thêm một số thú cưng mới nếu tổng số thú cưng ít hơn mục tiêu.
    """
    print(f"[{datetime.now()}] Bắt đầu sinh/cập nhật dữ liệu...")

    today = datetime.now().strftime('%Y-%m-%d')
    df_existing = pd.DataFrame(columns=['pet_id', 'date', 'food_amount', 'sleep_hours', 'meow_count', 'weight_kg', 'name', 'species', 'breed', 'gender', 'birth_year'])

    if os.path.exists(LOCAL_DATA_PATH):
        try:
            df_existing = pd.read_csv(LOCAL_DATA_PATH)
            # Chuyển đổi cột 'date' về datetime để xử lý tốt hơn
            df_existing['date'] = pd.to_datetime(df_existing['date'], errors='coerce')
            df_existing.dropna(subset=['date'], inplace=True)
            print(f"Đã đọc {len(df_existing)} bản ghi hiện có từ {LOCAL_DATA_PATH}.")
        except Exception as e:
            print(f"Lỗi khi đọc file CSV hiện có: {e}. Bắt đầu với DataFrame rỗng.")
            df_existing = pd.DataFrame(columns=['pet_id', 'date', 'food_amount', 'sleep_hours', 'meow_count', 'weight_kg', 'name', 'species', 'breed', 'gender', 'birth_year'])
    else:
        print(f"File dữ liệu {LOCAL_DATA_PATH} chưa tồn tại, tạo mới DataFrame rỗng.")

    unique_pets = df_existing['pet_id'].unique().tolist() if not df_existing.empty else []
    
    new_data_records = []

    # 1. Đảm bảo tất cả thú cưng hiện có đều có bản ghi cho ngày hôm nay
    for pet_id in unique_pets:
        # Kiểm tra xem pet_id này đã có dữ liệu cho ngày hôm nay chưa
        if not ((df_existing['pet_id'] == pet_id) & (df_existing['date'].dt.strftime('%Y-%m-%d') == today)).any():
            # Lấy thông tin cơ bản của thú cưng từ bản ghi gần nhất
            pet_info = df_existing[df_existing['pet_id'] == pet_id].sort_values('date', ascending=False).iloc[0]
            new_data_records.append({
                'pet_id': pet_id,
                'date': today,
                'food_amount': round(random.uniform(150, 300), 0),
                'sleep_hours': round(random.uniform(10, 18), 1),
                'meow_count': random.randint(0, 10),
                'weight_kg': round(random.uniform(2.5, 7.0), 1),
                'name': pet_info['name'],
                'species': pet_info['species'],
                'breed': pet_info['breed'],
                'gender': pet_info['gender'],
                'birth_year': int(pet_info['birth_year'])
            })
            # print(f"Generated new data for existing pet {pet_id} for {today}") # Bỏ comment để debug

    # 2. Thêm thú cưng mới nếu cần (cho đến khi đạt target_total_pets)
    current_total_pets = len(unique_pets)
    if current_total_pets < target_total_pets:
        pets_to_add = min(num_new_pets_per_run, target_total_pets - current_total_pets)
        for i in range(pets_to_add):
            # Tạo pet_id mới duy nhất
            new_pet_id_num = 0
            while f'pet{new_pet_id_num:03d}' in unique_pets:
                new_pet_id_num += 1
            new_pet_id = f'pet{new_pet_id_num:03d}'
            unique_pets.append(new_pet_id) # Thêm vào danh sách để tránh trùng lặp trong cùng một lần chạy

            species = random.choice(['dog', 'cat'])
            breed = random.choice(['Golden Retriever', 'Labrador', 'Poodle', 'German Shepherd']) if species == 'dog' else random.choice(['Siamese', 'Persian', 'Maine Coon', 'Sphynx'])
            gender = random.choice(['male', 'female'])
            birth_year = random.randint(2015, 2023)
            name = f'NewPet{new_pet_id_num}'
            
            new_data_records.append({
                'pet_id': new_pet_id,
                'date': today,
                'food_amount': round(random.uniform(150, 300), 0),
                'sleep_hours': round(random.uniform(10, 18), 1),
                'meow_count': random.randint(0, 10),
                'weight_kg': round(random.uniform(2.5, 7.0), 1),
                'name': name,
                'species': species,
                'breed': breed,
                'gender': gender,
                'birth_year': birth_year
            })
            # print(f"Generated new data for new pet {new_pet_id} for {today}") # Bỏ comment để debug

    df_new = pd.DataFrame(new_data_records)
    if not df_new.empty:
        df_new['date'] = pd.to_datetime(df_new['date'])
        
    # Nối dữ liệu mới vào dữ liệu hiện có và loại bỏ trùng lặp
    # Giữ lại bản ghi mới nhất cho cùng một pet_id và date
    df_combined = pd.concat([df_existing, df_new]).drop_duplicates(subset=['pet_id', 'date'], keep='last')
    
    # Sắp xếp lại và lưu
    df_combined = df_combined.sort_values(by=['pet_id', 'date']).reset_index(drop=True)

    df_combined.to_csv(LOCAL_DATA_PATH, index=False)
    print(f"Đã lưu {len(df_combined)} bản ghi vào {LOCAL_DATA_PATH}")
    print(f"[{datetime.now()}] Hoàn tất sinh/cập nhật dữ liệu.")

if __name__ == "__main__":
    generate_new_pet_data_for_all_existing_and_some_new()
