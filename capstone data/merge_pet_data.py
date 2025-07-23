import json
import pandas as pd

# Định nghĩa tên file đầu vào và đầu ra
activity_logs_file = 'activity_logs.json'
pet_profiles_file = 'pet_profiles.json'
output_merged_csv_file = 'merged_pet_data.csv'
# output_merged_json_file = 'merged_pet_data.json' # Nếu muốn lưu JSON

# 1. Đọc dữ liệu từ file JSON
try:
    with open(activity_logs_file, 'r', encoding='utf-8') as f:
        activity_data = json.load(f)
    print(f"Đã đọc {len(activity_data)} bản ghi từ {activity_logs_file}")

    with open(pet_profiles_file, 'r', encoding='utf-8') as f:
        # Pet profiles là định dạng JSON Lines, mỗi dòng là một JSON object
        pet_profile_data = [json.loads(line) for line in f]
    print(f"Đã đọc {len(pet_profile_data)} bản ghi từ {pet_profiles_file}")

except FileNotFoundError as e:
    print(f"Lỗi: Không tìm thấy file {e.filename}. Đảm bảo các file nằm cùng thư mục hoặc cung cấp đường dẫn đầy đủ.")
    exit()
except json.JSONDecodeError as e:
    print(f"Lỗi đọc JSON: {e}. Đảm bảo file JSON hợp lệ.")
    exit()
except Exception as e:
    print(f"Có lỗi xảy ra: {e}")
    exit()

# 2. Chuyển đổi danh sách dictionaries thành Pandas DataFrames
df_activity = pd.DataFrame(activity_data)
df_profile = pd.DataFrame(pet_profile_data)

print("\nSchema của activity_logs:")
print(df_activity.info())
print("\nSchema của pet_profiles:")
print(df_profile.info())

# *** PHẦN CHỈNH SỬA MỚI: Chuẩn hóa cột 'pet_id' về chữ thường ***
df_activity['pet_id'] = df_activity['pet_id'].str.lower()
df_profile['pet_id'] = df_profile['pet_id'].str.lower()
print("\nĐã chuẩn hóa cột 'pet_id' về chữ thường.")

# Kiểm tra lại các ID sau khi chuẩn hóa (tùy chọn)
print("\nSample pet_ids from activity_logs (sau khi chuẩn hóa):")
print(df_activity['pet_id'].value_counts().head())
print("\nSample pet_ids from pet_profiles (sau khi chuẩn hóa):")
print(df_profile['pet_id'].value_counts().head())


# 3. Gộp (Join) hai DataFrames
# 'inner' join: Chỉ giữ lại các bản ghi có pet_id tồn tại ở cả hai DataFrame
# Nếu muốn giữ tất cả bản ghi từ activity_logs và thêm thông tin profile nếu có, dùng how='left'
df_merged = pd.merge(df_activity, df_profile, on='pet_id', how='inner')

print(f"\nĐã gộp thành công {len(df_merged)} bản ghi.")
print("5 dòng đầu tiên của DataFrame đã gộp:")
print(df_merged.head())
print("\nCác cột trong DataFrame đã gộp:")
print(df_merged.columns.tolist())


# 4. Lưu DataFrame đã gộp vào file mới
# Lưu dưới dạng CSV
df_merged.to_csv(output_merged_csv_file, index=False, encoding='utf-8')
print(f"\nDữ liệu đã gộp được lưu thành công vào {output_merged_csv_file} (CSV).")

# Lưu dưới dạng JSON (nếu muốn giữ nguyên định dạng)
# df_merged.to_json(output_merged_json_file, orient='records', lines=True, force_ascii=False)
# print(f"Dữ liệu đã gộp được lưu thành công vào {output_merged_json_file} (JSON).")

print("Hoàn tất!")