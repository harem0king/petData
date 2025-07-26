import sys
import re
from datetime import datetime, timedelta
import json

# Import các thư viện PySpark cần thiết
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, lit, abs, round, avg, when,
    lag, count, current_date, year, month, dayofmonth,
    concat_ws, collect_list, mean, stddev, date_format,
    to_date, datediff, udf
)
from pyspark.sql.types import (
    DoubleType, StringType, StructType, StructField,
    IntegerType, TimestampType, DateType, BooleanType
)
from pyspark.sql.window import Window

# Import các thư viện Python cơ bản
import pandas as pd
import numpy as np

# --- Cấu hình SparkSession ---
spark = SparkSession.builder \
    .appName("PetHealthAnalysisWithSpeciesSpecific") \
    .getOrCreate()

# --- Cấu hình đường dẫn cơ sở HDFS của bạn ---
hdfs_base_path = "hdfs:///user/ntl/capstone_data/"
output_path = "hdfs:///user/ntl/capstone_data/analyzed_data/"

# --- Cờ bật/tắt trực quan hóa ---
PLOT_AVAILABLE = False

print("SparkSession và các đường dẫn đã được cấu hình.")

# ====================================================================
# A. Định nghĩa Hằng số và Ngưỡng Species-Specific
# ====================================================================
print("\n--- A. Định nghĩa Hằng số và Ngưỡng Species-Specific ---")

# --- Ngưỡng Z-score chung cho bất thường ---
Z_SCORE_THRESHOLD = 2.0

# --- Định nghĩa các hằng số và ngưỡng riêng cho CHÓ ---
DOG_NORMAL_FOOD_AVG = 250.0
DOG_NORMAL_FOOD_STD = 50.0
DOG_NORMAL_SLEEP_AVG = 12.0
DOG_NORMAL_SLEEP_STD = 2.0
DOG_NORMAL_WEIGHT_AVG = 15.0
DOG_NORMAL_WEIGHT_STD = 5.0

DOG_FOOD_ANOMALY_SCORE = 1
DOG_SLEEP_ANOMALY_SCORE = 1
DOG_WEIGHT_ANOMALY_SCORE = 2
DOG_MEOW_ANOMALY_SCORE = 0
DOG_NOTE_STRESS_SCORE = 2
DOG_NOTE_ILLNESS_SCORE = 3

DOG_HEALTH_RED_THRESHOLD = 5
DOG_HEALTH_ORANGE_THRESHOLD = 3

# --- Định nghĩa các hằng số và ngưỡng riêng cho MÈO ---
CAT_NORMAL_FOOD_AVG = 180.0
CAT_NORMAL_FOOD_STD = 30.0
CAT_NORMAL_SLEEP_AVG = 15.0
CAT_NORMAL_SLEEP_STD = 3.0
CAT_NORMAL_WEIGHT_AVG = 4.0
CAT_NORMAL_WEIGHT_STD = 1.0

CAT_NORMAL_MEOW_AVG = 5.0
CAT_NORMAL_MEOW_STD = 3.0

CAT_FOOD_ANOMALY_SCORE = 1
CAT_SLEEP_ANOMALY_SCORE = 1
CAT_WEIGHT_ANOMALY_SCORE = 2
CAT_MEOW_ANOMALY_SCORE = 1
CAT_NOTE_STRESS_SCORE = 2
CAT_NOTE_ILLNESS_SCORE = 3

CAT_HEALTH_RED_THRESHOLD = 5
CAT_HEALTH_ORANGE_THRESHOLD = 3

# --- Các từ khóa NLP riêng cho từng loài ---
DOG_STRESS_KEYWORDS = ["sợ sấm", "phá phách", "sủa nhiều", "cắn phá", "run rẩy", "gầm gừ", "lo lắng", "bồn chồn", "chán ăn"]
DOG_ILLNESS_KEYWORDS = ["ho khan", "nôn", "khập khiễng", "chảy nước mũi", "ghẻ", "bọ chét", "đau chân", "tiêu chảy", "rụng lông"]
DOG_POSITIVE_KEYWORDS = ["vẫy đuôi", "chào đón", "chơi đùa", "nghe lời", "trông nhà", "chạy nhảy", "ăn khỏe", "vui vẻ"]

CAT_STRESS_KEYWORDS = ["lẩn trốn", "liếm lông quá mức", "cào cấu đồ đạc", "gầm gừ", "giấu mình", "tiểu tiện không đúng chỗ", "lo lắng", "bồn chồn", "chán ăn"]
CAT_ILLNESS_KEYWORDS = ["nôn ra lông", "ho khan", "nhiễm trùng mắt", "nấm da", "rụng lông", "biếng ăn", "chảy nước mắt", "đi tiểu ra máu", "tiêu chảy"]
CAT_POSITIVE_KEYWORDS = ["cuộn mình", "cọ người", "kêu gừ gừ", "săn chuột", "chơi với đồ chơi", "tắm nắng", "ăn tốt", "vui vẻ"]

def check_keywords(text, keywords_list):
    if text is None:
        return False
    text_lower = text.lower()
    for keyword in keywords_list:
        if keyword.lower() in text_lower:
            return True
    return False

print("Đã định nghĩa các hằng số và ngưỡng species-specific.")

# ====================================================================
# B. Đọc dữ liệu đã gộp từ file CSV của bạn
# ====================================================================
print("\n--- B. Đọc dữ liệu đã gộp từ file CSV: merged_pet_data.csv ---")

merged_csv_path = hdfs_base_path + "merged_pet_data.csv"

try:
    df_raw_data = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(merged_csv_path)

    if "date" in df_raw_data.columns:
        df_raw_data = df_raw_data.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    if "food_amount" in df_raw_data.columns:
        df_raw_data = df_raw_data.withColumn("food_amount", col("food_amount").cast(DoubleType()))
    if "sleep_hours" in df_raw_data.columns:
        df_raw_data = df_raw_data.withColumn("sleep_hours", col("sleep_hours").cast(DoubleType()))
    if "weight_kg" in df_raw_data.columns:
        df_raw_data = df_raw_data.withColumn("weight_kg", col("weight_kg").cast(DoubleType()))
    if "meow_count" in df_raw_data.columns:
        df_raw_data = df_raw_data.withColumn("meow_count", col("meow_count").cast(IntegerType()))

    print(f"Đã đọc {df_raw_data.count()} bản ghi từ file CSV đã gộp.")
    print("\nSchema của DataFrame dữ liệu thô:")
    df_raw_data.printSchema()
    print("\n5 dòng đầu tiên của DataFrame dữ liệu thô:")
    df_raw_data.show(5, truncate=False)

except Exception as e:
    print(f"LỖI: Không thể đọc file CSV đã gộp '{merged_csv_path}'. Vui lòng kiểm tra đường dẫn và file. Lỗi: {e}")
    spark.stop()
    sys.exit(1)

df_raw_data = df_raw_data.fillna(0, subset=["food_amount", "sleep_hours", "meow_count", "weight_kg"])
print("Đã xử lý giá trị thiếu cho các cột số trong df_raw_data.")

# ====================================================================
# C. Đọc và tích hợp Pet Profiles
# ====================================================================
print("\n--- C. Đọc và tích hợp Pet Profiles ---")

pet_profiles_raw = [
    '{"pet_id": "pet001", "name": "Charlene", "species": "dog", "breed": "Siamese", "gender": "female", "birth_year": 2022}',
    '{"pet_id": "pet002", "name": "Steven", "species": "cat", "breed": "Munchkin", "gender": "male", "birth_year": 2022}',
    '{"pet_id": "pet003", "name": "Angelica", "species": "dog", "breed": "Siamese", "gender": "male", "birth_year": 2020}',
    '{"pet_id": "pet004", "name": "Oscar", "species": "cat", "breed": "Bengal", "gender": "female", "birth_year": 2019}',
    '{"pet_id": "pet005", "name": "Michael", "species": "cat", "breed": "Persian", "gender": "female", "birth_year": 2018}',
    '{"pet_id": "pet006", "name": "Sandra", "species": "dog", "breed": "Siamese", "gender": "female", "birth_year": 2022}',
    '{"pet_id": "pet007", "name": "Nicholas", "species": "dog", "breed": "Persian", "gender": "female", "birth_year": 2018}',
    '{"pet_id": "pet008", "name": "Jessica", "species": "cat", "breed": "British Shorthair", "gender": "female", "birth_year": 2022}',
    '{"pet_id": "pet009", "name": "Justin", "species": "cat", "breed": "Persian", "gender": "male", "birth_year": 2015}',
    '{"pet_id": "pet010", "name": "Chris", "species": "dog", "breed": "British Shorthair", "gender": "male", "birth_year": 2021}',
    '{"pet_id": "pet011", "name": "Lisa", "species": "dog", "breed": "Siamese", "gender": "female", "birth_year": 2020}',
    '{"pet_id": "pet012", "name": "Jessica", "species": "dog", "breed": "Bengal", "gender": "male", "birth_year": 2019}',
    '{"pet_id": "pet013", "name": "Adam", "species": "dog", "breed": "British Shorthair", "gender": "male", "birth_year": 2018}',
    '{"pet_id": "pet014", "name": "Kent", "species": "dog", "breed": "Munchkin", "gender": "female", "birth_year": 2016}',
    '{"pet_id": "pet015", "name": "Jose", "species": "dog", "breed": "Munchkin", "gender": "female", "birth_year": 2016}',
    '{"pet_id": "pet016", "name": "Mark", "species": "cat", "breed": "British Shorthair", "gender": "male", "birth_year": 2019}',
    '{"pet_id": "pet017", "name": "Tracy", "species": "cat", "breed": "Siamese", "gender": "male", "birth_year": 2017}',
    '{"pet_id": "pet018", "name": "Amy", "species": "cat", "breed": "Bengal", "gender": "female", "birth_year": 2016}',
    '{"pet_id": "pet019", "name": "Mark", "species": "cat", "breed": "Munchkin", "gender": "female", "birth_year": 2020}',
    '{"pet_id": "pet020", "name": "Veronica", "species": "cat", "breed": "Munchkin", "gender": "male", "birth_year": 2022}',
    '{"pet_id": "pet021", "name": "Nathan", "species": "cat", "breed": "British Shorthair", "gender": "male", "birth_year": 2021}',
    '{"pet_id": "pet022", "name": "William", "species": "dog", "breed": "Siamese", "gender": "male", "birth_year": 2021}',
    '{"pet_id": "pet023", "name": "Stephen", "species": "dog", "breed": "Persian", "gender": "female", "birth_year": 2020}',
    '{"pet_id": "pet024", "name": "Samuel", "species": "cat", "breed": "British Shorthair", "gender": "male", "birth_year": 2019}',
    '{"pet_id": "pet025", "name": "Matthew", "species": "dog", "breed": "Munchkin", "gender": "male", "birth_year": 2020}',
    '{"pet_id": "pet026", "name": "Jennifer", "species": "dog", "breed": "British Shorthair", "gender": "female", "birth_year": 2016}',
    '{"pet_id": "pet027", "name": "Ashley", "species": "cat", "breed": "Persian", "gender": "female", "birth_year": 2016}',
    '{"pet_id": "pet028", "name": "Deborah", "species": "cat", "breed": "Bengal", "gender": "female", "birth_year": 2019}',
    '{"pet_id": "pet029", "name": "Zachary", "species": "cat", "breed": "Siamese", "gender": "male", "birth_year": 2020}',
    '{"pet_id": "pet030", "name": "Melinda", "species": "dog", "breed": "Persian", "gender": "male", "birth_year": 2016}',
    '{"pet_id": "pet031", "name": "Joann", "species": "cat", "breed": "Munchkin", "gender": "female", "birth_year": 2022}',
    '{"pet_id": "pet032", "name": "Paul", "species": "dog", "breed": "Munchkin", "gender": "female", "birth_year": 2019}',
    '{"pet_id": "pet033", "name": "Linda", "species": "cat", "breed": "Persian", "gender": "female", "birth_year": 2022}',
    '{"pet_id": "pet034", "name": "Michael", "species": "dog", "breed": "British Shorthair", "gender": "male", "birth_year": 2022}',
    '{"pet_id": "pet035", "name": "Tabitha", "species": "cat", "breed": "Siamese", "gender": "female", "birth_year": 2016}',
    '{"pet_id": "pet036", "name": "April", "species": "cat", "breed": "Munchkin", "gender": "female", "birth_year": 2022}',
    '{"pet_id": "pet037", "name": "Johnny", "species": "cat", "breed": "Bengal", "gender": "male", "birth_year": 2019}',
    '{"pet_id": "pet038", "name": "Kayla", "species": "dog", "breed": "Persian", "gender": "female", "birth_year": 2021}',
    '{"pet_id": "pet039", "name": "Melissa", "species": "dog", "breed": "British Shorthair", "gender": "male", "birth_year": 2022}',
    '{"pet_id": "pet040", "name": "Ryan", "species": "cat", "breed": "Bengal", "gender": "female", "birth_year": 2018}',
    '{"pet_id": "pet041", "name": "Cynthia", "species": "cat", "breed": "Bengal", "gender": "male", "birth_year": 2021}',
    '{"pet_id": "pet042", "name": "Charles", "species": "cat", "breed": "British Shorthair", "gender": "male", "birth_year": 2016}',
    '{"pet_id": "pet043", "name": "Gregory", "species": "dog", "breed": "Munchkin", "gender": "male", "birth_year": 2020}',
    '{"pet_id": "pet044", "name": "Jorge", "species": "dog", "breed": "British Shorthair", "gender": "male", "birth_year": 2015}',
    '{"pet_id": "pet045", "name": "Robert", "species": "dog", "breed": "Munchkin", "gender": "male", "birth_year": 2015}',
    '{"pet_id": "pet046", "name": "Nicholas", "species": "cat", "breed": "Bengal", "gender": "female", "birth_year": 2020}',
    '{"pet_id": "pet047", "name": "Julie", "species": "cat", "breed": "British Shorthair", "gender": "male", "birth_year": 2019}',
    '{"pet_id": "pet048", "name": "Jason", "species": "dog", "breed": "Munchkin", "gender": "female", "birth_year": 2022}',
    '{"pet_id": "pet049", "name": "Maria", "species": "cat", "breed": "Siamese", "gender": "male", "birth_year": 2022}',
    '{"pet_id": "pet050", "name": "Alyssa", "species": "cat", "breed": "Persian", "gender": "male", "birth_year": 2015}'
]
pet_profiles_data = [json.loads(line) for line in pet_profiles_raw]

pet_profile_schema = StructType([
    StructField("pet_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("species", StringType(), True),
    StructField("breed", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birth_year", IntegerType(), True)
])

df_pet_profiles = spark.createDataFrame(pet_profiles_data, schema=pet_profile_schema)
print("Đã đọc thông tin Pet Profiles.")

# CHỈNH SỬA TẠI ĐÂY: Bỏ cột 'species', 'name' và 'breed' từ df_raw_data để tránh xung đột tên cột khi join
df_with_profiles = df_raw_data.drop("species", "name", "breed").join(
    df_pet_profiles.select("pet_id", "species", "name", "breed"),
    on="pet_id",
    how="left"
)
print("Đã join dữ liệu sức khỏe với Pet Profiles.")
df_with_profiles.show(5, truncate=False)

# ====================================================================
# D. Đọc và tích hợp Owner Notes
# ====================================================================
print("\n--- D. Đọc và tích hợp Owner Notes ---")

owner_notes_path = hdfs_base_path + "owner_notes_generated.md"

try:
    notes_rdd = spark.sparkContext.textFile(owner_notes_path)
    notes_raw_content = notes_rdd.collect()
    full_notes_text = "\n".join(notes_raw_content)

    records = re.findall(r"# Pet ID: (pet\d+) - Notes on (\d{4}-\d{2}-\d{2})\n(.*?)(?=# Pet ID:|\Z)", full_notes_text, re.DOTALL)

    notes_data = []
    for pet_id, date_str, note_text_raw in records:
        notes_data.append({
            "pet_id": pet_id.strip(),
            "date": date_str.strip(),
            "note_text": note_text_raw.strip()
        })

    df_notes = spark.createDataFrame(notes_data)
    df_notes = df_notes.withColumn("date", col("date").cast(DateType()))
    print(f"Đã đọc {df_notes.count()} bản ghi từ owner_notes_generated.md.")
    df_notes.show(5, truncate=False)

except Exception as e:
    print(f"LỖI: Không thể đọc hoặc xử lý owner_notes_generated.md '{owner_notes_path}'. Lỗi: {e}")
    df_notes = spark.createDataFrame([], StructType([
        StructField("pet_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("note_text", StringType(), True)
    ]))
    print("Tiếp tục chạy mà không có dữ liệu owner_notes.")

df_combined_data = df_with_profiles.join(
    df_notes,
    on=["pet_id", "date"],
    how="left_outer"
)

df_combined_data = df_combined_data.na.fill("Không có ghi chú", subset=["note_text"])
print("Đã join dữ liệu sức khỏe với Owner Notes.")
df_combined_data.show(5, truncate=False)

# ====================================================================
# E. Tách dữ liệu thành Chó và Mèo
# ====================================================================
print("\n--- E. Tách dữ liệu thành Chó và Mèo ---")
df_dogs = df_combined_data.filter(col("species") == "dog")
df_cats = df_combined_data.filter(col("species") == "cat")

print(f"Số lượng bản ghi cho chó: {df_dogs.count()}")
print(f"Số lượng bản ghi cho mèo: {df_cats.count()}")

# ====================================================================
# F. Hàm phân tích dữ liệu Species-Specific
# ====================================================================
print("\n--- F. Áp dụng phân tích Species-Specific ---")

def analyze_species_data(df_species, species_type):
    print(f"Bắt đầu phân tích cho loài: {species_type.upper()}")

    if species_type == "dog":
        normal_food_avg, normal_food_std = DOG_NORMAL_FOOD_AVG, DOG_NORMAL_FOOD_STD
        normal_sleep_avg, normal_sleep_std = DOG_NORMAL_SLEEP_AVG, DOG_NORMAL_SLEEP_STD
        normal_weight_avg, normal_weight_std = DOG_NORMAL_WEIGHT_AVG, DOG_NORMAL_WEIGHT_STD
        z_score_threshold = Z_SCORE_THRESHOLD
        food_anomaly_score = DOG_FOOD_ANOMALY_SCORE
        sleep_anomaly_score = DOG_SLEEP_ANOMALY_SCORE
        weight_anomaly_score = DOG_WEIGHT_ANOMALY_SCORE
        meow_anomaly_score = DOG_MEOW_ANOMALY_SCORE
        note_stress_score = DOG_NOTE_STRESS_SCORE
        note_illness_score = DOG_NOTE_ILLNESS_SCORE
        stress_keywords = DOG_STRESS_KEYWORDS
        illness_keywords = DOG_ILLNESS_KEYWORDS
        health_red_threshold = DOG_HEALTH_RED_THRESHOLD
        health_orange_threshold = DOG_HEALTH_ORANGE_THRESHOLD
        has_meow_count = False

    else:
        normal_food_avg, normal_food_std = CAT_NORMAL_FOOD_AVG, CAT_NORMAL_FOOD_STD
        normal_sleep_avg, normal_sleep_std = CAT_NORMAL_SLEEP_AVG, CAT_NORMAL_SLEEP_STD
        normal_weight_avg, normal_weight_std = CAT_NORMAL_WEIGHT_AVG, CAT_NORMAL_WEIGHT_STD
        normal_meow_avg, normal_meow_std = CAT_NORMAL_MEOW_AVG, CAT_NORMAL_MEOW_STD
        z_score_threshold = Z_SCORE_THRESHOLD
        food_anomaly_score = CAT_FOOD_ANOMALY_SCORE
        sleep_anomaly_score = CAT_SLEEP_ANOMALY_SCORE
        weight_anomaly_score = CAT_WEIGHT_ANOMALY_SCORE
        meow_anomaly_score = CAT_MEOW_ANOMALY_SCORE
        note_stress_score = CAT_NOTE_STRESS_SCORE
        note_illness_score = CAT_NOTE_ILLNESS_SCORE
        stress_keywords = CAT_STRESS_KEYWORDS
        illness_keywords = CAT_ILLNESS_KEYWORDS
        health_red_threshold = CAT_HEALTH_RED_THRESHOLD
        health_orange_threshold = CAT_HEALTH_ORANGE_THRESHOLD
        has_meow_count = True

    window_spec = Window.partitionBy("pet_id").orderBy("date")

    df_analyzed = df_species.withColumn("food_zscore", (col("food_amount") - normal_food_avg) / normal_food_std) \
                            .withColumn("sleep_zscore", (col("sleep_hours") - normal_sleep_avg) / normal_sleep_std) \
                            .withColumn("weight_zscore", (col("weight_kg") - normal_weight_avg) / normal_weight_std)

    df_analyzed = df_analyzed.withColumn("is_food_anomaly", abs(col("food_zscore")) > z_score_threshold) \
                             .withColumn("is_sleep_anomaly", abs(col("sleep_zscore")) > z_score_threshold) \
                             .withColumn("is_weight_anomaly", abs(col("weight_zscore")) > z_score_threshold)

    if has_meow_count:
        df_analyzed = df_analyzed.withColumn("meow_zscore", (col("meow_count") - normal_meow_avg) / normal_meow_std) \
                                 .withColumn("is_meow_anomaly", abs(col("meow_zscore")) > z_score_threshold)
    else:
        df_analyzed = df_analyzed.withColumn("meow_zscore", lit(None).cast(DoubleType())) \
                                 .withColumn("is_meow_anomaly", lit(False))

    udf_check_stress_species = udf(lambda text: check_keywords(text, stress_keywords), BooleanType())
    udf_check_illness_species = udf(lambda text: check_keywords(text, illness_keywords), BooleanType())

    df_analyzed = df_analyzed.withColumn("note_is_stressful", udf_check_stress_species(col("note_text"))) \
                             .withColumn("note_is_illness_related", udf_check_illness_species(col("note_text")))

    df_analyzed = df_analyzed.withColumn("risk_score", lit(0).cast(IntegerType()))

    df_analyzed = df_analyzed.withColumn(
        "risk_score",
        col("risk_score") + when(col("is_food_anomaly"), food_anomaly_score).otherwise(0) + \
                          when(col("is_sleep_anomaly"), sleep_anomaly_score).otherwise(0) + \
                          when(col("is_weight_anomaly"), weight_anomaly_score).otherwise(0)
    )

    if has_meow_count:
        df_analyzed = df_analyzed.withColumn("risk_score", col("risk_score") + when(col("is_meow_anomaly"), meow_anomaly_score).otherwise(0))
    else:
        df_analyzed = df_analyzed.withColumn("risk_score", col("risk_score"))

    df_analyzed = df_analyzed.withColumn(
        "risk_score",
        col("risk_score") + when(col("note_is_stressful"), note_stress_score).otherwise(0) + \
                          when(col("note_is_illness_related"), note_illness_score).otherwise(0)
    )

    df_final_species = df_analyzed.withColumn(
        "health_alert_level",
        when(col("risk_score") >= health_red_threshold, "Đỏ (Nguy cơ cao)")
        .when(col("risk_score") >= health_orange_threshold, "Cam (Cảnh báo)")
        .when(col("risk_score") >= 1, "Vàng (Theo dõi)")
        .otherwise("Xanh (Bình thường)")
    )
    print(f"Hoàn thành phân tích cho loài: {species_type.upper()}")
    return df_final_species

df_dogs_analyzed = analyze_species_data(df_dogs, "dog")
df_cats_analyzed = analyze_species_data(df_cats, "cat")

print("\n--- Kết quả phân tích sơ bộ cho CHÓ ---")
df_dogs_analyzed.select("pet_id", "date", "name", "species", "food_amount", "is_food_anomaly",
                        "sleep_hours", "is_sleep_anomaly", "weight_kg", "is_weight_anomaly",
                        "meow_count", "is_meow_anomaly", "note_is_stressful", "note_is_illness_related",
                        "risk_score", "health_alert_level").show(10, truncate=False)

print("\n--- Kết quả phân tích sơ bộ cho MÈO ---")
df_cats_analyzed.select("pet_id", "date", "name", "species", "food_amount", "is_food_anomaly",
                        "sleep_hours", "is_sleep_anomaly", "weight_kg", "is_weight_anomaly",
                        "meow_count", "is_meow_anomaly", "note_is_stressful", "note_is_illness_related",
                        "risk_score", "health_alert_level").show(10, truncate=False)

# ====================================================================
# G. Tổng hợp báo cáo cảnh báo sức khỏe (Health Alerts Report)
# ====================================================================
print("\n--- G. Tổng hợp báo cáo cảnh báo sức khỏe cuối cùng ---")

df_final_report = df_dogs_analyzed.unionAll(df_cats_analyzed)

df_health_alerts = df_final_report.filter(col("risk_score") > 0) \
                                 .select("pet_id", "name", "species", "breed", "date",
                                         "risk_score", "health_alert_level",
                                         "food_amount", "is_food_anomaly",
                                         "sleep_hours", "is_sleep_anomaly",
                                         "weight_kg", "is_weight_anomaly",
                                         "meow_count", "is_meow_anomaly",
                                         "note_text", "note_is_stressful", "note_is_illness_related") \
                                 .orderBy("date", "risk_score", ascending=False)

print(f"Số lượng bản ghi cảnh báo sức khỏe được tạo: {df_health_alerts.count()}")
if df_health_alerts.count() > 0:
    print("\n5 bản ghi cảnh báo sức khỏe hàng đầu (tổng hợp):")
    df_health_alerts.show(5, truncate=False)

# ====================================================================
# H. Lưu kết quả ra HDFS
# ====================================================================
print("\n--- H. Lưu kết quả ra HDFS ---")

output_alerts_path = output_path + "pet_health_alerts_final.csv"

try:
    spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).delete(
        spark._jvm.org.apache.hadoop.fs.Path(output_alerts_path), True
    )
    print(f"Đã xóa thư mục output cũ: {output_alerts_path}")
except Exception as e:
    print(f"Không thể xóa thư mục output cũ (có thể chưa tồn tại): {e}")

df_health_alerts.write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(output_alerts_path)

print(f"Báo cáo cảnh báo sức khỏe cuối cùng đã được lưu thành công tại: {output_alerts_path}")

# ====================================================================
# I. Dừng SparkSession
# ====================================================================
spark.stop()
print("\nSparkSession đã được dừng.")
