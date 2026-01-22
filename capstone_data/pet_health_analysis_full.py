import sys
import re
from datetime import datetime, timedelta
import json

# Libs cho Spark & ML
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
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# --- 1. Init Spark & Config ---
# Đặt AppName rõ ràng để dễ debug trên Spark UI
spark = SparkSession.builder \
    .appName("PetHealth_ETL_Pipeline") \
    .getOrCreate()

# Path HDFS (Lưu ý: đổi path nếu chạy môi trường khác)
hdfs_base_path = "hdfs:///user/ntl/capstone_data/"
output_path = "hdfs:///user/ntl/capstone_data/analyzed_data/"

print(">>> Spark Session initialized.")

# --- 2. Config ngưỡng sức khỏe (Thresholds) ---
# Ngưỡng Z-score > 2.0 là bất thường (theo phân phối chuẩn)
Z_SCORE_THRESHOLD = 2.0

# Ngưỡng cho CHÓ
DOG_NORMAL_FOOD_AVG = 250.0
DOG_NORMAL_FOOD_STD = 50.0
DOG_NORMAL_SLEEP_AVG = 12.0
DOG_NORMAL_SLEEP_STD = 2.0
DOG_NORMAL_WEIGHT_AVG = 15.0
DOG_NORMAL_WEIGHT_STD = 5.0

# Trọng số điểm (Weighting) - Cân nặng quan trọng hơn hành vi
DOG_FOOD_ANOMALY_SCORE = 1
DOG_SLEEP_ANOMALY_SCORE = 1
DOG_WEIGHT_ANOMALY_SCORE = 2 
DOG_MEOW_ANOMALY_SCORE = 0   # Chó ko tính meow
DOG_NOTE_STRESS_SCORE = 2
DOG_NOTE_ILLNESS_SCORE = 3

DOG_HEALTH_RED_THRESHOLD = 5
DOG_HEALTH_ORANGE_THRESHOLD = 3

# Ngưỡng cho MÈO
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

# Keywords NLP (Simple matching)
DOG_STRESS_KEYWORDS = ["sợ sấm", "phá phách", "sủa nhiều", "cắn phá", "run rẩy", "gầm gừ", "lo lắng", "bồn chồn", "chán ăn"]
DOG_ILLNESS_KEYWORDS = ["ho khan", "nôn", "khập khiễng", "chảy nước mũi", "ghẻ", "bọ chét", "đau chân", "tiêu chảy", "rụng lông"]

CAT_STRESS_KEYWORDS = ["lẩn trốn", "liếm lông quá mức", "cào cấu đồ đạc", "gầm gừ", "giấu mình", "tiểu tiện không đúng chỗ", "lo lắng", "bồn chồn", "chán ăn"]
CAT_ILLNESS_KEYWORDS = ["nôn ra lông", "ho khan", "nhiễm trùng mắt", "nấm da", "rụng lông", "biếng ăn", "chảy nước mắt", "đi tiểu ra máu", "tiêu chảy"]

# Helper check keyword
def check_keywords(text, keywords_list):
    if text is None: return False
    text_lower = text.lower()
    for keyword in keywords_list:
        if keyword.lower() in text_lower:
            return True
    return False

# --- 3. Load Data: Log hoạt động (CSV) ---
print(">>> Reading CSV Logs...")
merged_csv_path = hdfs_base_path + "merged_pet_data.csv"

try:
    # Dùng inferSchema cho tiện, production nên define schema cứng để nhanh hơn
    df_raw_data = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(merged_csv_path)
    
    # Cast type tường minh để tránh lỗi tính toán cộng trừ sau này
    if "date" in df_raw_data.columns:
        df_raw_data = df_raw_data.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    
    cols_to_cast = ["food_amount", "sleep_hours", "weight_kg"]
    for c in cols_to_cast:
        if c in df_raw_data.columns:
            df_raw_data = df_raw_data.withColumn(c, col(c).cast(DoubleType()))
            
    if "meow_count" in df_raw_data.columns:
        df_raw_data = df_raw_data.withColumn("meow_count", col("meow_count").cast(IntegerType()))

    print(f"Loaded {df_raw_data.count()} rows.")

except Exception as e:
    print(f"!!! Error reading CSV: {e}")
    spark.stop()
    sys.exit(1)

# Handle Nulls: Fill 0 để tránh crash khi tính Z-score
df_raw_data = df_raw_data.fillna(0, subset=["food_amount", "sleep_hours", "meow_count", "weight_kg"])

# --- 4. Load Data: Pet Profiles (JSON) ---
print(">>> Reading Profiles (JSON)...")

# Data giả lập (thực tế load từ file)
pet_profiles_raw = [
    '{"pet_id": "pet001", "name": "Charlene", "species": "dog", "breed": "Siamese", "gender": "female", "birth_year": 2022}',
    '{"pet_id": "pet002", "name": "Steven", "species": "cat", "breed": "Munchkin", "gender": "male", "birth_year": 2022}',
    # ... (giữ nguyên data mẫu)
    '{"pet_id": "pet050", "name": "Alyssa", "species": "cat", "breed": "Persian", "gender": "male", "birth_year": 2015}'
]
pet_profiles_data = [json.loads(line) for line in pet_profiles_raw]

# Define Schema trước (Schema-on-Read) để tối ưu performance
pet_profile_schema = StructType([
    StructField("pet_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("species", StringType(), True), # Quan trọng để phân loại Chó/Mèo
    StructField("breed", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birth_year", IntegerType(), True)
])

df_pet_profiles = spark.createDataFrame(pet_profiles_data, schema=pet_profile_schema)

# Join logs với profile. Dùng LEFT JOIN để giữ lại log kể cả khi thiếu profile
df_with_profiles = df_raw_data.drop("species", "name", "breed").join(
    df_pet_profiles.select("pet_id", "species", "name", "breed"),
    on="pet_id",
    how="left"
)

# --- 5. Load Data: Owner Notes (Unstructured Text) ---
print(">>> Parsing Owner Notes...")
owner_notes_path = hdfs_base_path + "owner_notes_generated.md"

try:
    # Đọc file text raw
    notes_rdd = spark.sparkContext.textFile(owner_notes_path)
    notes_raw_content = notes_rdd.collect()
    full_notes_text = "\n".join(notes_raw_content)

    # Parsing logic: Dùng Regex trích xuất Date & Content
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

except Exception as e:
    print(f"!!! Warning: Could not read notes file. {e}")
    # Fallback schema phòng khi file lỗi
    df_notes = spark.createDataFrame([], StructType([
        StructField("pet_id", StringType(), True),
        StructField("date", DateType(), True),
        StructField("note_text", StringType(), True)
    ]))

# Join Notes vào Main Data
df_combined_data = df_with_profiles.join(
    df_notes,
    on=["pet_id", "date"],
    how="left_outer"
)

# --- 6. Tách luồng Chó/Mèo ---
# Tách ra vì logic tính toán sức khỏe khác nhau
df_dogs = df_combined_data.filter(col("species") == "dog")
df_cats = df_combined_data.filter(col("species") == "cat")

# --- 7. Hàm xử lý chính (Core Logic) ---
def analyze_species_data(df_species, species_type):
    print(f"Processing: {species_type.upper()}")

    # Load config theo loài
    if species_type == "dog":
        normal_food_avg, normal_food_std = DOG_NORMAL_FOOD_AVG, DOG_NORMAL_FOOD_STD
        normal_sleep_avg, normal_sleep_std = DOG_NORMAL_SLEEP_AVG, DOG_NORMAL_SLEEP_STD
        normal_weight_avg, normal_weight_std = DOG_NORMAL_WEIGHT_AVG, DOG_NORMAL_WEIGHT_STD
        
        food_score = DOG_FOOD_ANOMALY_SCORE
        sleep_score = DOG_SLEEP_ANOMALY_SCORE
        weight_score = DOG_WEIGHT_ANOMALY_SCORE
        meow_score = DOG_MEOW_ANOMALY_SCORE
        stress_score = DOG_NOTE_STRESS_SCORE
        illness_score = DOG_NOTE_ILLNESS_SCORE
        
        stress_kw = DOG_STRESS_KEYWORDS
        illness_kw = DOG_ILLNESS_KEYWORDS
        
        red_thresh = DOG_HEALTH_RED_THRESHOLD
        orange_thresh = DOG_HEALTH_ORANGE_THRESHOLD
        has_meow = False

    else: # CAT
        normal_food_avg, normal_food_std = CAT_NORMAL_FOOD_AVG, CAT_NORMAL_FOOD_STD
        normal_sleep_avg, normal_sleep_std = CAT_NORMAL_SLEEP_AVG, CAT_NORMAL_SLEEP_STD
        normal_weight_avg, normal_weight_std = CAT_NORMAL_WEIGHT_AVG, CAT_NORMAL_WEIGHT_STD
        normal_meow_avg, normal_meow_std = CAT_NORMAL_MEOW_AVG, CAT_NORMAL_MEOW_STD
        
        food_score = CAT_FOOD_ANOMALY_SCORE
        sleep_score = CAT_SLEEP_ANOMALY_SCORE
        weight_score = CAT_WEIGHT_ANOMALY_SCORE
        meow_score = CAT_MEOW_ANOMALY_SCORE
        stress_score = CAT_NOTE_STRESS_SCORE
        illness_score = CAT_NOTE_ILLNESS_SCORE
        
        stress_kw = CAT_STRESS_KEYWORDS
        illness_kw = CAT_ILLNESS_KEYWORDS
        
        red_thresh = CAT_HEALTH_RED_THRESHOLD
        orange_thresh = CAT_HEALTH_ORANGE_THRESHOLD
        has_meow = True

    # 1. Tính Z-Score (Deviation check)
    df_analyzed = df_species.withColumn("food_zscore", (col("food_amount") - normal_food_avg) / normal_food_std) \
                            .withColumn("sleep_zscore", (col("sleep_hours") - normal_sleep_avg) / normal_sleep_std) \
                            .withColumn("weight_zscore", (col("weight_kg") - normal_weight_avg) / normal_weight_std)

    # 2. Flag Anomalies (True/False)
    df_analyzed = df_analyzed.withColumn("is_food_anomaly", abs(col("food_zscore")) > Z_SCORE_THRESHOLD) \
                             .withColumn("is_sleep_anomaly", abs(col("sleep_zscore")) > Z_SCORE_THRESHOLD) \
                             .withColumn("is_weight_anomaly", abs(col("weight_zscore")) > Z_SCORE_THRESHOLD)

    if has_meow:
        df_analyzed = df_analyzed.withColumn("meow_zscore", (col("meow_count") - normal_meow_avg) / normal_meow_std) \
                                 .withColumn("is_meow_anomaly", abs(col("meow_zscore")) > Z_SCORE_THRESHOLD)
    else:
        df_analyzed = df_analyzed.withColumn("meow_zscore", lit(None).cast(DoubleType())) \
                                 .withColumn("is_meow_anomaly", lit(False))

    # 3. NLP check (UDF)
    # Lưu ý: UDF có thể làm chậm job, cân nhắc dùng native functions nếu logic đơn giản
    udf_stress = udf(lambda text: check_keywords(text, stress_kw), BooleanType())
    udf_illness = udf(lambda text: check_keywords(text, illness_kw), BooleanType())

    df_analyzed = df_analyzed.withColumn("note_is_stressful", udf_stress(col("note_text"))) \
                             .withColumn("note_is_illness_related", udf_illness(col("note_text")))

    # 4. Tính điểm rủi ro (Risk Scoring)
    df_analyzed = df_analyzed.withColumn("risk_score", lit(0).cast(IntegerType()))

    df_analyzed = df_analyzed.withColumn(
        "risk_score",
        col("risk_score") + when(col("is_food_anomaly"), food_score).otherwise(0) + \
                          when(col("is_sleep_anomaly"), sleep_score).otherwise(0) + \
                          when(col("is_weight_anomaly"), weight_score).otherwise(0)
    )

    if has_meow:
        df_analyzed = df_analyzed.withColumn("risk_score", col("risk_score") + when(col("is_meow_anomaly"), meow_score).otherwise(0))

    df_analyzed = df_analyzed.withColumn(
        "risk_score",
        col("risk_score") + when(col("note_is_stressful"), stress_score).otherwise(0) + \
                          when(col("note_is_illness_related"), illness_score).otherwise(0)
    )

    # 5. Gán nhãn cảnh báo (Labeling)
    df_final = df_analyzed.withColumn(
        "health_alert_level",
        when(col("risk_score") >= red_thresh, "Đỏ (Nguy cơ cao)")
        .when(col("risk_score") >= orange_thresh, "Cam (Cảnh báo)")
        .when(col("risk_score") >= 1, "Vàng (Theo dõi)")
        .otherwise("Xanh (Bình thường)")
    )
    return df_final

df_dogs_analyzed = analyze_species_data(df_dogs, "dog")
df_cats_analyzed = analyze_species_data(df_cats, "cat")

# --- 8. Tổng hợp & Report ---
print(">>> Aggregating reports...")
df_final_report = df_dogs_analyzed.unionAll(df_cats_analyzed)

df_health_alerts = df_final_report.filter(col("risk_score") > 0) \
                                  .select("pet_id", "name", "species", "date", "risk_score", "health_alert_level") \
                                  .orderBy("date", "risk_score", ascending=False)

if df_health_alerts.count() > 0:
    print("Top 5 alerts:")
    df_health_alerts.show(5, truncate=False)

# --- 9. Machine Learning (Random Forest) ---
print(">>> ML Pipeline starting...")

# Gán nhãn số (Label encoding)
df_ml = df_final_report.withColumn(
    "label",
    when(col("health_alert_level") == "Đỏ (Nguy cơ cao)", 3.0)
    .when(col("health_alert_level") == "Cam (Cảnh báo)", 2.0)
    .when(col("health_alert_level") == "Vàng (Theo dõi)", 1.0)
    .otherwise(0.0)
).fillna(0, subset=["meow_count"])

# Prepare features
indexer_species = StringIndexer(inputCol="species", outputCol="species_index")
indexer_breed = StringIndexer(inputCol="breed", outputCol="breed_index")

cols = ["food_amount", "sleep_hours", "weight_kg", "meow_count", 
        "is_food_anomaly", "is_sleep_anomaly", "is_weight_anomaly", 
        "note_is_illness_related", "species_index", "breed_index"]

assembler = VectorAssembler(inputCols=cols, outputCol="features")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100)

pipeline = Pipeline(stages=[indexer_species, indexer_breed, assembler, rf])

# Train/Test Split
(training_data, test_data) = df_ml.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(training_data)
predictions = model.transform(test_data)

print(">>> ML Predictions:")
predictions.select("pet_id", "health_alert_level", "prediction").show(5)

# --- 10. Lưu trữ (Sink) ---
print(">>> Saving to HDFS...")
# Dùng mode overwrite để đảm bảo Idempotency (chạy lại ko bị duplicate)
df_health_alerts.write.mode("overwrite").option("header", "true").csv(output_path + "pet_health_alerts_final.csv")
predictions.select("pet_id", "date", "prediction").write.mode("overwrite").option("header", "true").csv(output_path + "ml_predictions.csv")

spark.stop()
print(">>> Done.")
