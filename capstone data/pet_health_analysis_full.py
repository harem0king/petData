from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, current_date, round, to_timestamp, regexp_extract, lower, when, lit, count, avg, sum
from pyspark.sql.types import IntegerType, StringType
import re # Thư viện regex chuẩn của Python, dùng để parse owner_notes


# 1. Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("PetHealthFullAnalysis") \
    .getOrCreate()

print("SparkSession đã được khởi tạo thành công.")

# Định nghĩa các đường dẫn tới các file trên HDFS
# Đảm bảo các đường dẫn này khớp với nơi em đã tải file lên
hdfs_base_path = "hdfs:///user/hduser/capstone_data/"

activity_logs_file = hdfs_base_path + "activity_logs.json"
pet_profiles_file = hdfs_base_path + "pet_profiles.json"
merged_pet_data_file = hdfs_base_path + "merged_pet_data.csv" # Dữ liệu đã gộp từ Pandas
feeder_log_file = hdfs_base_path + "feeder_device_log.txt"
owner_notes_file = hdfs_base_path + "owner_notes.md"
# metadata_file = hdfs_base_path + "metadata.json" # Tạm thời chưa dùng metadata.json này vì nội dung chỉ là liệt kê tên file.
# --- Bắt đầu đọc và tiền xử lý từng loại dữ liệu ---

# ==============================================================================
# A. Đọc và Tiền xử lý activity_logs và pet_profiles (hoặc dùng merged_pet_data.csv)
# ==============================================================================

print("\n--- Xử lý Dữ liệu Hoạt động và Hồ sơ thú cưng ---")

try:
    # Option 1: Đọc lại từ các file gốc và gộp (nếu em muốn đảm bảo quá trình gộp trên Spark)
    df_activity = spark.read.option("multiLine", "true").json(activity_logs_file)
    df_profile = spark.read.option("multiLine", "true").json(pet_profiles_file)

    # Chuẩn hóa pet_id về chữ thường cho cả hai DataFrames
    df_activity = df_activity.withColumn("pet_id", lower(col("pet_id")))
    df_profile = df_profile.withColumn("pet_id", lower(col("pet_id")))

    # Chuyển đổi cột 'date' trong activity_logs sang định dạng ngày tháng để gộp chính xác
    df_activity = df_activity.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd"))

    # Gộp (join) hai DataFrames:
    df_merged_base = df_activity.join(df_profile, on='pet_id', how='inner')
    print(f"Đã gộp thành công {df_merged_base.count()} bản ghi từ activity_logs và pet_profiles.")

    # Option 2: Hoặc đọc trực tiếp file merged_pet_data.csv nếu đã được xử lý tốt
    # df_merged_base = spark.read.option("header", "true").option("inferSchema", "true").csv(merged_pet_data_file)
    # df_merged_base = df_merged_base.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd")) # Đảm bảo cột date đúng kiểu
    # print(f"Đã đọc {df_merged_base.count()} bản ghi từ {merged_pet_data_file}")


    print("\nSchema của DataFrame cơ sở (đã gộp):")
    df_merged_base.printSchema()
    df_merged_base.show(5, truncate=False)

    # Tạo các đặc trưng ban đầu từ dữ liệu này
    # Ví dụ: Tính tuổi của thú cưng tại thời điểm log (approximate age)
    df_merged_base = df_merged_base.withColumn("current_year", year(col("date")))
    df_merged_base = df_merged_base.withColumn("pet_age_at_log", col("current_year") - col("birth_year"))
    df_merged_base = df_merged_base.drop("current_year") # Bỏ cột tạm

except Exception as e:
    print(f"Lỗi khi xử lý activity_logs hoặc pet_profiles: {e}")
    spark.stop()
    exit()

# ==============================================================================
# B. Xử lý feeder_device_log.txt (Dữ liệu bán cấu trúc/log)
# ==============================================================================

print("\n--- Xử lý Feeder Device Logs ---")

try:
    df_feeder_logs_raw = spark.read.text(feeder_log_file)
    # df_feeder_logs_raw.show(truncate=False)

    # Regex để parse các dòng log
    # Regex này phù hợp với định dạng: [INFO] 2025-06-25 08:03:21 - Pet Miu started eating.
    log_pattern = r"\[(INFO|WARN)\] (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - Pet (.*?) (.*)"

    df_parsed_feeder_logs = df_feeder_logs_raw.select(
        regexp_extract(col("value"), log_pattern, 2).alias("log_timestamp_str"),
        regexp_extract(col("value"), log_pattern, 1).alias("log_level"),
        regexp_extract(col("value"), log_pattern, 3).alias("pet_name_from_log"),
        regexp_extract(col("value"), log_pattern, 4).alias("log_message")
    )

    df_parsed_feeder_logs = df_parsed_feeder_logs.withColumn(
        "log_timestamp", to_timestamp(col("log_timestamp_str"), "yyyy-MM-dd HH:mm:ss")
    ).drop("log_timestamp_str")

    # Trích xuất food_dispensed (nếu có) và feeder_jammed
    df_parsed_feeder_logs = df_parsed_feeder_logs.withColumn(
        "food_dispensed_g",
        regexp_extract(col("log_message"), r"Food Dispensed: (\d+)g", 1).cast(IntegerType())
    )
    df_parsed_feeder_logs = df_parsed_feeder_logs.withColumn(
        "feeder_jammed_event",
        when(col("log_message").contains("Feeder Jammed"), True).otherwise(False)
    )
    df_parsed_feeder_logs = df_parsed_feeder_logs.withColumn(
        "unusual_behavior_detected",
        when(col("log_message").contains("Unusual behavior detected"), True).otherwise(False)
    )

    # Chuyển đổi tên thú cưng từ log thành pet_id để gộp
    # Bước này cần mapping pet_name_from_log với pet_id từ df_profile.
    # Đây là một bước cần dữ liệu profile đầy đủ để map tên pet sang pet_id
    # Tạm thời bỏ qua nếu không có mapping rõ ràng.
    # Hoặc nếu log chỉ dùng tên, em có thể JOIN df_profile để tìm pet_id
    # df_parsed_feeder_logs = df_parsed_feeder_logs.join(df_profile, col("pet_name_from_log") == col("name"), "left") \
    #                                             .select(df_parsed_feeder_logs["*"], df_profile["pet_id"].alias("mapped_pet_id"))

    # Để đơn giản, giả sử chúng ta có thể map tên pet trong log (Miu) với một pet_id nào đó nếu cần
    # Hoặc bỏ qua cột pet_name_from_log và chỉ tập trung vào các sự kiện food_dispensed/feeder_jammed
    
    # Do pet_profiles.json không có đủ tên để map, chúng ta sẽ gộp nó vào DataFrame chính bằng cách tạo một cột ngày cho nó.
    # Mỗi sự kiện log sẽ có một ngày. Chúng ta sẽ tổng hợp các sự kiện này theo ngày.
    df_feeder_summary_daily = df_parsed_feeder_logs \
        .withColumn("log_date", col("log_timestamp").cast(StringType()).substr(0, 10)) \
        .groupBy("log_date", "pet_name_from_log") \
        .agg(
            sum("food_dispensed_g").alias("daily_food_dispensed_g"),
            count(when(col("feeder_jammed_event") == True, True)).alias("daily_feeder_jams"),
            count(when(col("unusual_behavior_detected") == True, True)).alias("daily_unusual_behaviors")
        )

    # Chuẩn hóa pet_id từ tên (nếu có thể map được)
    # Hiện tại không có mapping rõ ràng từ "Miu" sang "petXXX" trong df_profile
    # Nếu trong owner_notes có nhắc đến pet_id và tên, thì có thể dùng để map.
    # Hoặc giả định "Miu" tương ứng với pet có pet_id thấp nhất hoặc một pet_id cụ thể.
    # Ví dụ: gán Miu = pet001 (chỉ là ví dụ tạm thời)
    df_feeder_summary_daily = df_feeder_summary_daily.withColumn(
        "pet_id", when(col("pet_name_from_log") == "Miu", lit("pet001")).otherwise(lit(None))
    )
    df_feeder_summary_daily = df_feeder_summary_daily.filter(col("pet_id").isNotNull()) # Loại bỏ các dòng không map được

    print("\nParsed and Summarized Feeder Logs Daily:")
    df_feeder_summary_daily.show(truncate=False)
    df_feeder_summary_daily.printSchema()

except Exception as e:
    print(f"Lỗi khi xử lý feeder_device_log.txt: {e}")
    # spark.stop() # Không dừng Spark ngay, tiếp tục với các phần khác
    # exit()

# ==============================================================================
# C. Xử lý owner_notes.md (Dữ liệu phi cấu trúc - Text)
# ==============================================================================

print("\n--- Xử lý Owner Notes (NLP) ---")

try:
    # Đọc toàn bộ file thành một chuỗi lớn để phân tách theo khối Markdown
    notes_content_rdd = spark.read.text(owner_notes_file).rdd.map(lambda row: row.value)
    full_notes_text = "\n".join(notes_content_rdd.collect())

    # Sử dụng regex để phân tách các ghi chú thành từng khối riêng biệt
    # Mỗi khối bắt đầu bằng "# Pet ID: PETxxx - Notes on YYYY-MM-DD"
    notes_blocks = re.split(r'(# Pet ID: (PET\d+) - Notes on (\d{4}-\d{2}-\d{2}))', full_notes_text)

    parsed_notes_list = []
    for i in range(1, len(notes_blocks), 4): # notes_blocks sẽ có (header, pet_id_match, date_match, content)
        if i+3 < len(notes_blocks):
            header_full = notes_blocks[i].strip()
            pet_id_raw = notes_blocks[i+1].strip()
            note_date_str = notes_blocks[i+2].strip()
            note_content = notes_blocks[i+3].strip()

            parsed_notes_list.append({
                "pet_id_raw": pet_id_raw,
                "note_date_str": note_date_str,
                "note_content": note_content
            })

    if not parsed_notes_list:
        print("Không tìm thấy ghi chú nào theo định dạng `# Pet ID: PETxxx - Notes on YYYY-MM-DD` trong owner_notes.md. Đảm bảo định dạng chuẩn.")
        # Nếu không có ghi chú nào được parse, tạo DataFrame rỗng để tránh lỗi
        df_owner_notes = spark.createDataFrame([], "pet_id_raw: string, note_date_str: string, note_content: string")
    else:
        df_owner_notes = spark.createDataFrame(parsed_notes_list)

    # Chuẩn hóa pet_id về chữ thường
    df_owner_notes = df_owner_notes.withColumn("pet_id", lower(col("pet_id_raw"))) \
                                  .withColumn("note_date", to_timestamp(col("note_date_str"), "yyyy-MM-dd")) \
                                  .drop("pet_id_raw", "note_date_str")

    print("\nParsed Owner Notes:")
    df_owner_notes.show(truncate=False)
    df_owner_notes.printSchema()

    # --- Áp dụng NLP cho cột 'note_content' ---
    from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF

    # 1. Tokenization (Tách từ)
    tokenizer = Tokenizer(inputCol="note_content", outputCol="words")
    df_tokenized = tokenizer.transform(df_owner_notes)

    # 2. Stop Words Removal (Loại bỏ từ dừng) - Sử dụng tiếng Việt nếu ghi chú bằng tiếng Việt
    # Nếu ghi chú là tiếng Việt, cần danh sách stop words tiếng Việt
    # Ví dụ đơn giản cho tiếng Việt (cần danh sách đầy đủ hơn):
    vietnamese_stop_words = ["là", "và", "của", "có", "không", "những", "một", "rất", "này", "mà", "để", "ở", "với"]
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=vietnamese_stop_words)
    df_filtered = remover.transform(df_tokenized)

    # 3. Feature Extraction (TF-IDF)
    # HashingTF biến đổi các từ (hashed) thành một vector tần suất thô
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=1000) # numFeatures: kích thước vector
    df_hashed = hashingTF.transform(df_filtered)

    # IDF (Inverse Document Frequency) điều chỉnh trọng số các từ phổ biến
    idf = IDF(inputCol="raw_features", outputCol="text_features")
    idfModel = idf.fit(df_hashed)
    df_nlp_features = idfModel.transform(df_hashed)

    print("\nOwner Notes sau khi trích xuất đặc trưng NLP (TF-IDF):")
    df_nlp_features.select("pet_id", "note_date", "note_content", "text_features").show(truncate=False)

except Exception as e:
    print(f"Lỗi khi xử lý owner_notes.md: {e}")
    # spark.stop() # Không dừng Spark ngay, tiếp tục với các phần khác
    # exit()


# ==============================================================================
# D. Gộp tất cả các DataFrame đã xử lý vào DataFrame cuối cùng
# ==============================================================================

print("\n--- Gộp tất cả dữ liệu đã xử lý ---")

# df_final sẽ bắt đầu từ df_merged_base (activity_logs + pet_profiles)
df_final = df_merged_base

# Gộp dữ liệu feeder logs
# Cần gộp theo pet_id và date.
# df_feeder_summary_daily['log_date'] cần chuyển thành date type trước khi join
df_feeder_summary_daily = df_feeder_summary_daily.withColumn("log_date_for_join", to_timestamp(col("log_date"), "yyyy-MM-dd"))

# Thực hiện left join để giữ lại tất cả các bản ghi từ df_final
df_final = df_final.join(
    df_feeder_summary_daily,
    (df_final["pet_id"] == df_feeder_summary_daily["pet_id"]) & (df_final["date"] == df_feeder_summary_daily["log_date_for_join"]),
    how="left"
).drop("pet_name_from_log", "log_date", "log_date_for_join") # Xóa các cột trùng lặp/tạm thời

# Gộp dữ liệu từ owner notes
# Cần gộp theo pet_id và note_date.
df_final = df_final.join(
    df_nlp_features.select("pet_id", "note_date", "text_features"),
    (df_final["pet_id"] == df_nlp_features["pet_id"]) & (df_final["date"] == df_nlp_features["note_date"]),
    how="left"
).drop(df_nlp_features["pet_id"], df_nlp_features["note_date"])


print("\nSchema của DataFrame cuối cùng (đã gộp tất cả):")
df_final.printSchema()
print("\n5 dòng đầu tiên của DataFrame cuối cùng:")
df_final.show(5, truncate=False)

print(f"\nDataFrame cuối cùng có {df_final.count()} bản ghi và {len(df_final.columns)} cột.")

# ==============================================================================
# E. Các phân tích và chuẩn bị mô hình tiếp theo (ví dụ)
# ==============================================================================

print("\n--- Bắt đầu Phân tích Sức khỏe ---")

# Ví dụ 1: Tính toán các chỉ số sức khỏe hàng tuần/hàng tháng
# df_weekly_summary = df_final.withColumn("week_of_year", weekofyear(col("date"))) \
#                             .groupBy("pet_id", "week_of_year") \
#                             .agg(
#                                 avg("food_amount").alias("avg_food_weekly"),
#                                 avg("sleep_hours").alias("avg_sleep_weekly"),
#                                 avg("weight_kg").alias("avg_weight_weekly")
#                             )
# print("\nTổng kết sức khỏe hàng tuần:")
# df_weekly_summary.show(5)

# Ví dụ 2: Tìm các hành vi bất thường
print("\nCác bản ghi có meow_count cao bất thường (>10):")
df_final.filter(col("meow_count") > 10).show(truncate=False)

print("\nCác bản ghi có sự kiện nghẽn thiết bị cho ăn (feeder_jammed_event):")
df_final.filter(col("feeder_jammed_event") == True).show(truncate=False)
from pyspark.sql.functions import col, avg, stddev, abs, lit, current_date, year, round

# --- Phân tích Dữ liệu Hoạt động và Hồ sơ Thú cưng ---
print("\n--- Phân tích Dữ liệu Hoạt động và Hồ sơ Thú cưng ---")

# Hiển thị một số thống kê mô tả cơ bản cho các cột số
print("Thống kê mô tả cho lượng thức ăn, thời gian ngủ và cân nặng:")
merged_df.select("food_amount", "sleep_hours", "weight_kg").describe().show()

# Tính toán trung bình và độ lệch chuẩn của lượng thức ăn và thời gian ngủ cho MỖI thú cưng
# Sử dụng Window Function để tính toán trên từng pet_id
from pyspark.sql.window import Window

window_spec = Window.partitionBy("pet_id")

df_pet_stats = merged_df.withColumn(
    "avg_food_amount_per_pet", avg("food_amount").over(window_spec)
).withColumn(
    "stddev_food_amount_per_pet", stddev("food_amount").over(window_spec)
).withColumn(
    "avg_sleep_hours_per_pet", avg("sleep_hours").over(window_spec)
).withColumn(
    "stddev_sleep_hours_per_pet", stddev("sleep_hours").over(window_spec)
)

# Để tránh trùng lặp dữ liệu và chỉ lấy các giá trị thống kê duy nhất cho mỗi pet
df_pet_stats_distinct = df_pet_stats.select(
    "pet_id", "avg_food_amount_per_pet", "stddev_food_amount_per_pet",
    "avg_sleep_hours_per_pet", "stddev_sleep_hours_per_pet"
).distinct()

print("\nThống kê trung bình và độ lệch chuẩn theo từng thú cưng:")
df_pet_stats_distinct.show(truncate=False)

# --- Phát hiện Hành vi Bất thường (Ví dụ đơn giản) ---
# Gộp lại DataFrame gốc với các thống kê đã tính
df_with_anomalies = merged_df.join(df_pet_stats_distinct, "pet_id", "left")

# Định nghĩa ngưỡng bất thường (ví dụ: lệch quá 2 độ lệch chuẩn)
z_score_threshold = 2.0

df_anomalies = df_with_anomalies.withColumn(
    "food_anomaly_score", 
    (col("food_amount") - col("avg_food_amount_per_pet")) / col("stddev_food_amount_per_pet")
).withColumn(
    "sleep_anomaly_score", 
    (col("sleep_hours") - col("avg_sleep_hours_per_pet")) / col("stddev_sleep_hours_per_pet")
).withColumn(
    "is_food_anomaly", 
    (abs(col("food_anomaly_score")) > lit(z_score_threshold))
).withColumn(
    "is_sleep_anomaly", 
    (abs(col("sleep_anomaly_score")) > lit(z_score_threshold))
)

print(f"\nPhân tích hành vi bất thường (ngưỡng Z-score > {z_score_threshold}):")
df_anomalies.filter(col("is_food_anomaly") | col("is_sleep_anomaly")).select(
    "pet_id", "date", "food_amount", "avg_food_amount_per_pet", "stddev_food_amount_per_pet", "is_food_anomaly",
    "sleep_hours", "avg_sleep_hours_per_pet", "stddev_sleep_hours_per_pet", "is_sleep_anomaly"
).orderBy("pet_id", "date").show(truncate=False)

# Ngoài ra, em có thể tính toán độ tuổi của thú cưng tại thời điểm ghi log
# Lưu ý: "date" là timestamp, "birth_year" là long.
# Cần chuyển đổi "date" sang dạng năm để tính toán chính xác
df_with_age = merged_df.withColumn("current_year", year(col("date"))) \
                       .withColumn("age_at_log", col("current_year") - col("birth_year"))

print("\nDataFrame với cột tuổi thú cưng tại thời điểm ghi log:")
df_with_age.select("pet_id", "date", "birth_year", "age_at_log").show(5)

# Em cũng có thể thực hiện phân tích mối tương quan giữa các biến
# Ví dụ: Mối tương quan giữa food_amount và weight_kg
print("\nMối tương quan giữa lượng thức ăn và cân nặng (ví dụ):")
# Để tính tương quan, Spark cần DataFrame được chọn các cột số.
# Giả sử chúng ta muốn xem tương quan trên toàn bộ dataset
# Lưu ý: method .corr() chỉ áp dụng cho DataFrame cục bộ hoặc RDD,
# để tính trên Spark DataFrame, em cần sử dụng hàm corr từ pyspark.sql.functions
print(f"Tương quan giữa food_amount và weight_kg: {merged_df.stat.corr('food_amount', 'weight_kg')}")
print(f"Tương quan giữa sleep_hours và weight_kg: {merged_df.stat.corr('sleep_hours', 'weight_kg')}")
print(f"Tương quan giữa food_amount và meow_count: {merged_df.stat.corr('food_amount', 'meow_count')}")
print(f"Tương quan giữa sleep_hours và meow_count: {merged_df.stat.corr('sleep_hours', 'meow_count')}")


# Các bước tiếp theo:
# - Lưu kết quả phân tích nếu cần
# df_anomalies.write.csv("hdfs:///user/hduser/capstone_output/pet_anomalies.csv", mode="overwrite", header=True)

# Dừng SparkSession


# ==============================================================================
# F. Dừng SparkSession
# ==============================================================================
spark.stop()
print("\nHoàn tất phân tích. SparkSession đã dừng.")
