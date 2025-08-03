#!/bin/bash

# Đặt đường dẫn LOG_FILE vào thư mục của người dùng hiện tại
LOG_FILE="/home/ntl/logs/spark_automation.log"

# Đảm bảo thư mục logs tồn tại
mkdir -p "$(dirname "$LOG_FILE")"

echo "--------------------------------------------------------" | tee -a "$LOG_FILE"
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): BẮT ĐẦU quá trình tự động hóa Spark Job." | tee -a "$LOG_FILE"

# --- Cấu hình các đường dẫn và tham số ---
GENERATE_DATA_SCRIPT="/home/ntl/capstone_data/local_project/pet_dashboard/scripts/generate_new_data.py"
SPARK_JOB_SCRIPT="/home/ntl/capstone_data/pet_health_analysis_full.py"
LOCAL_MERGED_DATA="/home/ntl/capstone_data/merged_pet_data.csv"

HDFS_RAW_DATA_DIR="/user/ntl/capstone_data/raw_data"
HDFS_MERGED_DATA_PATH="$HDFS_RAW_DATA_DIR/merged_pet_data.csv"
HDFS_ANALYZED_DATA_DIR="/user/ntl/capstone_data/analyzed_data"
LOCAL_FLASK_DATA_DIR="/home/ntl/capstone_data/local_project/pet_dashboard/data"

# --- CẤU HÌNH BIẾN MÔI TRƯỜNG QUAN TRỌNG VÀ CHÍNH XÁC ---
# Đảm bảo các biến môi trường này trỏ đúng đến các thư mục cài đặt của bạn
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/home/ntl/usr/local/hadoop
export SPARK_HOME=/home/ntl/usr/local/spark

# Cấu hình đường dẫn cho các file cấu hình
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export YARN_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_CONF_DIR="$SPARK_HOME/conf"

# Thêm bin của Java, Hadoop, Spark vào PATH để các lệnh có thể được tìm thấy
export PATH="$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin"

# --- Các bước của pipeline ---

# Bước 1: Sinh/Cập nhật dữ liệu cục bộ
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B1] Đang sinh/cập nhật dữ liệu cục bộ ($LOCAL_MERGED_DATA)..." | tee -a "$LOG_FILE"
python3 "$GENERATE_DATA_SCRIPT" >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [LỖI B1] Lỗi khi sinh dữ liệu. Dừng lại." | tee -a "$LOG_FILE"
    exit 1
fi
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B1] Đã sinh/cập nhật dữ liệu cục bộ thành công." | tee -a "$LOG_FILE"


# Bước 2: Xóa dữ liệu cũ trên HDFS (nếu tồn tại)
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B2] Đang xóa dữ liệu cũ trên HDFS ($HDFS_MERGED_DATA_PATH)..." | tee -a "$LOG_FILE"
if "$HADOOP_HOME"/bin/hdfs dfs -test -e "$HDFS_MERGED_DATA_PATH"; then
    "$HADOOP_HOME"/bin/hdfs dfs -rm -f "$HDFS_MERGED_DATA_PATH" >> "$LOG_FILE" 2>&1
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B2] Đã xóa dữ liệu cũ trên HDFS." | tee -a "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B2] Không tìm thấy dữ liệu cũ trên HDFS để xóa, bỏ qua." | tee -a "$LOG_FILE"
fi

# Bước 3: Tải dữ liệu mới lên HDFS
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B3] Đang tải dữ liệu mới lên HDFS từ $LOCAL_MERGED_DATA đến $HDFS_RAW_DATA_DIR/..." | tee -a "$LOG_FILE"
"$HADOOP_HOME"/bin/hdfs dfs -mkdir -p "$HDFS_RAW_DATA_DIR" >> "$LOG_FILE" 2>&1
"$HADOOP_HOME"/bin/hdfs dfs -put -f "$LOCAL_MERGED_DATA" "$HDFS_RAW_DATA_DIR/" >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [LỖI B3] Lỗi khi tải dữ liệu lên HDFS. Dừng lại." | tee -a "$LOG_FILE"
    exit 1
fi
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B3] Đã tải dữ liệu mới lên HDFS thành công." | tee -a "$LOG_FILE"

# Bước 4: Xóa thư mục kết quả phân tích cũ trên HDFS trước khi chạy Spark Job
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B4] Đang xóa thư mục kết quả phân tích cũ trên HDFS ($HDFS_ANALYZED_DATA_DIR)..." | tee -a "$LOG_FILE"
if "$HADOOP_HOME"/bin/hdfs dfs -test -d "$HDFS_ANALYZED_DATA_DIR"; then
    "$HADOOP_HOME"/bin/hdfs dfs -rm -r -f "$HDFS_ANALYZED_DATA_DIR" >> "$LOG_FILE" 2>&1
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B4] Đã xóa thư mục kết quả phân tích cũ trên HDFS." | tee -a "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B4] Không tìm thấy thư mục kết quả phân tích cũ trên HDFS để xóa, bỏ qua." | tee -a "$LOG_FILE"
fi

# Bước 5: Chạy Spark Job để phân tích dữ liệu trên HDFS (với lệnh submit cơ bản)
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B5] Đang chạy Spark Job..." | tee -a "$LOG_FILE"
# Sử dụng lệnh spark-submit chính xác mà bạn đã chỉ định
"$SPARK_HOME"/bin/spark-submit \
    "$SPARK_JOB_SCRIPT" \
    "$HDFS_MERGED_DATA_PATH" \
    "$HDFS_ANALYZED_DATA_DIR" >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [LỖI B5] Lỗi khi chạy Spark Job. Vui lòng kiểm tra log chi tiết." | tee -a "$LOG_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [LỖI B5] Xem thêm log Spark/YARN UI để biết lỗi chính xác." | tee -a "$LOG_FILE"
    exit 1
fi
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B5] Đã chạy Spark Job thành công." | tee -a "$LOG_FILE"

# Bước 6: Kéo các file kết quả từ HDFS về thư mục cục bộ cho Flask Dashboard
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B6] Đang kéo các file kết quả từ HDFS về cục bộ ($LOCAL_FLASK_DATA_DIR)..." | tee -a "$LOG_FILE"
mkdir -p "$LOCAL_FLASK_DATA_DIR" >> "$LOG_FILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B6] Đang xóa các file CSV cũ trong thư mục cục bộ ($LOCAL_FLASK_DATA_DIR)..." | tee -a "$LOG_FILE"
rm -f "$LOCAL_FLASK_DATA_DIR"/*.csv >> "$LOG_FILE" 2>&1

# Danh sách các file kết quả cần kéo về
declare -a ANALYZED_FILES=(
    "pet_health_alerts_final.csv"
    "ml_predictions.csv"
    "breed_trends.csv"
    "monthly_trends.csv"
)

# Kéo từng file một
for file_name in "${ANALYZED_FILES[@]}"; do
    HDFS_SOURCE_PATH="${HDFS_ANALYZED_DATA_DIR}/${file_name}"

    # Spark thường ghi output vào một thư mục chứa nhiều part-xxxx files.
    # getmerge sẽ nối các part-xxxx files trong thư mục đó thành một file duy nhất.
    # Vì vậy, chúng ta cần đảm bảo HDFS_SOURCE_PATH trỏ đúng vào thư mục đó.
    if "$HADOOP_HOME"/bin/hdfs dfs -test -d "$HDFS_SOURCE_PATH"; then
        LOCAL_DEST_PATH="${LOCAL_FLASK_DATA_DIR}/${file_name}"
        echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B6] Đang kéo ${HDFS_SOURCE_PATH} về ${LOCAL_DEST_PATH}..." | tee -a "$LOG_FILE"
        "$HADOOP_HOME"/bin/hdfs dfs -getmerge "$HDFS_SOURCE_PATH" "$LOCAL_DEST_PATH" >> "$LOG_FILE" 2>&1
        if [ $? -ne 0 ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [LỖI B6] Lỗi khi kéo file ${file_name}. Kiểm tra output của Spark Job." | tee -a "$LOG_FILE"
        fi
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B6] CẢNH BÁO: Thư mục kết quả ${HDFS_SOURCE_PATH} không tồn tại trên HDFS. Spark Job có thể chưa chạy hoặc ghi output sai vị trí." | tee -a "$LOG_FILE"
    fi
done
echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): [B6] Đã kéo các file kết quả về cục bộ thành công (hoặc bỏ qua các file không tìm thấy)." | tee -a "$LOG_FILE"

echo "$(date '+%Y-%m-%d %H:%M:%S %Z'): KẾT THÚC toàn bộ quá trình tự động hóa." | tee -a "$LOG_FILE"
