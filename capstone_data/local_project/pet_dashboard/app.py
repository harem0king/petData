from flask import Flask, render_template, url_for
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import numpy as np # Import numpy for numerical types

app = Flask(__name__)

# --- Cấu hình đường dẫn ---
LOCAL_DATA_DIR = "/home/ntl/capstone_data/local_project/pet_dashboard/data"
STATIC_DIR = "/home/ntl/capstone_data/local_project/pet_dashboard/static"

# Đảm bảo các thư mục tồn tại (đã làm ở bước chuẩn bị)
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

# --- Hàm tải và chuẩn bị dữ liệu cho biểu đồ ---
def load_and_prepare_data():
    alerts_path = os.path.join(LOCAL_DATA_DIR, 'pet_health_alerts_final.csv')
    ml_path = os.path.join(LOCAL_DATA_DIR, 'ml_predictions.csv')
    breed_trends_path = os.path.join(LOCAL_DATA_DIR, 'breed_trends.csv')
    monthly_trends_path = os.path.join(LOCAL_DATA_DIR, 'monthly_trends.csv')
    
    # Đã SỬA đường dẫn cho merged_pet_data.csv để trỏ đến vị trí thực tế
    merged_pet_data_path = "/home/ntl/capstone_data/merged_pet_data.csv" # SỬA TẠI ĐÂY

    dfs = {}
    try:
        # Xử lý alerts_df
        dfs['alerts'] = pd.read_csv(alerts_path)
        dfs['alerts']['date'] = pd.to_datetime(dfs['alerts']['date'], format='mixed', errors='coerce')
        dfs['alerts'].dropna(subset=['date'], inplace=True)
        print(f"DEBUG: alerts_df loaded. Shape: {dfs['alerts'].shape}, Latest Date: {dfs['alerts']['date'].max() if not dfs['alerts'].empty else 'N/A'}")

        # Xử lý ml_predictions_df
        dfs['ml_predictions'] = pd.read_csv(ml_path)
        dfs['ml_predictions']['date'] = pd.to_datetime(dfs['ml_predictions']['date'], format='mixed', errors='coerce')
        dfs['ml_predictions'].dropna(subset=['date'], inplace=True)
        dfs['ml_predictions']['health_alert_level'] = dfs['ml_predictions']['health_alert_level'].astype(str)
        # NEW: Chuyển đổi cột 'prediction' sang dạng số
        dfs['ml_predictions']['prediction'] = pd.to_numeric(dfs['ml_predictions']['prediction'], errors='coerce')
        dfs['ml_predictions'].dropna(subset=['prediction'], inplace=True) # Xóa các dòng nếu không thể chuyển đổi prediction
        print(f"DEBUG: ml_predictions_df loaded. Shape: {dfs['ml_predictions'].shape}")

        # Xử lý breed_trends_df
        dfs['breed_trends'] = pd.read_csv(breed_trends_path)
        print(f"DEBUG: breed_trends_df loaded. Shape: {dfs['breed_trends'].shape}")

        # Xử lý monthly_trends_df
        dfs['monthly_trends'] = pd.read_csv(monthly_trends_path)
        print(f"DEBUG: monthly_trends_df loaded. Shape: {dfs['monthly_trends'].shape}")

        # Xử lý merged_pet_data_df (Dữ liệu hoạt động)
        dfs['merged_pet_data'] = pd.read_csv(merged_pet_data_path)
        dfs['merged_pet_data']['date'] = pd.to_datetime(dfs['merged_pet_data']['date'], format='mixed', errors='coerce')
        dfs['merged_pet_data'].dropna(subset=['date'], inplace=True)
        # Bổ sung cột 'species' và 'name' cho merged_pet_data nếu cần cho các biểu đồ mới
        # Điều này yêu cầu bạn có df_pet_profiles hoặc đảm bảo 'species' có sẵn trong merged_pet_data.csv
        # Nếu merged_pet_data.csv không có 'species', bạn cần join thủ công ở đây
        # For simplicity, assuming 'species' is available or will be handled by Spark output.
        print(f"DEBUG: merged_pet_data_df loaded. Shape: {dfs['merged_pet_data'].shape}")

        print("Đã tải tất cả các file CSV thành công.")
    except FileNotFoundError as e:
        print(f"Lỗi: Không tìm thấy file CSV: {e}. Đảm bảo đã kéo file về từ HDFS.")
        # Đảm bảo tất cả các DataFrame được khởi tạo rỗng trong trường hợp lỗi
        dfs = {k: pd.DataFrame() for k in ['alerts', 'ml_predictions', 'breed_trends', 'monthly_trends', 'merged_pet_data']}
    except Exception as e:
        print(f"Lỗi khi đọc file CSV (Generic Error): {e}")
        # Đảm bảo tất cả các DataFrame được khởi tạo rỗng trong trường hợp lỗi
        dfs = {k: pd.DataFrame() for k in ['alerts', 'ml_predictions', 'breed_trends', 'monthly_trends', 'merged_pet_data']}
    return dfs

# --- Hàm tạo biểu đồ phân bố cảnh báo ---
def create_alert_distribution_plot(alerts_df):
    print(f"DEBUG: Inside create_alert_distribution_plot. alerts_df is empty: {alerts_df.empty}")
    plot_path = os.path.join(STATIC_DIR, 'alert_distribution.png')
    if alerts_df.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: alerts_df is empty, returning None for alert_distribution_plot.")
        return None

    if 'date' not in alerts_df.columns or alerts_df['date'].empty:
        latest_date = datetime.now().date()
        latest_alerts = alerts_df
    else:
        latest_date = alerts_df['date'].max()
        latest_alerts = alerts_df[alerts_df['date'] == latest_date]

    if latest_alerts.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: latest_alerts for distribution is empty, returning None.")
        return None

    order = ['Đỏ (Nguy cơ cao)', 'Cam (Cảnh báo)', 'Vàng (Theo dõi)', 'Xanh (Bình thường)']
    colors_map = {
        'Đỏ (Nguy cơ cao)': '#DC3545',
        'Cam (Cảnh báo)': '#FD7E14',
        'Vàng (Theo dõi)': '#FFC107',
        'Xanh (Bình thường)': '#28A745'
    }

    alert_counts = pd.Series(0, index=order)
    actual_counts = latest_alerts['health_alert_level'].value_counts()
    alert_counts.update(actual_counts)
    alert_counts = alert_counts.reindex(order)

    plt.figure(figsize=(10, 6))
    sns.set_style("whitegrid")
    ax = sns.barplot(x=alert_counts.index, y=alert_counts.values,
                     palette=[colors_map[level] for level in alert_counts.index])

    for p in ax.patches:
        ax.text(p.get_x() + p.get_width() / 2., p.get_height(), '%d' % int(p.get_height()),
                        fontsize=12, color='black', ha='center', va='bottom')

    plt.title(f'Thống kê mức cảnh báo sức khỏe thú cưng (Ngày: {latest_date.strftime("%Y-%m-%d")})', fontsize=16)
    plt.xlabel('Mức cảnh báo', fontsize=12)
    plt.ylabel('Số lượng thú cưng', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()
    return plot_path

# --- Hàm tạo biểu đồ xu hướng cảnh báo ---
def create_alert_trend_plot(ml_predictions_df, pet_id_to_show):
    print(f"DEBUG: Inside create_alert_trend_plot. ml_predictions_df is empty: {ml_predictions_df.empty}")
    plot_path = os.path.join(STATIC_DIR, f'alert_trend_{pet_id_to_show}.png')
    if ml_predictions_df.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: ml_predictions_df is empty, returning None for alert_trend_plot.")
        return None

    pet_data = ml_predictions_df[ml_predictions_df['pet_id'] == pet_id_to_show].sort_values('date')
    print(f"DEBUG: pet_data for {pet_id_to_show} is empty: {pet_data.empty}")
    if pet_data.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: pet_data for {pet_id_to_show} is empty, returning None for alert_trend_plot.")
        return None

    alert_mapping = {
        'Xanh (Bình thường)': 0,
        'Vàng (Theo dõi)': 1,
        'Cam (Cảnh báo)': 2,
        'Đỏ (Nguy cơ cao)': 3
    }
    pet_data['rule_based_alert_numeric'] = pet_data['health_alert_level'].map(alert_mapping).fillna(-1)
    # Đảm bảo prediction là numeric trước khi plot
    pet_data['ml_prediction_numeric'] = pd.to_numeric(pet_data['prediction'], errors='coerce')
    pet_data.dropna(subset=['ml_prediction_numeric'], inplace=True)

    plt.figure(figsize=(14, 7))
    sns.set_style("whitegrid")
    plt.plot(pet_data['date'], pet_data['rule_based_alert_numeric'],
             label='Cảnh báo theo Quy tắc', marker='o', linestyle='-', color='#007BFF', linewidth=2)
    plt.plot(pet_data['date'], pet_data['ml_prediction_numeric'],
             label='Dự đoán ML', marker='x', linestyle='--', color='#DC3545', linewidth=2)

    plt.yticks([0, 1, 2, 3], ['Xanh (Bình thường)', 'Vàng (Theo dõi)', 'Cam (Cảnh báo)', 'Đỏ (Nguy cơ cao)'])

    plt.title(f'Xu hướng Cảnh báo Sức khỏe cho {pet_id_to_show}', fontsize=16)
    plt.xlabel('Ngày', fontsize=12)
    plt.ylabel('Mức độ Cảnh báo', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(fontsize=12)
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()
    return plot_path

# --- Hàm tạo biểu đồ tần suất từ khóa ---
def create_keyword_frequency_plot(alerts_df):
    print(f"DEBUG: Inside create_keyword_frequency_plot. alerts_df is empty: {alerts_df.empty}")
    plot_path = os.path.join(STATIC_DIR, 'keyword_frequency.png')
    if alerts_df.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: alerts_df is empty, returning None for keyword_frequency_plot.")
        return None

    if 'date' not in alerts_df.columns or alerts_df['date'].empty:
        latest_date = datetime.now().date()
        latest_alerts = alerts_df
    else:
        latest_date = alerts_df['date'].max()
        latest_alerts = alerts_df[alerts_df['date'] == latest_date]

    print(f"DEBUG: latest_alerts for keyword frequency (date={latest_date}) is empty: {latest_alerts.empty}")

    if latest_alerts.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: latest_alerts for keyword frequency is empty, returning None.")
        return None

    stress_counts = latest_alerts['note_is_stressful'].value_counts() if 'note_is_stressful' in latest_alerts.columns else pd.Series()
    illness_counts = latest_alerts['note_is_illness_related'].value_counts() if 'note_is_illness_related' in latest_alerts.columns else pd.Series()

    plot_data = pd.DataFrame({
        'Category': ['Stress', 'Bệnh tật'],
        'Có ghi nhận': [stress_counts.get(True, 0), illness_counts.get(True, 0)],
        'Không ghi nhận': [stress_counts.get(False, 0), illness_counts.get(False, 0)]
    })

    plt.figure(figsize=(10, 6))
    sns.set_style("whitegrid")
    ax = plot_data.set_index('Category').plot(kind='bar', stacked=True, color=['#FFC107', '#28A745'], ax=plt.gca())

    for container in ax.containers:
        ax.bar_label(container, fmt='%d', label_type='center', color='black')

    plt.title(f'Tần suất Ghi nhận Dấu hiệu Stress và Bệnh tật (Ngày: {latest_date.strftime("%Y-%m-%d")})', fontsize=16)
    plt.xlabel('Loại ghi nhận', fontsize=12)
    plt.ylabel('Số lượng ghi nhận', fontsize=12)
    plt.xticks(rotation=0)
    plt.legend(title='Trạng thái ghi nhận')
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()
    return plot_path

# --- NEW: Hàm tạo biểu đồ xu hướng hoạt động (Food, Sleep, Weight) ---
def create_activity_trend_plot(merged_pet_data_df, pet_id_to_show):
    print(f"DEBUG: Inside create_activity_trend_plot. merged_pet_data_df is empty: {merged_pet_data_df.empty}")
    plot_path = os.path.join(STATIC_DIR, f'activity_trend_{pet_id_to_show}.png')
    if merged_pet_data_df.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: merged_pet_data_df is empty, returning None for activity_trend_plot.")
        return None

    pet_activity_data = merged_pet_data_df[merged_pet_data_df['pet_id'] == pet_id_to_show].sort_values('date')
    print(f"DEBUG: pet_activity_data for {pet_id_to_show} is empty: {pet_activity_data.empty}")
    if pet_activity_data.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: pet_activity_data for {pet_id_to_show} is empty, returning None for activity_trend_plot.")
        return None

    plt.figure(figsize=(14, 10))
    sns.set_style("whitegrid")

    # Food Amount
    plt.subplot(3, 1, 1)
    sns.lineplot(x='date', y='food_amount', data=pet_activity_data, marker='o', color='skyblue')
    plt.title('Xu hướng Lượng thức ăn (kg)', fontsize=14)
    plt.ylabel('Lượng thức ăn (kg)', fontsize=10)
    plt.xlabel('')
    plt.xticks(rotation=45, ha='right')

    # Sleep Hours
    plt.subplot(3, 1, 2)
    sns.lineplot(x='date', y='sleep_hours', data=pet_activity_data, marker='o', color='lightcoral')
    plt.title('Xu hướng Giờ ngủ', fontsize=14)
    plt.ylabel('Giờ ngủ', fontsize=10)
    plt.xlabel('')
    plt.xticks(rotation=45, ha='right')

    # Weight
    plt.subplot(3, 1, 3)
    sns.lineplot(x='date', y='weight_kg', data=pet_activity_data, marker='o', color='lightgreen')
    plt.title('Xu hướng Cân nặng (kg)', fontsize=14)
    plt.ylabel('Cân nặng (kg)', fontsize=10)
    plt.xlabel('Ngày', fontsize=12)
    plt.xticks(rotation=45, ha='right')

    plt.suptitle(f'Xu hướng hoạt động hàng ngày cho {pet_id_to_show}', fontsize=18, y=1.02)
    plt.tight_layout(rect=[0, 0.03, 1, 0.98]) # Adjust layout to prevent suptitle overlap
    plt.savefig(plot_path)
    plt.close()
    return plot_path

# --- NEW: Hàm tạo biểu đồ Box Plot cho các chỉ số hoạt động chung ---
def create_overall_activity_boxplot(merged_pet_data_df):
    print(f"DEBUG: Inside create_overall_activity_boxplot. merged_pet_data_df is empty: {merged_pet_data_df.empty}")
    plot_path = os.path.join(STATIC_DIR, 'overall_activity_boxplot.png')
    if merged_pet_data_df.empty or 'species' not in merged_pet_data_df.columns:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: merged_pet_data_df is empty or missing 'species', returning None for overall_activity_boxplot.")
        return None
    
    # Lấy dữ liệu ngày gần nhất để tránh các giá trị lịch sử làm lệch phân bố
    if 'date' in merged_pet_data_df.columns and not merged_pet_data_df['date'].empty:
        latest_date = merged_pet_data_df['date'].max()
        recent_data = merged_pet_data_df[merged_pet_data_df['date'] == latest_date].copy()
    else:
        recent_data = merged_pet_data_df.copy()

    if recent_data.empty:
        if os.path.exists(plot_path): os.remove(plot_path)
        print(f"DEBUG: Recent data for boxplot is empty, returning None for overall_activity_boxplot.")
        return None

    # Chuyển đổi dữ liệu sang định dạng "long" để dễ vẽ boxplot
    # Chỉ chọn các cột numeric quan trọng
    plot_df = recent_data[['species', 'food_amount', 'sleep_hours', 'weight_kg']].melt(id_vars=['species'], 
                                                                                       var_name='Activity Type', 
                                                                                       value_name='Value')
    # Ánh xạ tên cột thân thiện hơn
    plot_df['Activity Type'] = plot_df['Activity Type'].replace({
        'food_amount': 'Lượng thức ăn (kg)',
        'sleep_hours': 'Giờ ngủ',
        'weight_kg': 'Cân nặng (kg)'
    })

    plt.figure(figsize=(15, 7))
    sns.set_style("whitegrid")
    sns.boxplot(x='Activity Type', y='Value', hue='species', data=plot_df, palette='viridis')
    plt.title('Phân bố các chỉ số hoạt động của thú cưng theo loài', fontsize=16)
    plt.xlabel('Chỉ số hoạt động', fontsize=12)
    plt.ylabel('Giá trị', fontsize=12)
    plt.xticks(rotation=0)
    plt.legend(title='Loài', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig(plot_path)
    plt.close()
    return plot_path


# --- Hàm lấy thông tin thống kê hoạt động hàng ngày của thú cưng ---
def get_pet_daily_activity_summary(merged_pet_data_df, pet_id):
    print(f"DEBUG: Inside get_pet_daily_activity_summary for {pet_id}.")
    summary = {}
    if merged_pet_data_df.empty:
        print("DEBUG: merged_pet_data_df is empty in get_pet_daily_activity_summary.")
        return summary

    pet_data = merged_pet_data_df[merged_pet_data_df['pet_id'] == pet_id].copy()
    if pet_data.empty:
        print(f"DEBUG: No activity data found for pet_id: {pet_id}.")
        return summary

    # Lấy dữ liệu của ngày gần nhất
    latest_date_data = pet_data.sort_values('date', ascending=False).iloc[0]

    # Các cột activity cần hiển thị
    activity_cols = ['food_amount', 'sleep_hours', 'meow_count', 'weight_kg']
    for col in activity_cols:
        if col in latest_date_data and pd.notna(latest_date_data[col]): # Check for NaN
            # Convert numpy types to native Python types for better display
            if isinstance(latest_date_data[col], np.number):
                summary[col] = latest_date_data[col].item()
            else:
                summary[col] = latest_date_data[col]
        else:
            summary[col] = "N/A" # Nếu cột không tồn tại hoặc là NaN

    summary['date'] = latest_date_data['date'].strftime('%Y-%m-%d') if 'date' in latest_date_data and pd.notna(latest_date_data['date']) else "N/A"
    print(f"DEBUG: Activity summary for {pet_id} on {summary.get('date', 'N/A')}: {summary}")
    return summary

# --- Hàm lấy tóm tắt cảnh báo gần đây của thú cưng ---
def get_pet_latest_alert_summary(ml_predictions_df, alerts_df, pet_id):
    print(f"DEBUG: Inside get_pet_latest_alert_summary for {pet_id}.")
    summary = {
        'latest_date': 'N/A',
        'health_alert_level': 'N/A',
        'ml_prediction': 'N/A',
        'note_summary': 'Không có ghi chú bất thường'
    }

    if ml_predictions_df.empty and alerts_df.empty:
        print("DEBUG: Both ml_predictions_df and alerts_df are empty.")
        return summary

    # Lấy thông tin cảnh báo từ ml_predictions_df
    pet_ml_data = ml_predictions_df[ml_predictions_df['pet_id'] == pet_id].sort_values('date', ascending=False)
    if not pet_ml_data.empty:
        latest_ml = pet_ml_data.iloc[0]
        summary['latest_date'] = latest_ml['date'].strftime('%Y-%m-%d')
        summary['health_alert_level'] = latest_ml['health_alert_level']
        # Đảm bảo prediction là số trước khi định dạng
        if pd.notna(latest_ml['prediction']):
            summary['ml_prediction'] = f"{latest_ml['prediction']:.2f}"
        else:
            summary['ml_prediction'] = "N/A"

    # Lấy ghi chú bất thường từ alerts_df (nếu có)
    pet_alerts = alerts_df[alerts_df['pet_id'] == pet_id].sort_values('date', ascending=False)
    if not pet_alerts.empty:
        latest_alert_note = pet_alerts.iloc[0]
        if 'note_text' in latest_alert_note and pd.notna(latest_alert_note['note_text']):
            summary['note_summary'] = latest_alert_note['note_text']
        else:
            summary['note_summary'] = 'Không có ghi chú đặc biệt.'

    print(f"DEBUG: Latest alert summary for {pet_id}: {summary}")
    return summary


# --- Tuyến đường chính của Flask (Trang tổng quan) ---
@app.route('/')
def index():
    dfs = load_and_prepare_data()
    alerts_df = dfs['alerts']
    ml_predictions_df = dfs['ml_predictions']
    merged_pet_data_df = dfs['merged_pet_data']

    available_pet_ids = []
    # Ưu tiên lấy pet_id từ ml_predictions_df
    if not ml_predictions_df.empty and 'pet_id' in ml_predictions_df.columns:
        available_pet_ids = sorted(ml_predictions_df['pet_id'].unique().tolist())
    # Nếu không có, thử từ alerts_df
    if not available_pet_ids and not alerts_df.empty and 'pet_id' in alerts_df.columns:
        available_pet_ids = sorted(alerts_df['pet_id'].unique().tolist())
    # Nếu vẫn không có, thử từ merged_pet_data_df
    if not available_pet_ids and not merged_pet_data_df.empty and 'pet_id' in merged_pet_data_df.columns:
        available_pet_ids = sorted(merged_pet_data_df['pet_id'].unique().tolist())

    if not available_pet_ids:
        available_pet_ids = ["Không có thú cưng nào"]

    initial_pet_id = available_pet_ids[0] if available_pet_ids and available_pet_ids[0] != "Không có thú cưng nào" else "pet001"
    
    # Đảm bảo initial_pet_id tồn tại trong df phù hợp trước khi tạo plot
    # Nếu ml_predictions_df rỗng, tạo plot xu hướng cảnh báo sẽ trả về None
    # Nếu merged_pet_data_df rỗng, tạo plot xu hướng hoạt động sẽ trả về None
            
    alert_dist_plot_path = create_alert_distribution_plot(alerts_df)
    keyword_freq_plot_path = create_keyword_frequency_plot(alerts_df)
    initial_alert_trend_plot_path = create_alert_trend_plot(ml_predictions_df, pet_id_to_show=initial_pet_id)
    initial_activity_trend_plot_path = create_activity_trend_plot(merged_pet_data_df, pet_id_to_show=initial_pet_id) # NEW
    overall_activity_boxplot_path = create_overall_activity_boxplot(merged_pet_data_df) # NEW

    # Lấy thông tin chi tiết cho initial_pet_id để hiển thị lên trang chủ
    initial_activity_summary = get_pet_daily_activity_summary(merged_pet_data_df, initial_pet_id)
    initial_alert_summary = get_pet_latest_alert_summary(ml_predictions_df, alerts_df, initial_pet_id)

    return render_template('index.html',
                            alert_distribution_plot=alert_dist_plot_path,
                            alert_trend_plot=initial_alert_trend_plot_path,
                            keyword_frequency_plot=keyword_freq_plot_path,
                            activity_trend_plot=initial_activity_trend_plot_path, # NEW
                            overall_activity_boxplot=overall_activity_boxplot_path, # NEW
                            available_pet_ids=available_pet_ids,
                            selected_pet_id=initial_pet_id,
                            pet_activity_summary=initial_activity_summary,
                            pet_latest_alert_summary=initial_alert_summary)

# --- Tuyến đường chi tiết cho từng thú cưng ---
@app.route('/pet/<selected_pet_id>')
def pet_detail(selected_pet_id):
    dfs = load_and_prepare_data()
    alerts_df = dfs['alerts']
    ml_predictions_df = dfs['ml_predictions']
    merged_pet_data_df = dfs['merged_pet_data']

    available_pet_ids = []
    if not ml_predictions_df.empty and 'pet_id' in ml_predictions_df.columns:
        available_pet_ids = sorted(ml_predictions_df['pet_id'].unique().tolist())
    if not available_pet_ids and not alerts_df.empty and 'pet_id' in alerts_df.columns:
        available_pet_ids = sorted(alerts_df['pet_id'].unique().tolist())
    if not available_pet_ids and not merged_pet_data_df.empty and 'pet_id' in merged_pet_data_df.columns:
        available_pet_ids = sorted(merged_pet_data_df['pet_id'].unique().tolist())

    if not available_pet_ids:
        available_pet_ids = ["Không có thú cưng nào"]

    alert_trend_plot_path = create_alert_trend_plot(ml_predictions_df, pet_id_to_show=selected_pet_id)
    activity_trend_plot_path = create_activity_trend_plot(merged_pet_data_df, pet_id_to_show=selected_pet_id) # NEW

    alert_dist_plot_path = create_alert_distribution_plot(alerts_df) # Giữ nguyên tổng quan
    keyword_freq_plot_path = create_keyword_frequency_plot(alerts_df) # Giữ nguyên tổng quan
    overall_activity_boxplot_path = create_overall_activity_boxplot(merged_pet_data_df) # NEW

    # Lấy thông tin chi tiết cho pet_id được chọn
    pet_activity_summary = get_pet_daily_activity_summary(merged_pet_data_df, selected_pet_id)
    pet_latest_alert_summary = get_pet_latest_alert_summary(ml_predictions_df, alerts_df, selected_pet_id)

    return render_template('index.html',
                            alert_distribution_plot=alert_dist_plot_path,
                            alert_trend_plot=alert_trend_plot_path,
                            keyword_frequency_plot=keyword_freq_plot_path,
                            activity_trend_plot=activity_trend_plot_path, # NEW
                            overall_activity_boxplot=overall_activity_boxplot_path, # NEW
                            available_pet_ids=available_pet_ids,
                            selected_pet_id=selected_pet_id,
                            pet_activity_summary=pet_activity_summary,
                            pet_latest_alert_summary=pet_latest_alert_summary)

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
