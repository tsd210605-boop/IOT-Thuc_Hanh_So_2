import sys
import pandas as pd
import numpy as np
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from sklearn.preprocessing import MinMaxScaler

# Ép bảng mã UTF-8 cho Windows Terminal
sys.stdout.reconfigure(encoding='utf-8')

# Cấu hình InfluxDB 2.x
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-auth-token"
INFLUX_ORG = "iot_lab"
RAW_BUCKET = "iot_raw"
PROCESSED_BUCKET = "iot_processed"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

def run_pipeline():
    print("[PIPELINE] Đang truy vấn dữ liệu thô từ bucket iot_raw...", flush=True)

    # 1. Quét dữ liệu 24h gần nhất để tránh lệch múi giờ Docker
    query = f'''
    from(bucket: "{RAW_BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r._measurement == "sensor_telemetry")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    df = client.query_api().query_data_frame(query)

    # Xử lý trường hợp InfluxDB trả về dạng danh sách DataFrame
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True)

    record_count = len(df) if (df is not None and not df.empty) else 0
    print(f"[DEBUG] Số bản ghi thô tìm thấy trong Database: {record_count}", flush=True)

    if record_count < 5:
        print(f"[THÔNG BÁO] Hiện mới có {record_count}/5 bản ghi. Hãy chờ ESP32 chạy thêm một lát...", flush=True)
        return

    # Sắp xếp thời gian
    df['_time'] = pd.to_datetime(df['_time'])
    df = df.set_index('_time').sort_index()

    cols = ['temperature', 'humidity', 'distance_cm']
    for c in cols:
        if c not in df.columns:
            print(f"[CẢNH BÁO] Thiếu cột {c} trong dữ liệu!", flush=True)
            return

    print(f"[1. INPUT] Bắt đầu xử lý {len(df)} bản ghi thô.", flush=True)

    # 2. Xử lý giá trị thiếu (Missing values) bằng nội suy thời gian
    df[cols] = df[cols].interpolate(method='time').ffill().bfill()

    # 3. Phát hiện và xử lý Outlier bằng phương pháp IQR (Interquartile Range)
    for col in ['temperature', 'distance_cm']:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outlier_mask = (df[col] < lower_bound) | (df[col] > upper_bound)
        num_outliers = outlier_mask.sum()
        if num_outliers > 0:
            print(f"[2. OUTLIER - IQR] Phát hiện {num_outliers} điểm ngoại lai ở cột [{col}]:", flush=True)
            print(f"   Ngưỡng an toàn: [{lower_bound:.2f} -> {upper_bound:.2f}]", flush=True)
            print(f"   Giá trị bất thường: {df.loc[outlier_mask, col].values}", flush=True)
            # Gán NaN cho các điểm ngoại lai và nội suy làm mượt lại
            df.loc[outlier_mask, col] = np.nan
            df[col] = df[col].interpolate(method='time').ffill().bfill()
        else:
            print(f"[2. OUTLIER - IQR] Thuộc tính [{col}] không có ngoại lai.", flush=True)

    # 4. Resampling theo cửa sổ thời gian 15 giây (lấy trung bình mean)
    df_resampled = df[cols].resample('15s').mean().dropna()
    print(f"[3. RESAMPLING] Nén {len(df)} bản ghi thô thành {len(df_resampled)} bản ghi định kỳ 15s.", flush=True)

    # 5. Tạo đặc trưng (Feature Engineering):
    # - Rolling Mean (MA-3): Trung bình trượt 3 chu kỳ làm mịn dao động
    # - Delta: Độ biến thiên giữa 2 mốc thời gian liên tiếp (x_t - x_{t-1})
    df_resampled['temp_rolling_mean'] = df_resampled['temperature'].rolling(window=3, min_periods=1).mean()
    df_resampled['dist_delta'] = df_resampled['distance_cm'].diff().fillna(0)

    # 6. Chuẩn hóa dữ liệu (Min-Max Normalization về dải [0, 1])
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(df_resampled[['temperature', 'humidity', 'distance_cm']])
    df_resampled['temp_norm'] = scaled[:, 0]
    df_resampled['hum_norm'] = scaled[:, 1]
    df_resampled['dist_norm'] = scaled[:, 2]

    # 7. Ghi kết quả vào Bucket iot_processed
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []
    for t, row in df_resampled.iterrows():
        p = (
            Point("sensor_processed")
            .tag("device_id", "esp32_wokwi")
            .field("temp_clean", float(row['temperature']))
            .field("temp_rolling_mean", float(row['temp_rolling_mean']))
            .field("temp_norm", float(row['temp_norm']))
            .field("hum_clean", float(row['humidity']))
            .field("hum_norm", float(row['hum_norm']))
            .field("dist_clean", float(row['distance_cm']))
            .field("dist_delta", float(row['dist_delta']))
            .field("dist_norm", float(row['dist_norm']))
            .time(t)
        )
        points.append(p)

    write_api.write(bucket=PROCESSED_BUCKET, org=INFLUX_ORG, record=points)
    print(f"[4. SUCCESS] Đã lưu thành công {len(points)} bản ghi tiền xử lý vào bucket [{PROCESSED_BUCKET}]!", flush=True)

if __name__ == "__main__":
    run_pipeline()
