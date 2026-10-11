# BÀI THỰC HÀNH SỐ 2: THU THẬP, LƯU TRỮ VÀ TIỀN XỬ LÝ DỮ LIỆU IOT
- **Học phần:** IoT và Ứng dụng (INT14149)
- **Sinh viên:** Trần Sơn Duy - B23DCCN250

## 1. Kiến trúc hệ thống
ESP32 (Wokwi) -> EMQX Public Broker (MQTT) -> Python Ingest Worker -> InfluxDB 2.x (TSDB) -> Preprocessing Pipeline (IQR Filter, Resampling 15s, Min-Max Scaler) -> Data Explorer / Dashboard.

## 2. Hướng dẫn chạy
1. Khởi động InfluxDB qua Docker:
   `docker run -d --name influxdb_iot -p 8086:8086 -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=admin -e DOCKER_INFLUXDB_INIT_PASSWORD=adminpassword123 -e DOCKER_INFLUXDB_INIT_ORG=iot_lab -e DOCKER_INFLUXDB_INIT_BUCKET=iot_raw -e DOCKER_INFLUXDB_INIT_RETENTION=7d -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=my-super-secret-auth-token influxdb:2.7`
2. Tạo thêm bucket `iot_processed` với Retention 30 days trên InfluxDB UI.
3. Cài đặt thư viện: `pip install -r requirements.txt`
4. Khởi chạy worker: `python src/ingest_worker.py`
5. Bật mô phỏng trên Wokwi để truyền dữ liệu.
6. Chạy tiền xử lý: `python src/preprocess_pipeline.py`
7. Đo độ trễ hệ thống: `python src/measure_latency.py`
