# Bàn giao từ Batch Layer (Member D) sang Serving Layer (Member E)

Tài liệu này tóm tắt các tính năng, kết quả đầu ra và cách vận hành của hệ thống Batch Processing (do Member D phát triển) để Member E nắm rõ các tài nguyên sẵn có trước khi xây dựng API và Frontend.

## 1. Tóm tắt hệ thống Batch Layer
- **Nhiệm vụ:** Xử lý khối lượng dữ liệu lớn (30 ngày gần nhất) để tính toán thống kê, phân tích đồ thị mạng lưới và huấn luyện mô hình dự đoán.
- **Công nghệ:** PySpark, Kubernetes Spark Operator (chạy ScheduledSparkApplication).
- **Lịch trình chạy:** Tự động kích hoạt vào lúc **2h sáng mỗi ngày** trên Minikube.

## 2. Đầu ra dành cho Member E (MongoDB)
Hệ thống Batch ghi đè (overwrite) kết quả phân tích mỗi đêm vào 5 collections chính trong database `transit_db` của MongoDB. Dưới đây là các collections mà FastAPI của Member E có thể query trực tiếp:

### 2.1. `batch_hourly_pivot` (Biểu đồ giờ cao điểm)
- **Mục đích:** Cung cấp thông tin thời gian chờ (ETA) trung bình của mỗi tuyến xe buýt tại một trạm cụ thể theo từng giờ trong ngày (0h - 23h).
- **Trường dữ liệu chính:** `service_no`, `bus_stop_code`, `road_name` và các cột từ `0` đến `23` (tương ứng với số giây ETA trung bình trong giờ đó).

### 2.2. `batch_bus_daily` (Thống kê xe buýt hằng ngày)
- **Mục đích:** Tổng hợp hiệu năng của hệ thống xe buýt theo từng ngày.
- **Trường dữ liệu chính:** `service_no`, `bus_stop_code`, `road_name`, `date_sg`, `avg_eta`, `std_eta`, `min_eta`, `max_eta`, `total_readings`, `avg_congestion` (chỉ số ùn tắc), `reliability` ("RELIABLE", "MODERATE", "UNRELIABLE").

### 2.3. `batch_mrt_daily` (Thống kê MRT hằng ngày)
- **Mục đích:** Thống kê mức độ đông đúc của các trạm tàu điện ngầm theo ngày.
- **Trường dữ liệu chính:** `station`, `train_line`, `date_sg`, `hours_high` (số giờ cực kỳ đông đúc), `hours_medium`, `hours_low`, `total_readings`.

### 2.4. `batch_carpark_daily` (Thống kê bãi đỗ xe hằng ngày)
- **Mục đích:** Tình trạng sức chứa trung bình của bãi đỗ xe.
- **Trường dữ liệu chính:** `carpark_id`, `development`, `agency`, `date_sg`, `avg_lots` (số chỗ trống trung bình), `min_lots`, `times_full` (số lần bãi báo hết chỗ), `total_readings`.

### 2.5. `batch_graph_pagerank` (Phân tích mạng lưới trạm quan trọng)
- **Mục đích:** Ứng dụng thuật toán PageRank qua GraphFrames để xếp hạng độ quan trọng của các trạm xe buýt.
- **Trường dữ liệu chính:** `id` (mã trạm), `name` (tên trạm), `road` (đường), `importance` (điểm số đánh giá độ quan trọng/đóng vai trò hub trung chuyển).

## 3. Mô hình Dự đoán Độ trễ (Machine Learning)
Batch Layer huấn luyện một mô hình **GBTRegressor (Gradient-Boosted Trees)** mỗi đêm để dự đoán độ trễ ETA thực tế so với trung bình của tuyến xe buýt.

- **Vị trí lưu trữ:** Bucket MinIO tại `s3a://sg-transit-data/models/delay_predictor_v1`.
- **Nhiệm vụ của E:** Backend FastAPI cần tải mô hình này về từ MinIO để thực hiện dự đoán realtime cho người dùng.
- **Yêu cầu Input Features (khi predict):**
  Mô hình cần 8 đặc trưng (features) đã qua Pipeline: 
  - `hour_sin`, `hour_cos`, `day_sin`, `day_cos` (để mô hình hiểu yếu tố thời gian và chu kỳ)
  - `rolling_avg_1h`, `rolling_std_1h`, `rolling_avg_1d` (thông số lịch sử từ bảng streaming/batch)
  - `load_idx` (StringIndexer của mức độ tải: "SEA", "SDA", "LSD")

## 4. Cách Vận hành và Test (Dành cho Member E)
Để E có thể tự test các API mà không cần chờ dữ liệu từ Ingestion, D đã cung cấp công cụ giả lập dữ liệu:

1. **Tạo dữ liệu giả (Mock Data):** Chạy lệnh `python3 create_test_data.py`. File này sẽ sinh ra 50,000 bản ghi raw giả lập và ném trực tiếp vào MinIO.
2. **Kích hoạt Batch Job thủ công:** Nếu không muốn chờ đến 2h sáng, E có thể ép Kubernetes chạy ngay lập tức bằng lệnh:
   ```bash
   kubectl delete sparkapplication transit-batch-manual -n spark-operator
   kubectl apply -f batch-spark-app.yaml
   ```
   *Lưu ý: Image `apache/spark:3.5.0` cần được load vào Minikube và có thể cần build thêm thư viện `numpy` vào Image nếu muốn train MLlib thành công.*
