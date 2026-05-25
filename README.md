# Singapore Multi-Modal Transit Monitoring System

Dự án Big Data theo dõi giao thông công cộng đa phương tiện tại Singapore (Bus, MRT, Carpark, EV Charging, Taxi) áp dụng kiến trúc **Lambda Architecture**. Toàn bộ hạ tầng được triển khai cục bộ (local) sử dụng **Minikube** và **MinIO** để tối ưu chi phí (hoàn toàn miễn phí).

---

## 🏗 Kiến trúc Hệ thống
*   **Ingestion:** Python (gọi API từ LTA DataMall Singapore).
*   **Message Broker:** Apache Kafka (triển khai qua Strimzi Operator).
*   **Streaming & Batch Processing:** Apache Spark (chạy qua Spark Operator).
*   **Database:** MongoDB (lưu trữ views) & Redis (caching).
*   **Object Storage:** MinIO (tương thích S3/GCS, dùng lưu trữ raw data & checkpoints cho Spark).
*   **API & Backend:** FastAPI.
*   **Monitoring:** Grafana.
*   **Orchestration:** Kubernetes (Minikube).

---

## 🛠 Yêu cầu Tiền quyết (Prerequisites)

Để chạy dự án này trên máy tính của bạn, hãy đảm bảo bạn đã cài đặt các công cụ sau:
1. **Docker Desktop:** [Tải tại đây](https://www.docker.com/products/docker-desktop/) (Đảm bảo Docker đang chạy).
2. **Minikube:** [Hướng dẫn cài đặt](https://minikube.sigs.k8s.io/docs/start/) (hoặc `winget install Kubernetes.minikube` trên Windows).
3. **Helm:** [Hướng dẫn cài đặt](https://helm.sh/docs/intro/install/) (hoặc `winget install Helm.Helm`).
4. **Kubectl:** Thường đi kèm với Minikube/Docker Desktop.
5. **MinIO Client (mc):** [Hướng dẫn cài đặt](https://min.io/docs/minio/linux/reference/minio-mc.html) (hoặc `winget install MinIO.MinIOClient`).
6. **Python 3.10+** cài đặt trên máy local (dùng để chạy script nạp dữ liệu tĩnh ban đầu).

---

## 🚀 Hướng dẫn Cài đặt & Chạy Dự án (Từng bước chi tiết)

### Bước 1: Khởi động Minikube cluster
Cấp đủ tài nguyên (Khuyến nghị ít nhất 10GB RAM, 4 CPUs) để chạy toàn bộ stack:
```bash
minikube start --driver=docker --memory=10240 --cpus=4 --disk-size=40g
```
Bật các addons cần thiết:
```bash
minikube addons enable storage-provisioner
minikube addons enable default-storageclass
minikube addons enable metrics-server
```

### Bước 2: Tạo Namespaces
```bash
kubectl create namespace kafka
kubectl create namespace data
kubectl create namespace transit
kubectl create namespace spark-operator
kubectl create namespace monitoring
```

### Bước 3: Deploy Kafka (Strimzi)
Cài đặt Strimzi Operator bằng Helm:
```bash
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi strimzi/strimzi-kafka-operator --namespace kafka --set watchAnyNamespace=false
```
Đợi operator chạy xong, sau đó tạo cluster và các topics:
```bash
kubectl apply -f k8s/kafka-cluster.yaml
# Đợi các pod của Kafka chạy (trạng thái 1/1 Running)
kubectl apply -f k8s/kafka-topics.yaml
```

### Bước 4: Deploy MongoDB & Redis
Triển khai hệ thống cơ sở dữ liệu và lưu trữ cache:
```bash
kubectl apply -f k8s/mongodb-redis.yaml
```

### Bước 5: Deploy MinIO & Tải mã nguồn Spark lên Object Storage
1. **Triển khai MinIO:**
   ```bash
   kubectl apply -f k8s/minio.yaml
   ```
2. **Khởi tạo Bucket trong MinIO:**
   Mở port-forward cho MinIO API ở một terminal mới:
   ```bash
   kubectl port-forward svc/minio 9000:9000 9001:9001 -n data
   ```
   Mở một terminal khác, dùng `mc` (MinIO Client) để liên kết và tạo các bucket cần thiết:
   ```bash
   mc alias set local http://localhost:9000 minioadmin minioadmin123
   mc mb local/sg-transit-data
   mc mb local/sg-transit-data/raw
   mc mb local/sg-transit-data/checkpoints
   mc mb local/sg-transit-data/models
   mc mb local/sg-transit-data/jobs
   ```
3. **Upload mã nguồn Spark lên MinIO (QUAN TRỌNG):**
   Spark Operator sẽ kéo file Python từ MinIO để chạy các Job. Hãy upload 2 file python của Spark vào thư mục `jobs` trên MinIO:
   ```bash
   mc cp spark/streaming_job.py local/sg-transit-data/jobs/
   mc cp spark/batch_job.py local/sg-transit-data/jobs/
   ```
   *(Giao diện web của MinIO có tại: `http://localhost:9001` - User/Pass: `minioadmin`/`minioadmin123`)*

### Bước 6: Deploy Spark Operator & Grafana
1. **Cài đặt Spark Operator:**
   ```bash
   helm repo add spark-operator https://kubeflow.github.io/spark-operator
   helm repo update
   helm install spark-operator spark-operator/spark-operator --namespace spark-operator --set webhook.enable=true
   ```
2. **Cấu hình Service Account & Quyền hạn cho Spark:**
   Do file Spark Application của chúng ta cấu hình chạy ở namespace `default` bằng ServiceAccount `spark-operator-spark`, ta cần khởi tạo nó tại namespace `default`:
   ```bash
   kubectl create serviceaccount spark-operator-spark -n default
   kubectl create clusterrolebinding spark-operator-edit --clusterrole=edit --serviceaccount=default:spark-operator-spark -n default
   ```
3. **Cài đặt Grafana:**
   ```bash
   helm repo add grafana https://grafana.github.io/helm-charts
   helm repo update
   helm install grafana grafana/grafana --namespace monitoring --set adminPassword=Admin@2024 --set service.type=NodePort --set persistence.enabled=true --set persistence.size=2Gi
   ```

### Bước 7: Cấu hình Secret, ConfigMap & Deploy Ứng dụng
1. **Chuẩn bị file Secrets:**
   Copy file mẫu `k8s/k8s-secrets.example.yaml` thành `k8s/k8s-secrets.yaml` và điền API Key của LTA DataMall vào trường `LTA_API_KEY`, điền mật khẩu MongoDB/Redis của bạn:
   ```bash
   # Tạo file thực tế từ file mẫu và chỉnh sửa
   cp k8s/k8s-secrets.example.yaml k8s/k8s-secrets.yaml
   ```
2. **Tạo ConfigMap chứa mã nguồn ứng dụng (FastAPI & Ingestor):**
   ```bash
   # Tạo ConfigMap cho code ingestor
   kubectl create configmap ingestor-script --from-file=ingestor.py=ingestion/ingestor.py -n transit

   # Tạo ConfigMap cho code FastAPI (chứa cả file python API chính và giao diện HTML)
   kubectl create configmap api-script --from-file=main.py=api/main.py --from-file=index.html=api/static/index.html -n transit
   ```
3. **Triển khai Secret và ConfigMap cấu hình:**
   ```bash
   # Deploy Secrets & ConfigMap cấu hình chung vào namespace transit
   kubectl apply -f k8s/k8s-secrets.yaml
   kubectl apply -f k8s/transit-config.yaml -n transit

   # Đồng thời apply ConfigMap & Secrets vào namespace default để các ứng dụng Spark (chạy ở default namespace) có thể truy cập được:
   kubectl apply -f k8s/transit-config.yaml -n default
   kubectl apply -f k8s/k8s-secrets.yaml -n default
   ```
4. **Deploy các ứng dụng chính (Ingestor & FastAPI API):**
   ```bash
   kubectl apply -f k8s/k8s-manifests.yaml
   ```

### Bước 8: Khởi tạo Dữ liệu Tĩnh (QUAN TRỌNG)
Hệ thống cần danh sách thông tin tĩnh của 5,000 trạm xe buýt và lộ trình để hiển thị lên bản đồ. Ta cần chạy script `load_static.py` dưới local để nạp dữ liệu này vào MongoDB:
1. Đảm bảo cổng MongoDB đang được chuyển tiếp (port-forward) ra máy local:
   ```bash
   # Chạy ở một terminal riêng biệt
   kubectl port-forward svc/mongodb 27017:27017 -n data
   ```
2. Cài đặt các thư viện Python cần thiết tại local:
   ```bash
   pip install requests pymongo
   ```
3. Chạy script nạp dữ liệu tĩnh:
   * **Trên Windows (cmd/powershell):**
     ```bash
     $env:LTA_API_KEY="Điền_API_Key_LTA_Của_Bạn_Vào_Đây"
     python ingestion/load_static.py
     ```
   * **Trên Linux/macOS:**
     ```bash
     export LTA_API_KEY="Điền_API_Key_LTA_Của_Bạn_Vào_Đây"
     python ingestion/load_static.py
     ```

### Bước 9: Deploy Spark Jobs & Cleanup CronJob
Sau khi các nguồn dữ liệu đã sẵn sàng, ta khởi động các luồng xử lý Spark và Job dọn dẹp tài nguyên tự động:
```bash
# Triển khai Spark Streaming Job để xử lý dữ liệu từ Kafka ghi vào MongoDB & MinIO
kubectl apply -f k8s/streaming-spark-app.yaml

# Triển khai Spark Batch Job để tính toán PageRank của các trạm xe buýt
kubectl apply -f k8s/batch-spark-app.yaml

# Triển khai CronJob tự động dọn dẹp dữ liệu cũ hơn 3 tiếng
kubectl apply -f k8s/cleanup-cronjob.yaml
```
*Kiểm tra trạng thái Spark Application:* `kubectl get sparkapplication -n default` hoặc `kubectl get pods -n default` (Đợi các driver pod ở trạng thái `Running`).

> [!WARNING]
> **Thời gian khởi động ban đầu của Spark (Lưu ý quan trọng):**
> Do Spark chạy lần đầu sẽ cần tải các thư viện `.jar` cần thiết từ Maven Central (như Kafka Connector, MongoDB Connector, AWS SDK/Hadoop AWS kết nối MinIO, nặng tổng cộng gần ~200MB) về bộ nhớ tạm thời của Pod, **thời gian khởi động của các Pod `transit-streaming-driver` và `transit-batch-*-driver` sẽ mất từ 7 đến 10 phút** tùy thuộc vào tốc độ mạng.
> 
> Trong khoảng thời gian này:
> * Giao diện Web/API sẽ **chưa hiển thị dữ liệu** của MRT, Carpark, EV Charging.
> * Bạn có thể theo dõi tiến độ tải bằng lệnh: `kubectl logs transit-streaming-driver -n default -f`.
> * Sau khi Spark Driver log ra dòng `All streaming queries running...`, dữ liệu sẽ được cập nhật thành công lên Bản đồ giám sát.

### Bước 8: Deploy Batch Layer (Spark)

Batch Layer xử lý dữ liệu lịch sử hàng ngày (Window functions, Pivot) và huấn luyện mô hình ML (GraphFrames, GBT Regressor).

1. Upload script Spark lên thư mục jobs của MinIO:
```bash
# Mở port-forward cho MinIO API (nếu chưa mở)
kubectl port-forward svc/minio 9000:9000 -n data &

# Dùng MinIO client để upload
mc cp spark/batch_job.py local/sg-transit-data/jobs/
```

2. Tạo dữ liệu mẫu Parquet để test luồng (tuỳ chọn, dùng khi chưa có raw history):
```bash
# Đảm bảo đã port-forward MinIO trước khi chạy script
pip install -r requirements.txt
python spark/create_test_data.py
```

3. Triển khai cấu hình ScheduledSparkApplication (CronJob tự động chạy lúc 2h sáng):
```bash
kubectl apply -f k8s/batch-spark-app.yaml
```

4. Kích hoạt chạy thử (Manual run) không chờ lịch:
```bash
kubectl create job batch-test --from=scheduledsparkapplication/transit-batch -n default
# Theo dõi log của job
kubectl logs -f job/batch-test -n default
```

---


## 🌐 Cách truy cập các dịch vụ

Sau khi tất cả các Pod đã ở trạng thái `Running` (kiểm tra bằng `kubectl get pods -A`), bạn có thể truy cập các dịch vụ như sau:

*   **FastAPI Web UI (Bản đồ giám sát trực tuyến):** 
    ```bash
    kubectl port-forward svc/transit-api 8000:8000 -n transit
    ```
    Truy cập giao diện Web tại địa chỉ: `http://localhost:8000`

*   **MinIO Console (Quản lý File & Storage raw):**
    ```bash
    kubectl port-forward svc/minio 9001:9001 -n data
    ```
    Truy cập tại: `http://localhost:9001` (User: `minioadmin` / Pass: `minioadmin123`)

*   **Grafana Dashboard (Giám sát chỉ số):**
    ```bash
    minikube service grafana -n monitoring --url
    ```
    Mở đường link được in ra trên terminal. Đăng nhập bằng tài khoản: `admin` / Mật khẩu: `Admin@2024`.

*   **MongoDB (Kết nối qua MongoDB Compass để kiểm tra database):**
    ```bash
    kubectl port-forward svc/mongodb 27017:27017 -n data
    ```
    Kết nối bằng URL: `mongodb://root:Transit@2024@localhost:27017/transit_db?authSource=admin`

---

## 🛑 Dừng / Khởi động lại Cluster hàng ngày
Để tiết kiệm tài nguyên RAM/CPU khi không sử dụng, bạn không cần xoá cụm Minikube. Hãy dùng lệnh sau:
```bash
minikube stop
```
Khi muốn làm việc tiếp:
```bash
minikube start
```
Minikube sẽ tự động khôi phục dữ liệu và chạy lại toàn bộ trạng thái dịch vụ trước đó.
