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

---

## 🚀 Hướng dẫn Cài đặt & Chạy Dự án

### Bước 1: Khởi động Minikube cluster
Cấp đủ tài nguyên (Khuyến nghị 10GB RAM, 4 CPUs) để chạy toàn bộ stack:
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
Đợi operator chạy xong, sau đó tạo cluster và topics:
```bash
kubectl apply -f k8s/kafka-cluster.yaml
# Đợi các pod của Kafka chạy (1/1 Running)
kubectl apply -f k8s/kafka-topics.yaml
```

### Bước 4: Deploy MongoDB & Redis
```bash
kubectl apply -f k8s/mongodb-redis.yaml
```

### Bước 5: Deploy MinIO (Object Storage)
```bash
kubectl apply -f k8s/minio.yaml
```
**Khởi tạo Bucket trong MinIO:**
Mở port-forward cho MinIO API ở một terminal mới:
```bash
kubectl port-forward svc/minio 9000:9000 9001:9001 -n data
```
Mở một terminal khác, dùng `mc` để tạo bucket:
```bash
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc mb local/sg-transit-data
mc mb local/sg-transit-data/raw
mc mb local/sg-transit-data/checkpoints
mc mb local/sg-transit-data/models
mc mb local/sg-transit-data/jobs
```
*(Giao diện web của MinIO có tại: `http://localhost:9001` - User/Pass: `minioadmin`/`minioadmin123`)*

### Bước 6: Deploy Spark Operator & Grafana
```bash
# Cài Spark Operator
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator --namespace spark-operator --set webhook.enable=true

# Tạo role cho Spark
kubectl create serviceaccount spark --namespace spark-operator
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=spark-operator:spark --namespace=spark-operator

# Cài Grafana
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
helm install grafana grafana/grafana --namespace monitoring --set adminPassword=Admin@2024 --set service.type=NodePort --set persistence.enabled=true --set persistence.size=2Gi
```

### Bước 7: Cấu hình Secret & Deploy Ứng dụng
1. Bạn cần chuẩn bị API Key của LTA DataMall.
2. Sửa file `k8s/k8s-secrets.yaml` (hoặc tạo từ file mẫu `.env.example`) và điền API Key vào mục `LTA_API_KEY`.
3. Apply ConfigMap, Secret và code Python:
```bash
# Tạo ConfigMap cho code ingestor
kubectl create configmap ingestor-script --from-file=ingestor.py=ingestion/ingestor.py -n transit

# Deploy Secret và các ứng dụng (Ingestor, FastAPI)
kubectl apply -f k8s/k8s-secrets.yaml
kubectl apply -f k8s/k8s-manifests.yaml
```

### Bước 8: Deploy Batch Layer (Spark)

Batch Layer xử lý dữ liệu lịch sử hàng ngày (Window functions, Pivot) và huấn luyện mô hình ML (GraphFrames, GBT Regressor).

1. Upload script Spark lên thư mục jobs của MinIO:
```bash
# Mở port-forward cho MinIO API (nếu chưa mở)
kubectl port-forward svc/minio 9000:9000 -n data &

# Dùng MinIO client để upload
mc cp spark/batch_job.py local/sg-transit-data/jobs/
```

2. Tạo dữ liệu mẫu để test luồng (Tuỳ chọn):
```bash
# Đảm bảo đã port-forward MinIO trước khi chạy script
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

*   **FastAPI:** 
    ```bash
    kubectl port-forward svc/transit-api 8000:8000 -n transit
    ```
    Truy cập tại: `http://localhost:8000`
*   **MinIO Console (Web UI):**
    ```bash
    kubectl port-forward svc/minio 9001:9001 -n data
    ```
    Truy cập tại: `http://localhost:9001` (minioadmin / minioadmin123)
*   **Grafana Dashboard:**
    ```bash
    minikube service grafana -n monitoring --url
    ```
    Mở đường link được in ra (admin / Admin@2024)
*   **MongoDB (Kết nối qua Compass):**
    ```bash
    kubectl port-forward svc/mongodb 27017:27017 -n data
    ```
    Kết nối bằng URL: `mongodb://root:Transit@2024@localhost:27017/transit_db?authSource=admin`

## 🛑 Dừng / Khởi động lại Cluster hàng ngày
Để tiết kiệm RAM khi không dùng, bạn không cần xóa cụm đi. Hãy dùng:
```bash
minikube stop
```
Khi muốn làm việc lại:
```bash
minikube start
```
Kubernetes sẽ tự động giữ nguyên dữ liệu và chạy lại các trạng thái lúc trước.
