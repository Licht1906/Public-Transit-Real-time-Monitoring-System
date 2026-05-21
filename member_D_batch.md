# Hướng dẫn Thành viên D — Batch Layer Lead (cập nhật MinIO)

## Dành cho AI đọc tài liệu này
Đây là hướng dẫn cho một thành viên trong nhóm 5 người làm dự án Big Data môn học. Thành viên là newbie, có Python cơ bản. Hướng dẫn từng bước, tiếng Việt đơn giản. Phiên bản này dùng MinIO thay GCS — chỉ cần đổi config Spark, không đổi logic xử lý.

---

## Bối cảnh dự án

**Tên:** Singapore Multi-Modal Transit Monitoring System  
**Kiến trúc:** Lambda Architecture  
**5 tính năng:** Bus (map + ETA), MRT (crowd level), Carpark (chỗ trống), EV Charging (trạm sạc), Taxi (vị trí rảnh)

**Vai trò của bạn:** Viết Spark Batch job chạy mỗi đêm — đọc raw Parquet từ MinIO (thay GCS), thực hiện các phép biến đổi phức tạp, train ML model dự đoán, phân tích mạng lưới GraphFrames.

**Phụ thuộc:**
- Cần Thành viên A xong MinIO
- Cần raw data trong MinIO (Thành viên C ghi vào `s3a://sg-transit-data/raw/`)
- Có thể tự tạo data mẫu để test trước

---

## Thay đổi so với phiên bản GCS

Chỉ có **2 chỗ thay đổi** trong toàn bộ code của bạn:

| Trước (GCS) | Sau (MinIO) |
|---|---|
| `GCS_BUCKET = "gs://sg-transit-data"` | Dùng biến MinIO endpoint |
| Config Spark không có S3A | Thêm 5 dòng config S3A cho MinIO |

Logic xử lý Spark, Window function, Pivot, UDF, MLlib, GraphFrames — **không đổi gì**.

---

## Luồng data của Batch Layer

```
MinIO: s3a://sg-transit-data/raw/bus/     ← Thành viên C ghi
                    ↓
            Spark đọc Parquet (30 ngày)
                    ↓
    Window → UDF → Broadcast Join → Pivot
                    ↓
            MLlib GBT train model
                    ↓
    MinIO: s3a://sg-transit-data/models/  ← Lưu model
    MongoDB: batch_*                       ← Lưu batch views
```

---

## Batch job hoàn chỉnh

Tạo file `spark/batch_job.py`:

```python
"""
Spark Batch Job — Batch Layer
Thành viên D
Chạy mỗi đêm lúc 2h sáng, xử lý 30 ngày lịch sử từ MinIO.

Thay đổi so với phiên bản GCS:
- Đổi GCS_BUCKET → MINIO_ENDPOINT + MINIO_BUCKET
- Thêm config S3A trong SparkSession
- Đổi prefix gs:// → s3a://
- Logic xử lý không đổi gì
"""

import subprocess
import sys
import os

try:
    import numpy
except ImportError:
    pkg_dir = "/tmp/packages"
    if not os.path.exists(pkg_dir):
        os.makedirs(pkg_dir)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", pkg_dir, "numpy"])
    sys.path.append(pkg_dir)
    import numpy

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# ---- Cấu hình ----
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",
                   "http://minio.data.svc.cluster.local:9000")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "sg-transit-data")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MONGO_URI      = os.getenv("MONGODB_URI",
                   "mongodb://root:Transit@2024@mongodb.data.svc.cluster.local:27017")
DAYS_BACK      = int(os.getenv("DAYS_BACK", "30"))

# Đường dẫn dùng s3a:// thay vì gs://
RAW_PATH    = f"s3a://{MINIO_BUCKET}/raw"
MODELS_PATH = f"s3a://{MINIO_BUCKET}/models"

# ---- Spark Session với config MinIO ----
# 5 dòng config S3A bên dưới là điểm khác biệt duy nhất so với phiên bản GCS
spark = SparkSession.builder \
    .appName("TransitBatchLayer") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access",   "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=== Batch job started ===")
print(f"Reading from MinIO: {RAW_PATH}")

# ============================================================
# BƯỚC 1: ĐỌC RAW DATA từ MinIO — partition pruning
# ============================================================
df_raw = spark.read.parquet(f"{RAW_PATH}/bus/*.parquet") \
    .filter(F.col("ingested_at") >=
            F.date_sub(F.current_timestamp(), DAYS_BACK))
print(f"Raw records (from MinIO, last {DAYS_BACK} days): {df_raw.count()}")

# ============================================================
# BƯỚC 2: WINDOW FUNCTIONS
# ============================================================
window_1h = Window \
    .partitionBy("bus_stop_code", "service_no") \
    .orderBy("ingested_at") \
    .rowsBetween(-11, 0)

window_1d = Window \
    .partitionBy("bus_stop_code", "service_no") \
    .orderBy("ingested_at") \
    .rowsBetween(-287, 0)

df_windowed = df_raw \
    .withColumn("rolling_avg_1h",  F.avg("eta_seconds").over(window_1h)) \
    .withColumn("rolling_std_1h",  F.stddev("eta_seconds").over(window_1h)) \
    .withColumn("rolling_avg_1d",  F.avg("eta_seconds").over(window_1d)) \
    .withColumn("delay_trend",
        F.col("eta_seconds") - F.col("rolling_avg_1h"))

print("Window functions done")

# ============================================================
# BƯỚC 3: CUSTOM UDFs
# ============================================================
def classify_reliability(std_dev):
    if std_dev is None:  return "UNKNOWN"
    if std_dev < 30:     return "RELIABLE"
    if std_dev < 90:     return "MODERATE"
    return "UNRELIABLE"

def compute_congestion(eta, avg):
    if eta is None or avg is None or avg == 0: return 0.0
    ratio = eta / avg
    if ratio > 2.0:  return 100.0
    if ratio > 1.5:  return 75.0
    if ratio > 1.2:  return 50.0
    if ratio > 1.0:  return 25.0
    return 0.0

reliability_udf = F.udf(classify_reliability, StringType())
congestion_udf  = F.udf(compute_congestion,   DoubleType())

df_enriched = df_windowed \
    .withColumn("reliability",
        reliability_udf(F.col("rolling_std_1h"))) \
    .withColumn("congestion_index",
        congestion_udf(F.col("eta_seconds"), F.col("rolling_avg_1h")))

# ============================================================
# BƯỚC 4: BROADCAST JOIN với bus stop metadata từ MongoDB
# ============================================================
bus_meta = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.database", "transit_db") \
    .option("spark.mongodb.collection", "bus_stops_static") \
    .load() \
    .select(
        F.col("BusStopCode").alias("stop_code"),
        F.col("RoadName").alias("road_name"),
        F.col("Description").alias("stop_desc"),
        F.col("Latitude").alias("stop_lat"),
        F.col("Longitude").alias("stop_lng")
    )

bus_meta.cache()
bus_meta.count()

df_final = df_enriched.join(
    F.broadcast(bus_meta),
    df_enriched["bus_stop_code"] == bus_meta["stop_code"],
    "left"
)
print("Broadcast join done")

# ============================================================
# BƯỚC 5: PIVOT — ETA trung bình theo giờ
# ============================================================
df_with_hour = df_final \
    .withColumn("hour_sg",
        F.hour(F.from_utc_timestamp("ingested_at", "Asia/Singapore")))

hourly_pivot = df_with_hour \
    .groupBy("service_no", "bus_stop_code", "road_name") \
    .pivot("hour_sg", list(range(24))) \
    .agg(F.round(F.avg("eta_seconds"), 1))

hourly_pivot.write \
    .mode("overwrite") \
    .format("mongodb") \
    .option("spark.mongodb.database", "transit_db") \
    .option("spark.mongodb.collection", "batch_hourly_pivot") \
    .save()
print("Hourly pivot saved → dùng cho Grafana biểu đồ giờ cao điểm")

# ============================================================
# BƯỚC 6: DAILY SUMMARY
# ============================================================
bus_daily = df_final \
    .withColumn("date_sg",
        F.to_date(F.from_utc_timestamp("ingested_at", "Asia/Singapore"))) \
    .groupBy("service_no", "bus_stop_code", "road_name", "date_sg") \
    .agg(
        F.avg("eta_seconds").alias("avg_eta"),
        F.stddev("eta_seconds").alias("std_eta"),
        F.min("eta_seconds").alias("min_eta"),
        F.max("eta_seconds").alias("max_eta"),
        F.count("*").alias("total_readings"),
        F.avg("congestion_index").alias("avg_congestion"),
        F.first("reliability").alias("reliability")
    )

bus_daily.write \
    .mode("overwrite") \
    .format("mongodb") \
    .option("spark.mongodb.database", "transit_db") \
    .option("spark.mongodb.collection", "batch_bus_daily") \
    .save()

# MRT summary
mrt_raw_path = f"{RAW_PATH}/mrt/"
try:
    mrt_df = spark.read.parquet(mrt_raw_path)
    mrt_daily = mrt_df \
        .withColumn("date_sg",
            F.to_date(F.from_utc_timestamp("ingested_at", "Asia/Singapore"))) \
        .withColumn("crowd_text",
            F.when(F.col("crowd_level") == "h", "HIGH")
             .when(F.col("crowd_level") == "m", "MEDIUM")
             .when(F.col("crowd_level") == "l", "LOW")
             .otherwise("UNKNOWN")) \
        .groupBy("station", "train_line", "date_sg") \
        .agg(
            F.sum(F.when(F.col("crowd_text") == "HIGH",   1).otherwise(0)).alias("hours_high"),
            F.sum(F.when(F.col("crowd_text") == "MEDIUM", 1).otherwise(0)).alias("hours_medium"),
            F.sum(F.when(F.col("crowd_text") == "LOW",    1).otherwise(0)).alias("hours_low"),
            F.count("*").alias("total_readings")
        )
    mrt_daily.write \
        .mode("overwrite") \
        .format("mongodb") \
        .option("spark.mongodb.database", "transit_db") \
        .option("spark.mongodb.collection", "batch_mrt_daily") \
        .save()
    print("MRT daily summary saved")
except Exception as e:
    print(f"MRT batch skipped (no data yet): {e}")

# Carpark summary
carpark_raw_path = f"{RAW_PATH}/carpark/"
try:
    cp_df = spark.read.parquet(carpark_raw_path)
    cp_daily = cp_df \
        .withColumn("date_sg",
            F.to_date(F.from_utc_timestamp("ingested_at", "Asia/Singapore"))) \
        .groupBy("carpark_id", "development", "agency", "date_sg") \
        .agg(
            F.avg("available_lots").alias("avg_lots"),
            F.min("available_lots").alias("min_lots"),
            F.sum(F.when(F.col("available_lots") == 0, 1).otherwise(0)).alias("times_full"),
            F.count("*").alias("total_readings")
        )
    cp_daily.write \
        .mode("overwrite") \
        .format("mongodb") \
        .option("spark.mongodb.database", "transit_db") \
        .option("spark.mongodb.collection", "batch_carpark_daily") \
        .save()
    print("Carpark daily summary saved")
except Exception as e:
    print(f"Carpark batch skipped: {e}")

# ============================================================
# BƯỚC 7: MLlib — Train GBT Regressor
# ============================================================
print("Training MLlib model...")

df_ml = df_final \
    .withColumn("hour_sg",
        F.hour(F.from_utc_timestamp("ingested_at", "Asia/Singapore"))) \
    .withColumn("day_of_week",
        F.dayofweek(F.from_utc_timestamp("ingested_at", "Asia/Singapore"))) \
    .withColumn("hour_sin", F.sin(2 * 3.14159 * F.col("hour_sg") / 24)) \
    .withColumn("hour_cos", F.cos(2 * 3.14159 * F.col("hour_sg") / 24)) \
    .withColumn("day_sin",  F.sin(2 * 3.14159 * F.col("day_of_week") / 7)) \
    .withColumn("day_cos",  F.cos(2 * 3.14159 * F.col("day_of_week") / 7)) \
    .withColumn("target",
        (F.col("eta_seconds") - F.col("rolling_avg_1h")).cast(DoubleType())) \
    .dropna(subset=["target", "rolling_avg_1h", "rolling_std_1h"])

load_idx  = StringIndexer(inputCol="load", outputCol="load_idx",
                           handleInvalid="keep")
assembler = VectorAssembler(
    inputCols=["hour_sin", "hour_cos", "day_sin", "day_cos",
               "rolling_avg_1h", "rolling_std_1h", "rolling_avg_1d", "load_idx"],
    outputCol="features", handleInvalid="skip")
scaler    = StandardScaler(inputCol="features", outputCol="scaled_features",
                            withMean=True, withStd=True)
gbt       = GBTRegressor(featuresCol="scaled_features", labelCol="target",
                          maxIter=50, maxDepth=5, stepSize=0.1, seed=42)

pipeline = Pipeline(stages=[load_idx, assembler, scaler, gbt])
train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_df)

evaluator = RegressionEvaluator(labelCol="target",
    predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(model.transform(test_df))
print(f"Model RMSE: {rmse:.2f} seconds")

# Lưu model vào MinIO (thay GCS)
model.save(f"{MODELS_PATH}/delay_predictor_v1")
print(f"Model saved to MinIO: {MODELS_PATH}/delay_predictor_v1")

# ============================================================
# BƯỚC 8: GraphFrames
# ============================================================
print("GraphFrames analysis...")
try:
    from graphframes import GraphFrame

    vertices = bus_meta \
        .withColumnRenamed("stop_code", "id") \
        .withColumnRenamed("road_name", "road") \
        .withColumnRenamed("stop_desc", "name")

    bus_routes = spark.read \
        .format("mongodb") \
        .option("spark.mongodb.database", "transit_db") \
        .option("spark.mongodb.collection", "bus_routes_static") \
        .load()

    edges = bus_routes \
        .filter(F.col("StopSequence") > 1) \
        .withColumnRenamed("BusStopCode", "dst") \
        .select("ServiceNo", "dst", "StopSequence") \
        .withColumn("src",
            F.lag("dst").over(
                Window.partitionBy("ServiceNo").orderBy("StopSequence"))) \
        .filter(F.col("src").isNotNull()) \
        .withColumnRenamed("ServiceNo", "service") \
        .withColumn("relationship", F.lit("CONNECTS"))

    graph = GraphFrame(vertices, edges)
    pr    = graph.pageRank(resetProbability=0.15, maxIter=10)

    pr.vertices \
        .orderBy(F.desc("pagerank")) \
        .select("id", "name", "road",
                F.round("pagerank", 4).alias("importance")) \
        .write \
        .mode("overwrite") \
        .format("mongodb") \
        .option("spark.mongodb.database", "transit_db") \
        .option("spark.mongodb.collection", "batch_graph_pagerank") \
        .save()
    print("GraphFrames done → top hub stations saved")

except ImportError:
    print("GraphFrames not available — add package to spark-submit")

print("=== Batch job completed ===")
spark.stop()
```

---

## Deploy lên Minikube

Tạo file `batch-spark-app.yaml`:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: ScheduledSparkApplication
metadata:
  name: transit-batch
  namespace: default
spec:
  schedule: "0 2 * * *"
  concurrencyPolicy: Forbid
  template:
    type: Python
    pythonVersion: "3"
    mode: cluster
    image: "apache/spark:3.5.0"
    mainApplicationFile: "s3a://sg-transit-data/jobs/batch_job.py"
    sparkVersion: "3.5.0"
    restartPolicy:
      type: OnFailure
      onFailureRetries: 2
    sparkConf:
      spark.jars.ivy: "/tmp/.ivy2"
      spark.hadoop.fs.s3a.endpoint: "http://minio.data.svc.cluster.local:9000"
      spark.hadoop.fs.s3a.path.style.access: "true"
      spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
    deps:
      packages:
        - "org.mongodb.spark:mongo-spark-connector_2.12:10.6.1"
        - "org.apache.hadoop:hadoop-aws:3.3.4"
        - "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        - "graphframes:graphframes:0.8.3-spark3.5-s_2.12"
    driver:
      cores: 1
      memory: "1g"
      serviceAccount: spark-operator-spark
      env:
      - name: MINIO_ENDPOINT
        valueFrom:
          configMapKeyRef:
            name: transit-config
            key: MINIO_ENDPOINT
      - name: MINIO_BUCKET
        valueFrom:
          configMapKeyRef:
            name: transit-config
            key: MINIO_BUCKET
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: transit-secrets
            key: MINIO_ACCESS_KEY
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: transit-secrets
            key: MINIO_SECRET_KEY
      - name: MONGODB_URI
        valueFrom:
          secretKeyRef:
            name: transit-secrets
            key: MONGODB_URI
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: transit-secrets
            key: MINIO_ACCESS_KEY
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: transit-secrets
            key: MINIO_SECRET_KEY
    executor:
      cores: 1
      instances: 2
      memory: "1g"
      env:
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: transit-secrets
            key: MINIO_ACCESS_KEY
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: transit-secrets
            key: MINIO_SECRET_KEY

---
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: transit-batch-manual
  namespace: default
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: "apache/spark:3.5.0"
  mainApplicationFile: "s3a://sg-transit-data/jobs/batch_job.py"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 2
  sparkConf:
    spark.jars.ivy: "/tmp/.ivy2"
    spark.hadoop.fs.s3a.endpoint: "http://minio.data.svc.cluster.local:9000"
    spark.hadoop.fs.s3a.path.style.access: "true"
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
  deps:
    packages:
      - "org.mongodb.spark:mongo-spark-connector_2.12:10.6.1"
      - "org.apache.hadoop:hadoop-aws:3.3.4"
      - "com.amazonaws:aws-java-sdk-bundle:1.12.262"
      - "graphframes:graphframes:0.8.3-spark3.5-s_2.12"
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark-operator-spark
    env:
    - name: MINIO_ENDPOINT
      valueFrom:
        configMapKeyRef:
          name: transit-config
          key: MINIO_ENDPOINT
    - name: MINIO_BUCKET
      valueFrom:
        configMapKeyRef:
          name: transit-config
          key: MINIO_BUCKET
    - name: MINIO_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: transit-secrets
          key: MINIO_ACCESS_KEY
    - name: MINIO_SECRET_KEY
      valueFrom:
        secretKeyRef:
          name: transit-secrets
          key: MINIO_SECRET_KEY
    - name: MONGODB_URI
      valueFrom:
        secretKeyRef:
          name: transit-secrets
          key: MONGODB_URI
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: transit-secrets
          key: MINIO_ACCESS_KEY
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: transit-secrets
          key: MINIO_SECRET_KEY
  executor:
    cores: 1
    instances: 2
    memory: "1g"
    env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: transit-secrets
          key: MINIO_ACCESS_KEY
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: transit-secrets
          key: MINIO_SECRET_KEY
```

```bash
# Upload batch_job.py lên MinIO
kubectl port-forward svc/minio 9000:9000 -n data &
.\mc cp spark/batch_job.py local/sg-transit-data/jobs/

# Deploy
kubectl apply -f k8s/batch-spark-app.yaml

# Chạy thủ công để test (không chờ 2h sáng)
kubectl create job batch-test --from=scheduledsparkapplication/transit-batch -n default

kubectl logs -f transit-batch-manual-driver -n default
```

---

## Tạo data mẫu để test

```python
# create_test_data.py — tạo data giống ingestor thật
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

MINIO_ENDPOINT = "http://localhost:9000"  # port-forward khi test local
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin123"
MINIO_BUCKET   = "sg-transit-data"

spark = SparkSession.builder \
    .appName("TestData") \
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

HOT_STOPS = ["83139","84009","09022","10199","15239",
             "03219","46211","53121","77009","28031"]
SERVICES  = ["15", "65", "190", "851", "7"]
LOADS     = ["SEA", "SDA", "LSD"]

records = []
base = datetime.now()
for i in range(50000):
    hour = random.randint(0, 23)
    is_peak = 7 <= hour <= 9 or 17 <= hour <= 19
    eta = random.randint(180, 600) if is_peak else random.randint(60, 300)
    t = base - timedelta(minutes=i * 5)
    records.append({
        "bus_stop_code": random.choice(HOT_STOPS),
        "service_no":    random.choice(SERVICES),
        "eta_seconds":   eta,
        "load":          random.choice(LOADS),
        "ingested_at":   t.isoformat(),
    })

df = spark.createDataFrame(records)
df.write.mode("overwrite") \
  .parquet(f"s3a://{MINIO_BUCKET}/raw/bus/")
print(f"Created {df.count()} test records in MinIO")
spark.stop()
```

---

## Checklist

- [ ] Hiểu thay đổi duy nhất: GCS → MinIO, `gs://` → `s3a://`, thêm 5 dòng config S3A
- [ ] `create_test_data.py` tạo data mẫu vào MinIO thành công
- [ ] Kiểm tra data trong MinIO: `mc ls local/sg-transit-data/raw/bus/`
- [ ] `batch_job.py` chạy được với data mẫu
- [ ] MLlib model train xong, có RMSE metric
- [ ] Model lưu vào MinIO tại `s3a://sg-transit-data/models/`
- [ ] GraphFrames PageRank ra top 20 trạm quan trọng
- [ ] 5 batch collections trong MongoDB có data
- [ ] `ScheduledSparkApplication` deploy lên Minikube

---

## Câu hỏi thuyết trình

**Q: Tại sao dùng MinIO thay GCS?**  
A: MinIO là object storage open source, API tương thích 100% với S3/GCS. Spark dùng connector `s3a://` để đọc/ghi MinIO y hệt GCS — không thay đổi logic xử lý, chỉ đổi endpoint. Hoàn toàn miễn phí, chạy như 1 pod trong cluster Minikube.

**Q: Model MLlib lưu vào MinIO có load lại được không?**  
A: Có. `PipelineModel.load("s3a://sg-transit-data/models/delay_predictor_v1")` load lại model y hệt như load từ GCS. Chỉ cần đảm bảo SparkSession có config S3A đúng.

**Q: Tại sao cần Batch Layer nếu đã có Streaming?**  
A: Streaming xử lý real-time nhưng không thể train ML model trên 30 ngày lịch sử — không đủ memory. Batch đọc toàn bộ MinIO 30 ngày, train GBT Regressor chính xác hơn, tạo hourly pivot chart — những việc Streaming không làm được.
