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
