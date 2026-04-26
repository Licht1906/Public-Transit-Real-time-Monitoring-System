import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, TimestampType, LongType
)

# ---- Cấu hình ----
KAFKA_BOOT     = os.getenv("KAFKA_BOOTSTRAP",
                   "kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
MONGO_URI      = os.getenv("MONGODB_URI",
                   "mongodb://root:Transit@2024@mongodb.data.svc.cluster.local:27017")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",
                   "http://minio.data.svc.cluster.local:9000")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "sg-transit-data")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

# Đường dẫn dùng s3a:// thay vì gs://
CHECKPOINT = f"s3a://{MINIO_BUCKET}/checkpoints"
RAW_PATH   = f"s3a://{MINIO_BUCKET}/raw"


spark = SparkSession.builder \
    .appName("TransitSpeedLayer") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.hadoop.fs.s3a.endpoint",                MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",              MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key",              MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access",       "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled",  "false") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==============================================================
# PHẦN 1: BUS ARRIVALS
# Xử lý data từ ~50 trạm đông nhất để làm analytics + Grafana.
# ETA on-demand của user click: FastAPI tự gọi LTA trực tiếp.
# ==============================================================

bus_schema = StructType([
    StructField("event_type",        StringType()),
    StructField("bus_stop_code",     StringType()),
    StructField("service_no",        StringType()),
    StructField("operator",          StringType()),
    StructField("estimated_arrival", StringType()),
    StructField("monitored",         IntegerType()),
    StructField("latitude",          DoubleType()),
    StructField("longitude",         DoubleType()),
    StructField("load",              StringType()),
    StructField("bus_type",          StringType()),
    StructField("visit_number",      StringType()),
    StructField("ingested_at",       StringType()),
])

def classify_load(load_code):
    return {"SEA": "LOW", "SDA": "MEDIUM", "LSD": "HIGH"}.get(load_code, "UNKNOWN")

load_udf = F.udf(classify_load, StringType())

def compute_priority(eta_seconds, load_level):
    if eta_seconds is None:
        return "NORMAL"
    if eta_seconds < 180:
        return "URGENT"
    if load_level == "HIGH":
        return "CROWDED"
    return "NORMAL"

priority_udf = F.udf(compute_priority, StringType())

bus_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOT) \
    .option("subscribe", "bus-arrivals") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

bus_parsed = bus_raw \
    .select(F.from_json(F.col("value").cast("string"),
                        bus_schema).alias("d")) \
    .select("d.*") \
    .withColumn("ingested_at",
        F.to_timestamp("ingested_at")) \
    .withColumn("estimated_arrival",
        F.to_timestamp("estimated_arrival")) \
    .filter(F.col("bus_stop_code").isNotNull()) \
    .withColumn("eta_seconds",
        (F.unix_timestamp("estimated_arrival") -
         F.unix_timestamp("ingested_at")).cast(IntegerType())) \
    .withColumn("eta_seconds",
        F.when(F.col("eta_seconds") < 0, 0)
         .otherwise(F.col("eta_seconds"))) \
    .withColumn("load_level", load_udf(F.col("load"))) \
    .withColumn("hour_sg",
        # [Bug 2 FIX] Dùng F.col() thay vì string trực tiếp
        F.hour(F.from_utc_timestamp(F.col("ingested_at"), "Asia/Singapore"))) \
    .withColumn("is_peak_hour",
        # [Bug 4 FIX] Cast rõ ràng thành boolean
        (
            ((F.col("hour_sg") >= 7) & (F.col("hour_sg") <= 9)) |
            ((F.col("hour_sg") >= 17) & (F.col("hour_sg") <= 19))
        ).cast("boolean"))

bus_watermarked = bus_parsed.withWatermark("ingested_at", "3 minutes")

bus_agg = bus_watermarked \
    .groupBy(
        F.window("ingested_at", "5 minutes", "1 minute"),
        "bus_stop_code", "service_no"
    ) \
    .agg(
        F.avg("eta_seconds").alias("avg_eta_seconds"),
        F.min("eta_seconds").alias("min_eta_seconds"),
        F.count("*").alias("data_points"),
        F.last("load_level").alias("current_load"),
        F.last("latitude").alias("latest_lat"),
        F.last("longitude").alias("latest_lng"),
        F.last("is_peak_hour").alias("is_peak_hour"),
        F.last("operator").alias("operator"),
        F.last("monitored").alias("monitored")
    ) \
    .withColumn("window_start", F.col("window.start")) \
    .withColumn("window_end",   F.col("window.end")) \
    .withColumn("priority",
        priority_udf(F.col("min_eta_seconds"), F.col("current_load"))) \
    .drop("window")

# Ghi speed views vào MongoDB (cho FastAPI đọc)
bus_query = bus_agg.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT}/bus") \
    .option("spark.mongodb.database", "transit_db") \
    .option("spark.mongodb.collection", "speed_bus") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

# Ghi raw data vào MinIO (cho Batch Layer đọc)
bus_raw_query = bus_parsed.writeStream \
    .format("parquet") \
    .option("checkpointLocation", f"{CHECKPOINT}/bus-raw") \
    .option("path", f"{RAW_PATH}/bus") \
    .partitionBy("hour_sg") \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

# ==============================================================
# PHẦN 2: MRT CROWD
# ==============================================================

mrt_schema = StructType([
    StructField("event_type",  StringType()),
    StructField("train_line",  StringType()),
    StructField("station",     StringType()),
    StructField("crowd_level", StringType()),
    StructField("start_time",  StringType()),
    StructField("end_time",    StringType()),
    StructField("ingested_at", StringType()),
])

def decode_crowd(code):
    return {"l": "LOW", "m": "MEDIUM", "h": "HIGH"}.get(code, "UNKNOWN")

crowd_udf = F.udf(decode_crowd, StringType())

def mrt_alert_level(crowd_text):
    if crowd_text == "HIGH":   return "ALERT"
    if crowd_text == "MEDIUM": return "WATCH"
    return "NORMAL"

mrt_alert_udf = F.udf(mrt_alert_level, StringType())

mrt_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOT) \
    .option("subscribe", "mrt-crowd") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

mrt_parsed = mrt_raw \
    .select(F.from_json(F.col("value").cast("string"),
                        mrt_schema).alias("d")) \
    .select("d.*") \
    .withColumn("ingested_at", F.to_timestamp("ingested_at")) \
    .withColumn("crowd_text",  crowd_udf(F.col("crowd_level"))) \
    .withColumn("alert_level", mrt_alert_udf(F.col("crowd_text"))) \
    .filter(F.col("station").isNotNull()) \
    .withWatermark("ingested_at", "15 minutes")

mrt_query = mrt_parsed.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT}/mrt") \
    .option("spark.mongodb.database", "transit_db") \
    .option("spark.mongodb.collection", "speed_mrt") \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

# Raw MRT vào MinIO
mrt_raw_query = mrt_parsed.writeStream \
    .format("parquet") \
    .option("checkpointLocation", f"{CHECKPOINT}/mrt-raw") \
    .option("path", f"{RAW_PATH}/mrt") \
    .outputMode("append") \
    .trigger(processingTime="120 seconds") \
    .start()

# ==============================================================
# PHẦN 3: CARPARK
# ==============================================================

carpark_schema = StructType([
    StructField("event_type",     StringType()),
    StructField("carpark_id",     StringType()),
    StructField("area",           StringType()),
    StructField("development",    StringType()),
    StructField("location",       StringType()),
    StructField("available_lots", IntegerType()),
    StructField("lot_type",       StringType()),
    StructField("agency",         StringType()),
    StructField("ingested_at",    StringType()),
])

def carpark_status(available):
    if available is None: return "UNKNOWN"
    if available == 0:    return "FULL"
    if available <= 10:   return "ALMOST_FULL"
    if available <= 50:   return "LIMITED"
    return "AVAILABLE"

carpark_status_udf = F.udf(carpark_status, StringType())

cp_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOT) \
    .option("subscribe", "carpark-lots") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

cp_parsed = cp_raw \
    .select(F.from_json(F.col("value").cast("string"),
                        carpark_schema).alias("d")) \
    .select("d.*") \
    .withColumn("ingested_at", F.to_timestamp("ingested_at")) \
    .withColumn("status", carpark_status_udf(F.col("available_lots"))) \
    .filter(F.col("carpark_id").isNotNull()) \
    .withWatermark("ingested_at", "3 minutes")

cp_query = cp_parsed.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT}/carpark") \
    .option("spark.mongodb.database", "transit_db") \
    .option("spark.mongodb.collection", "speed_carpark") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

cp_raw_query = cp_parsed.writeStream \
    .format("parquet") \
    .option("checkpointLocation", f"{CHECKPOINT}/carpark-raw") \
    .option("path", f"{RAW_PATH}/carpark") \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

# ==============================================================
# PHẦN 4: EV CHARGING
# ==============================================================

ev_schema = StructType([
    StructField("event_type",   StringType()),
    StructField("name",         StringType()),
    StructField("address",      StringType()),
    StructField("latitude",     DoubleType()),
    StructField("longitude",    DoubleType()),
    StructField("location_id",  StringType()),
    StructField("total_points", IntegerType()),
    StructField("available",    IntegerType()),
    StructField("occupied",     IntegerType()),
    StructField("operator",     StringType()),
    StructField("ingested_at",  StringType()),
])

def ev_availability(available, total):
    if total is None or total == 0: return "UNKNOWN"
    ratio = (available or 0) / total
    if ratio == 0:   return "FULL"
    if ratio < 0.3:  return "BUSY"
    if ratio < 0.7:  return "MODERATE"
    return "FREE"

ev_avail_udf = F.udf(ev_availability, StringType())

ev_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOT) \
    .option("subscribe", "ev-stations") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

ev_parsed = ev_raw \
    .select(F.from_json(F.col("value").cast("string"),
                        ev_schema).alias("d")) \
    .select("d.*") \
    .withColumn("ingested_at", F.to_timestamp("ingested_at")) \
    .withColumn("availability_level",
        ev_avail_udf(F.col("available"), F.col("total_points"))) \
    .filter(F.col("location_id").isNotNull()) \
    .withWatermark("ingested_at", "10 minutes")

ev_query = ev_parsed.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT}/ev") \
    .option("spark.mongodb.database", "transit_db") \
    .option("spark.mongodb.collection", "speed_ev") \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

# [Bug 3 FIX] Thêm raw write cho EV vào MinIO (bản gốc bị thiếu)
ev_raw_query = ev_parsed.writeStream \
    .format("parquet") \
    .option("checkpointLocation", f"{CHECKPOINT}/ev-raw") \
    .option("path", f"{RAW_PATH}/ev") \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

# ==============================================================
# PHẦN 5: TAXI
# ==============================================================

taxi_schema = StructType([
    StructField("event_type",  StringType()),
    StructField("latitude",    DoubleType()),
    StructField("longitude",   DoubleType()),
    StructField("ingested_at", StringType()),
])

taxi_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOT) \
    .option("subscribe", "taxi-positions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

taxi_parsed = taxi_raw \
    .select(F.from_json(F.col("value").cast("string"),
                        taxi_schema).alias("d")) \
    .select("d.*") \
    .withColumn("ingested_at", F.to_timestamp("ingested_at")) \
    .filter(F.col("latitude") != 0) \
    .withWatermark("ingested_at", "3 minutes")

taxi_agg = taxi_parsed \
    .groupBy(
        F.window("ingested_at", "2 minutes", "1 minute"),
        F.round("latitude",  2).alias("lat_grid"),
        F.round("longitude", 2).alias("lng_grid")
    ) \
    .agg(
        F.count("*").alias("taxi_count"),
        F.avg("latitude").alias("center_lat"),
        F.avg("longitude").alias("center_lng")
    ) \
    .withColumn("window_start", F.col("window.start")) \
    .withColumn("window_end",   F.col("window.end")) \
    .drop("window")

taxi_query = taxi_agg.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", f"{CHECKPOINT}/taxi") \
    .option("spark.mongodb.database", "transit_db") \
    .option("spark.mongodb.collection", "speed_taxi") \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

# [Bug 3 FIX] Thêm raw write cho Taxi vào MinIO (bản gốc bị thiếu)
taxi_raw_query = taxi_agg.writeStream \
    .format("parquet") \
    .option("checkpointLocation", f"{CHECKPOINT}/taxi-raw") \
    .option("path", f"{RAW_PATH}/taxi") \
    .outputMode("append") \
    .trigger(processingTime="60 seconds") \
    .start()

# ==============================================================
# Chờ tất cả queries
# ==============================================================
print("All streaming queries running...")
print(f"Checkpoint: {CHECKPOINT}")
print(f"Raw data:   {RAW_PATH}")
spark.streams.awaitAnyTermination()