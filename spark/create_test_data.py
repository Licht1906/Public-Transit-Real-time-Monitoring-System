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
