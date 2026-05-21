"""
cleanup.py — Dọn dẹp data cũ hơn 3 tiếng
Vị trí: big_data/scripts/cleanup.py
Chạy mỗi tiếng 1 lần qua Kubernetes CronJob

Những gì BỊ XÓA:
  - MongoDB speed_*     : documents cũ hơn 3 tiếng
  - MinIO raw/*         : Parquet files cũ hơn 3 tiếng

Những gì KHÔNG đụng vào:
  - MinIO checkpoints/  : Spark Streaming cần để restart đúng offset
  - MinIO models/       : MLlib model đã train
  - MinIO jobs/         : file .py của Spark jobs
  - MongoDB batch_*     : batch views lịch sử cho Grafana
  - MongoDB *_static    : 5,000 trạm bus, routes tĩnh
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient
from minio import Minio
from minio.deleteobjects import DeleteObject
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ---- Cấu hình ----
MONGO_URI      = os.getenv("MONGODB_URI",
                   "mongodb://root:Transit@2024@mongodb.data.svc.cluster.local:27017")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",
                   "minio.data.svc.cluster.local:9000")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "sg-transit-data")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
KEEP_HOURS     = int(os.getenv("CLEANUP_KEEP_HOURS", "1"))

cutoff = datetime.now(timezone.utc) - timedelta(hours=KEEP_HOURS)
log.info(f"=== Cleanup started ===")
log.info(f"Cutoff: {cutoff.isoformat()} (giữ {KEEP_HOURS} tiếng gần nhất)")

# ============================================================
# 1. Dọn MongoDB — speed views cũ hơn 3 tiếng
# ============================================================
log.info("--- MongoDB cleanup ---")
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db     = client["transit_db"]

    # Chỉ xóa speed views — không đụng batch_* và *_static
    speed_collections = [
        "speed_bus",
        "speed_mrt",
        "speed_carpark",
        "speed_ev",
        "speed_taxi",
    ]

    total_mongo = 0
    for coll_name in speed_collections:
        try:
            result = db[coll_name].delete_many({
                "ingested_at": {"$lt": cutoff}
            })
            total_mongo += result.deleted_count
            log.info(f"  {coll_name}: xóa {result.deleted_count} documents")
        except Exception as e:
            log.warning(f"  {coll_name}: skip ({e})")

    log.info(f"MongoDB: tổng {total_mongo} documents đã xóa")
    client.close()

except Exception as e:
    log.error(f"MongoDB kết nối thất bại: {e}")

# ============================================================
# 2. Dọn MinIO — raw Parquet files cũ hơn 3 tiếng
# ============================================================
log.info("--- MinIO cleanup ---")
try:
    endpoint = MINIO_ENDPOINT \
        .replace("http://", "") \
        .replace("https://", "")

    minio_client = Minio(
        endpoint,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False
    )

    # Chỉ dọn raw/ — không đụng checkpoints/, models/, jobs/
    raw_prefixes = [
        "raw/bus/",
        "raw/mrt/",
        "raw/carpark/",
        "raw/ev/",
        "raw/taxi/",
    ]

    total_minio = 0

    for prefix in raw_prefixes:
        to_delete = []

        try:
            objects = minio_client.list_objects(
                MINIO_BUCKET,
                prefix=prefix,
                recursive=True
            )

            for obj in objects:
                if obj.last_modified and obj.last_modified < cutoff:
                    to_delete.append(obj.object_name)

        except S3Error as e:
            log.warning(f"  {prefix}: list lỗi ({e}), bỏ qua")
            continue

        if not to_delete:
            log.info(f"  {prefix}: không có file cũ")
            continue

        # Xóa theo batch 100 objects
        BATCH = 100
        deleted = 0
        for i in range(0, len(to_delete), BATCH):
            batch = to_delete[i:i + BATCH]
            errors = list(minio_client.remove_objects(
                MINIO_BUCKET,
                [DeleteObject(name) for name in batch]
            ))
            deleted += len(batch) - len(errors)
            if errors:
                for err in errors:
                    log.warning(f"    Lỗi xóa {err.name}: {err.message}")

        total_minio += deleted
        log.info(f"  {prefix}: xóa {deleted}/{len(to_delete)} files")

    log.info(f"MinIO: tổng {total_minio} files đã xóa")

except Exception as e:
    log.error(f"MinIO kết nối thất bại: {e}")

log.info("=== Cleanup hoàn thành ===")