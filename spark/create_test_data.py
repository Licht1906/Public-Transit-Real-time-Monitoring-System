"""
Create a small local/MinIO-compatible sample dataset for the batch layer.

Run this after port-forwarding MinIO:
  kubectl port-forward svc/minio 9000:9000 -n data

The script writes Parquet files under:
  s3a://sg-transit-data/raw/bus/sample.parquet
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone
from io import BytesIO

from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq


MINIO_ENDPOINT = os.getenv("MINIO_EXTERNAL_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "sg-transit-data")

SERVICES = ["14", "36", "65", "106", "111", "167", "190", "502"]
STOPS = ["83139", "09048", "01012", "02049", "08057", "17009", "44259"]


def ensure_bucket(client: Minio) -> None:
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)


def build_rows() -> list[dict[str, object]]:
    base = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(days=2)
    rows: list[dict[str, object]] = []
    for hour_offset in range(48):
        window_start = base + timedelta(hours=hour_offset)
        for stop in STOPS:
            for service in SERVICES:
                eta = random.randint(90, 1500)
                rows.append(
                    {
                        "bus_stop_code": stop,
                        "service_no": service,
                        "operator": "SBST" if int(service) % 2 else "SMRT",
                        "eta_seconds": eta,
                        "load": random.choice(["LOW", "MEDIUM", "HIGH"]),
                        "window_start": window_start,
                        "ingested_at": window_start + timedelta(minutes=5),
                    }
                )
    return rows


def upload_parquet(client: Minio, object_name: str, rows: list[dict[str, object]]) -> None:
    table = pa.Table.from_pylist(rows)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    payload = buffer.getvalue()
    client.put_object(
        MINIO_BUCKET,
        object_name,
        BytesIO(payload),
        length=len(payload),
        content_type="application/octet-stream",
    )


def main() -> None:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    ensure_bucket(client)
    rows = build_rows()
    object_name = "raw/bus/sample.parquet"
    upload_parquet(client, object_name, rows)
    print(f"Uploaded {len(rows)} rows to s3://{MINIO_BUCKET}/{object_name}")


if __name__ == "__main__":
    main()
