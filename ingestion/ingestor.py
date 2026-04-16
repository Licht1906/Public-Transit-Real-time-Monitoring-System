"""
ingestor.py — LTA DataMall Ingestor
Poll 6 API nguồn, đẩy vào 6 Kafka topics.

QUAN TRỌNG: Bus Arrival chỉ poll BUS_HOT_STOPS (~50 trạm đông nhất).
Không poll tất cả 5,000 trạm — sẽ bị rate limit.
ETA của các trạm khác được lấy on-demand qua FastAPI khi user click.
"""

import requests
import json
import time
import os
import logging
import zipfile
import io
from datetime import datetime, timezone

from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ---- Cấu hình ----
LTA_KEY    = os.getenv("LTA_API_KEY")
if not LTA_KEY:
    log.warning("Missing environment variable: LTA_API_KEY")
KAFKA_BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
BASE       = "https://datamall2.mytransport.sg/ltaodataservice"
HEADERS    = {"AccountKey": LTA_KEY, "accept": "application/json"}

# ~50 trạm đông nhất Singapore — đủ để Big Data pipeline có data thực
# (Central, Orchard, Bugis, Raffles Place, Jurong, Tampines, Woodlands...)
BUS_HOT_STOPS = [
    # Orchard / Somerset
    "09022", "09048", "09057", "10199", "10221",
    # Raffles Place / City Hall / Marina
    "03219", "03239", "04167", "05011", "01119",
    # Bugis / Lavender
    "01012", "01019", "02049", "07371",
    # Jurong East / Clementi
    "28031", "28059", "17009", "17091",
    # Tampines / Bedok
    "83139", "84009", "82009", "75009",
    # Woodlands / Yishun
    "46211", "65199", "67759", "46009",
    # Bishan / Ang Mo Kio
    "53121", "52009", "54009", "55001",
    # Queenstown / Buona Vista
    "11009", "11071", "21009",
    # Serangoon / Hougang
    "62009", "62101", "64009",
    # Changi / Airport
    "95009", "95129",
    # Punggol / Sengkang
    "77009", "77131", "72009",
    # Toa Payoh / Novena
    "51009", "51079", "52119",
]

MRT_LINES = ["NSL", "EWL", "NEL", "CCL", "DTL", "TEL", "BPL"]

# ---- Kafka producer ----
def make_producer():
    for attempt in range(5):
        try:
            p = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOT],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=10
            )
            log.info("Kafka connected")
            return p
        except Exception as e:
            log.warning(f"Kafka attempt {attempt+1}/5: {e}")
            time.sleep(5)
    raise RuntimeError("Cannot connect to Kafka")

# ---- Helpers ----
def get(url, params=None):
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=10)
        r.raise_for_status()
        return r
    except requests.HTTPError as e:
        log.error(f"HTTP {r.status_code} {url}: {e}")
    except Exception as e:
        log.error(f"Request error {url}: {e}")
    return None

def now_utc():
    return datetime.now(timezone.utc).isoformat()

# ---- Fetch functions ----

def fetch_bus_arrivals(stop_code):
    """
    ETA xe buýt tại một trạm.
    Chỉ gọi cho các trạm trong BUS_HOT_STOPS.
    ETA của trạm khác → FastAPI gọi on-demand khi user click.
    """
    r = get(f"{BASE}/v3/BusArrival", {"BusStopCode": stop_code})
    if not r:
        return []
    msgs = []
    ts = now_utc()
    for svc in r.json().get("Services", []):
        for bus_key in ["NextBus", "NextBus2", "NextBus3"]:
            bus = svc.get(bus_key, {})
            if not bus.get("EstimatedArrival"):
                continue
            msgs.append({
                "event_type":        "bus_arrival",
                "bus_stop_code":     stop_code,
                "service_no":        svc.get("ServiceNo", ""),
                "operator":          svc.get("Operator", ""),
                "estimated_arrival": bus["EstimatedArrival"],
                "monitored":         bus.get("Monitored", 0),
                "latitude":          float(bus.get("Latitude") or 0),
                "longitude":         float(bus.get("Longitude") or 0),
                "load":              bus.get("Load", ""),
                "bus_type":          bus.get("Type", ""),
                "feature":           bus.get("Feature", ""),
                "visit_number":      bus_key,
                "ingested_at":       ts
            })
    return msgs


def fetch_mrt_crowd(train_line):
    """
    Mật độ hành khách real-time.
    CrowdLevel: 'l'=low, 'm'=moderate, 'h'=high, 'NA'=unknown
    Giữ nguyên ký tự LTA trả về — Spark sẽ decode trong UDF.
    """
    r = get(f"{BASE}/PCDRealTime", {"TrainLine": train_line})
    if not r:
        return []
    ts = now_utc()
    msgs = []
    for s in r.json().get("value", []):
        msgs.append({
            "event_type":  "mrt_crowd",
            "train_line":  train_line,
            "station":     s.get("Station", ""),
            "start_time":  s.get("StartTime", ""),
            "end_time":    s.get("EndTime", ""),
            "crowd_level": s.get("CrowdLevel", "NA"),
            "ingested_at": ts
        })
    return msgs


def fetch_train_alerts():
    """Cảnh báo gián đoạn tàu điện"""
    r = get(f"{BASE}/TrainServiceAlerts")
    if not r:
        return []
    value = r.json().get("value", {})
    status = value.get("Status", 1)
    msgs = []
    ts = now_utc()
    for seg in value.get("AffectedSegments", []):
        msgs.append({
            "event_type":  "train_alert",
            "status":      status,
            "line":        seg.get("Line", ""),
            "direction":   seg.get("Direction", ""),
            "stations":    seg.get("Stations", ""),
            "free_bus":    seg.get("FreePublicBus", ""),
            "ingested_at": ts
        })
    if not msgs and status == 2:
        msgs.append({"event_type": "train_alert", "status": 2,
                     "line": "UNKNOWN", "ingested_at": ts})
    return msgs


def fetch_carpark():
    """Số chỗ trống bãi đỗ xe toàn Singapore — dùng pagination"""
    all_records = []
    skip = 0
    while True:
        r = get(f"{BASE}/CarParkAvailabilityv2",
                {"$skip": skip} if skip else None)
        if not r:
            break
        records = r.json().get("value", [])
        if not records:
            break
        all_records.extend(records)
        skip += 500
        if len(records) < 500:
            break
    ts = now_utc()
    msgs = []
    for cp in all_records:
        msgs.append({
            "event_type":     "carpark",
            "carpark_id":     cp.get("CarParkID", ""),
            "area":           cp.get("Area", ""),
            "development":    cp.get("Development", ""),
            "location":       cp.get("Location", ""),
            "available_lots": cp.get("AvailableLots", 0),
            "lot_type":       cp.get("LotType", "C"),
            "agency":         cp.get("Agency", ""),
            "ingested_at":    ts
        })
    return msgs


def fetch_ev_batch():
    """
    Toàn bộ trạm sạc EV Singapore.
    Dùng EVCBatch (trả về 1 file ZIP) thay EVChargingPoints (cần PostalCode từng lần).
    """
    r = get(f"{BASE}/EVCBatch")
    if not r:
        return []
    link = r.json().get("value", [{}])[0].get("Link", "")
    if not link:
        return []
    try:
        data_r = requests.get(link, timeout=30)
        data_r.raise_for_status()

        # File có thể là ZIP hoặc JSON trực tiếp
        content_type = data_r.headers.get("content-type", "")
        if "zip" in content_type or link.endswith(".zip"):
            zf = zipfile.ZipFile(io.BytesIO(data_r.content))
            fname = zf.namelist()[0]
            stations = json.loads(zf.read(fname).decode("utf-8"))
        else:
            stations = data_r.json()

        ts = now_utc()
        msgs = []
        for s in (stations if isinstance(stations, list) else [stations]):
            msgs.append({
                "event_type":   "ev_station",
                "location_id":  str(s.get("GeneralLocation", s.get("PostalCode", ""))),
                "name":         s.get("GeneralLocation", ""),
                "address":      s.get("Address", ""),
                "latitude":     float(s.get("Latitude") or 0),
                "longitude":    float(s.get("Longitude") or 0),
                "total_points": int(s.get("EVCount", 0)),
                "available":    int(s.get("Available", 0)),
                "occupied":     int(s.get("Occupied", 0)),
                "operator":     s.get("Operator", ""),
                "ingested_at":  ts
            })
        return msgs
    except Exception as e:
        log.error(f"EV batch error: {e}")
        return []


def fetch_taxi():
    """Vị trí taxi đang rảnh (sẵn sàng đón khách) toàn Singapore"""
    r = get(f"{BASE}/Taxi-Availability")
    if not r:
        return []
    ts = now_utc()
    msgs = []
    features = r.json().get("features", [])
    for feat in features:
        coords = feat.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            msgs.append({
                "event_type":  "taxi",
                "longitude":   float(coords[0]),
                "latitude":    float(coords[1]),
                "ingested_at": ts
            })
    return msgs


# ---- Main loop ----
def run():
    producer = make_producer()
    log.info(f"Ingestor started — polling {len(BUS_HOT_STOPS)} hot stops for bus")
    log.info("Note: 5,000 trạm static đã có trong MongoDB (load_static.py)")
    log.info("      ETA on-demand (user click) đi qua FastAPI trực tiếp")

    intervals = {
        "bus":     int(os.getenv("POLL_BUS_SECONDS",     "30")),
        "mrt":     int(os.getenv("POLL_MRT_SECONDS",     "600")),
        "alerts":  int(os.getenv("POLL_MRT_SECONDS",     "300")),
        "carpark": int(os.getenv("POLL_CARPARK_SECONDS", "60")),
        "ev":      int(os.getenv("POLL_EV_SECONDS",      "300")),
        "taxi":    int(os.getenv("POLL_TAXI_SECONDS",    "60")),
    }
    last = {k: 0 for k in intervals}

    while True:
        now = time.time()
        sent = 0

        # Bus — chỉ poll BUS_HOT_STOPS (~50 trạm)
        if now - last["bus"] >= intervals["bus"]:
            for stop in BUS_HOT_STOPS:
                for msg in fetch_bus_arrivals(stop):
                    key = f"{msg['bus_stop_code']}-{msg['service_no']}"
                    producer.send("bus-arrivals", key=key, value=msg)
                    sent += 1
                time.sleep(0.1)   # tránh rate limit (50 trạm × 0.1s = 5 giây)
            last["bus"] = now

        # MRT crowd — poll mỗi 10 phút
        if now - last["mrt"] >= intervals["mrt"]:
            for line in MRT_LINES:
                for msg in fetch_mrt_crowd(line):
                    producer.send("mrt-crowd", key=msg["station"], value=msg)
                    sent += 1
            last["mrt"] = now

        # Train alerts — poll mỗi 5 phút
        if now - last["alerts"] >= intervals["alerts"]:
            for msg in fetch_train_alerts():
                producer.send("train-alerts", key=msg.get("line", "ALL"), value=msg)
                sent += 1
            last["alerts"] = now

        # Carpark — poll mỗi 1 phút
        if now - last["carpark"] >= intervals["carpark"]:
            for msg in fetch_carpark():
                producer.send("carpark-lots", key=msg["carpark_id"], value=msg)
                sent += 1
            last["carpark"] = now

        # EV — poll mỗi 5 phút
        if now - last["ev"] >= intervals["ev"]:
            for msg in fetch_ev_batch():
                producer.send("ev-stations", key=msg["location_id"], value=msg)
                sent += 1
            last["ev"] = now

        # Taxi — poll mỗi 1 phút
        if now - last["taxi"] >= intervals["taxi"]:
            for msg in fetch_taxi():
                producer.send("taxi-positions", key=None, value=msg)
                sent += 1
            last["taxi"] = now

        if sent > 0:
            producer.flush()
            log.info(f"Sent {sent} messages to Kafka")

        time.sleep(5)   # check lại sau 5 giây


if __name__ == "__main__":
    run()