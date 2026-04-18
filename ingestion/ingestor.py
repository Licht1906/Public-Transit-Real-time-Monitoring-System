"""
ingestor.py — LTA DataMall Ingestor
Poll 6 API nguồn, đẩy vào 6 Kafka topics.
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

LTA_KEY    = os.getenv("LTA_API_KEY", "your_key_here")
KAFKA_BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
BASE       = "https://datamall2.mytransport.sg/ltaodataservice"
HEADERS    = {"AccountKey": LTA_KEY, "accept": "application/json"}

BUS_HOT_STOPS = [
    "09022", "09048", "09057", "10199", "10221",
    "03219", "03239", "04167", "05011", "01119",
    "01012", "01019", "02049", "07371",
    "28031", "28059", "17009", "17091",
    "83139", "84009", "82009", "75009",
    "46211", "65199", "67759", "46009",
    "53121", "52009", "54009", "55001",
    "11009", "11071", "21009",
    "62009", "62101", "64009",
    "95009", "95129",
    "77009", "77131", "72009",
    "51009", "51079", "52119",
]

MRT_LINES = ["NSL", "EWL", "NEL", "CCL", "DTL", "TEL", "BPL"]

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

def fetch_bus_arrivals(stop_code):
    r = get(f"{BASE}/v3/BusArrival", params={"BusStopCode": stop_code})
    if not r:
        return []
    msgs = []
    for svc in r.json().get("Services", []):
        for visit in ["NextBus", "NextBus2", "NextBus3"]:
            b = svc.get(visit, {})
            if b.get("EstimatedArrival"):
                msgs.append({
                    "event_type":        "bus_arrival",
                    "bus_stop_code":     stop_code,
                    "service_no":        svc.get("ServiceNo"),
                    "operator":          svc.get("Operator"),
                    "estimated_arrival": b.get("EstimatedArrival"),
                    "monitored":         b.get("Monitored"),
                    "latitude":          float(b.get("Latitude", 0)),
                    "longitude":         float(b.get("Longitude", 0)),
                    "load":              b.get("Load"),
                    "bus_type":          b.get("Type"),
                    "visit_number":      visit,
                    "ingested_at":       now_utc(),
                })
    return msgs

def fetch_mrt_crowd(line):
    r = get(f"{BASE}/PCDRealTime", params={"TrainLine": line})
    if not r:
        return []
    msgs = []
    for item in r.json().get("value", []):
        msgs.append({
            "event_type":  "mrt_crowd",
            "train_line":  line,
            "station":     item.get("Station"),
            "crowd_level": item.get("CrowdLevel"),
            "ingested_at": now_utc(),
        })
    return msgs

def fetch_train_alerts():
    r = get(f"{BASE}/TrainServiceAlerts")
    if not r:
        return []
    data = r.json().get("value", {})
    msgs = []
    for seg in data.get("AffectedSegments", []):
        msgs.append({
            "event_type":  "train_alert",
            "line":        seg.get("Line"),
            "direction":   seg.get("Direction"),
            "stations":    seg.get("Stations"),
            "free_public": seg.get("FreePublicBus"),
            "free_mrt":    seg.get("FreeMRTShuttle"),
            "status":      data.get("Status"),
            "ingested_at": now_utc(),
        })
    if not msgs:
        msgs.append({
            "event_type":  "train_alert",
            "line":        "ALL",
            "status":      data.get("Status", 1),
            "ingested_at": now_utc(),
        })
    return msgs

def fetch_carpark():
    r = get(f"{BASE}/CarParkAvailabilityv2")
    if not r:
        return []
    msgs = []
    for item in r.json().get("value", []):
        msgs.append({
            "event_type":     "carpark",
            "carpark_id":     item.get("CarParkID"),
            "available_lots": item.get("AvailableLots"),
            "lot_type":       item.get("LotType"),
            "agency":         item.get("Agency"),
            "ingested_at":    now_utc(),
        })
    return msgs

def fetch_ev_batch():
    r = get(f"{BASE}/EVCBatch")
    if not r:
        return []
    msgs = []
    try:
        z = zipfile.ZipFile(io.BytesIO(r.content))
        for name in z.namelist():
            if name.endswith(".json"):
                data = json.loads(z.read(name))
                for item in data.get("value", []):
                    msgs.append({
                        "event_type":      "ev_station",
                        "location_id":     item.get("LocationID"),
                        "operator":        item.get("Operator"),
                        "available_lots":  item.get("AvailableLots"),
                        "total_lots":      item.get("TotalLots"),
                        "ingested_at":     now_utc(),
                    })
    except Exception as e:
        log.error(f"EV batch parse error: {e}")
    return msgs

def fetch_taxi():
    r = get(f"{BASE}/Taxi-Availability")
    if not r:
        return []
    msgs = []
    for item in r.json().get("value", []):
        msgs.append({
            "event_type":  "taxi",
            "latitude":    item.get("Latitude"),
            "longitude":   item.get("Longitude"),
            "ingested_at": now_utc(),
        })
    return msgs

def run():
    producer = make_producer()
    intervals = {
        "bus":     int(os.getenv("POLL_BUS_SECONDS",     "30")),
        "mrt":     int(os.getenv("POLL_MRT_SECONDS",     "600")),
        "alerts":  int(os.getenv("POLL_ALERTS_SECONDS",  "300")),
        "carpark": int(os.getenv("POLL_CARPARK_SECONDS", "60")),
        "ev":      int(os.getenv("POLL_EV_SECONDS",      "300")),
        "taxi":    int(os.getenv("POLL_TAXI_SECONDS",    "60")),
    }
    last = {k: 0 for k in intervals}

    while True:
        now = time.time()
        sent = 0

        if now - last["bus"] >= intervals["bus"]:
            for stop in BUS_HOT_STOPS:
                for msg in fetch_bus_arrivals(stop):
                    key = f"{msg['bus_stop_code']}-{msg['service_no']}"
                    producer.send("bus-arrivals", key=key, value=msg)
                    sent += 1
                time.sleep(0.1)
            last["bus"] = now

        if now - last["mrt"] >= intervals["mrt"]:
            for line in MRT_LINES:
                for msg in fetch_mrt_crowd(line):
                    producer.send("mrt-crowd", key=msg["station"], value=msg)
                    sent += 1
            last["mrt"] = now

        if now - last["alerts"] >= intervals["alerts"]:
            for msg in fetch_train_alerts():
                producer.send("train-alerts", key=msg.get("line", "ALL"), value=msg)
                sent += 1
            last["alerts"] = now

        if now - last["carpark"] >= intervals["carpark"]:
            for msg in fetch_carpark():
                producer.send("carpark-lots", key=msg["carpark_id"], value=msg)
                sent += 1
            last["carpark"] = now

        if now - last["ev"] >= intervals["ev"]:
            for msg in fetch_ev_batch():
                producer.send("ev-stations", key=msg["location_id"], value=msg)
                sent += 1
            last["ev"] = now

        if now - last["taxi"] >= intervals["taxi"]:
            for msg in fetch_taxi():
                producer.send("taxi-positions", key=None, value=msg)
                sent += 1
            last["taxi"] = now

        if sent > 0:
            producer.flush()
            log.info(f"Sent {sent} messages to Kafka")

        time.sleep(5)

if __name__ == "__main__":
    run()