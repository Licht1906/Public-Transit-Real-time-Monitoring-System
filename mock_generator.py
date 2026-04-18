"""
Mock data cho Member C test Spark Streaming.
Không cần LTA API key thật, không cần Kafka thật trên GKE.
Chỉ cần Docker Kafka local của Member C.
"""
import json, time, random
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

BUS_STOPS = ["83139","84009","09022","10199","15239",
             "03219","46211","53121","77009","28031"]
SERVICES  = ["15", "65", "190", "851", "NR8", "7", "14"]
LOADS     = ["SEA", "SDA", "LSD"]
STATIONS  = ["NS1", "NS2", "EW1", "EW2", "CC1", "DT1", "TE1"]
LINES     = ["NSL", "EWL", "CCL"]
CROWD     = ["l", "m", "h"]

while True:
    ts = datetime.now(timezone.utc)

    for _ in range(30):
        eta = ts + timedelta(seconds=random.randint(60, 900))
        if random.random() < 0.05:
            eta -= timedelta(minutes=4)
        producer.send("bus-arrivals", value={
            "event_type":        "bus_arrival",
            "bus_stop_code":     random.choice(BUS_STOPS),
            "service_no":        random.choice(SERVICES),
            "operator":          "GAS",
            "estimated_arrival": eta.isoformat(),
            "monitored":         1,
            "latitude":          1.3 + random.uniform(-0.05, 0.05),
            "longitude":         103.8 + random.uniform(-0.05, 0.05),
            "load":              random.choice(LOADS),
            "bus_type":          "SD",
            "visit_number":      "NextBus",
            "ingested_at":       ts.isoformat()
        })

    for station in STATIONS:
        producer.send("mrt-crowd", value={
            "event_type":  "mrt_crowd",
            "train_line":  random.choice(LINES),
            "station":     station,
            "crowd_level": random.choice(CROWD),
            "ingested_at": ts.isoformat()
        })

    for i in range(10):
        producer.send("carpark-lots", value={
            "event_type":     "carpark",
            "carpark_id":     f"CP{i:03d}",
            "available_lots": random.randint(0, 200),
            "lot_type":       "C",
            "agency":         random.choice(["HDB", "LTA", "URA"]),
            "ingested_at":    ts.isoformat()
        })

    for _ in range(50):
        producer.send("taxi-positions", value={
            "event_type":  "taxi",
            "latitude":    1.3521 + random.uniform(-0.1, 0.1),
            "longitude":   103.8198 + random.uniform(-0.1, 0.1),
            "ingested_at": ts.isoformat()
        })

    producer.flush()
    print(f"Mock: sent bus+mrt+carpark+taxi at {ts.isoformat()}")
    time.sleep(5)