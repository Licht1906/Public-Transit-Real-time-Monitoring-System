"""
load_static.py — Tải Bus Stops, Bus Routes, Bus Services từ LTA vào MongoDB
Chạy 1 lần duy nhất. Data này ít thay đổi.

Sau khi chạy xong, Frontend có thể gọi GET /bus/stops để lấy 5,000 trạm
và vẽ lên bản đồ mà không cần gọi LTA liên tục.
"""
import requests
import pymongo
import os
import time

KEY    = os.getenv("LTA_API_KEY")
if not KEY:
    raise ValueError("Missing environment variable: LTA_API_KEY")

BASE   = "https://datamall2.mytransport.sg/ltaodataservice"
HDRS   = {"AccountKey": KEY}
client = pymongo.MongoClient(os.getenv("MONGODB_URI",
         "mongodb://root:Transit%402024@localhost:27017"))
db     = client["transit_db"]

def fetch_all(endpoint):
    """Lấy toàn bộ records với pagination ($skip)"""
    records, skip = [], 0
    while True:
        params = {"$skip": skip} if skip else {}
        r = requests.get(f"{BASE}/{endpoint}", headers=HDRS,
                         params=params, timeout=15)
        data = r.json().get("value", [])
        if not data:
            break
        records.extend(data)
        print(f"  {endpoint}: {len(records)} records loaded...")
        skip += 500
        time.sleep(0.3)   # lịch sự với server LTA
    return records

# Tải 3 loại static data
for endpoint, coll, indexes in [
    ("BusStops",    "bus_stops_static",    ["BusStopCode"]),
    ("BusRoutes",   "bus_routes_static",   ["ServiceNo", "BusStopCode"]),
    ("BusServices", "bus_services_static", ["ServiceNo"]),
]:
    print(f"\nLoading {coll}...")
    data = fetch_all(endpoint)
    db[coll].drop()
    if data:
        db[coll].insert_many(data)
        for field in indexes:
            db[coll].create_index(field)
        # Index tọa độ cho BusStops (để query nearby về sau)
        if coll == "bus_stops_static":
            db[coll].create_index([("Latitude", 1), ("Longitude", 1)])
    print(f"  Done: {len(data)} records, indexes created")

print("\n✓ Static data loaded! Frontend có thể dùng ngay.")
client.close()