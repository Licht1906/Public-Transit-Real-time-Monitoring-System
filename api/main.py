"""
FastAPI Serving Layer — Thành viên E

3 loại data source:
1. MongoDB bus_stops_static → GET /bus/stops (5,000 trạm, 1 lần)
2. LTA API trực tiếp (on-demand) → GET /bus/arrivals/{code} (khi user click)
3. MongoDB speed_* (Spark Streaming ghi) → MRT / Carpark / EV / Taxi
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pymongo import MongoClient
import redis
import httpx        # async HTTP client để gọi LTA API
import json
import os
import math
from typing import Optional
from datetime import datetime, timezone

MONGO_URI  = os.getenv("MONGODB_URI",
    "mongodb://root:Transit%402024@mongodb.data.svc.cluster.local:27017")
REDIS_HOST = os.getenv("REDIS_HOST",
    "redis-master.data.svc.cluster.local")
REDIS_PASS = os.getenv("REDIS_PASSWORD", "Redis@2024")
LTA_KEY    = os.getenv("LTA_API_KEY", "your_key")
LTA_BASE   = "https://datamall2.mytransport.sg/ltaodataservice"

mongo = MongoClient(MONGO_URI)
db    = mongo["transit_db"]
r     = redis.Redis(host=REDIS_HOST, password=REDIS_PASS,
                    decode_responses=True, socket_timeout=2)

app = FastAPI(title="SG Transit API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["GET"], allow_headers=["*"])


app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return FileResponse("static/index.html")
# ============================================================
# BUS ENDPOINT A: Tải 5,000 trạm (1 lần khi mở web)
# ============================================================

@app.get("/bus/stops")
async def get_all_stops():
    """
    Trả về tất cả bus stops để Frontend vẽ map.
    Đọc từ MongoDB bus_stops_static — dữ liệu tĩnh, không gọi LTA.
    Frontend gọi 1 lần duy nhất khi mở web, cache lại trong browser.
    """
    # Thử Redis cache trước (TTL 1 tiếng — data tĩnh, ít thay đổi)
    cached = None
    try:
        cached = r.get("bus:stops:all")
    except Exception:
        pass

    if cached:
        return {"source": "cache", "count": json.loads(cached)["count"],
                "stops": json.loads(cached)["stops"]}

    stops = list(db.bus_stops_static.find(
        {},
        {"_id": 0, "BusStopCode": 1, "RoadName": 1,
         "Description": 1, "Latitude": 1, "Longitude": 1}
    ).limit(5000))

    result = {"count": len(stops), "stops": stops}

    # Cache 1 tiếng
    try:
        r.setex("bus:stops:all", 3600, json.dumps(result))
    except Exception:
        pass

    return {"source": "mongodb", **result}


# ============================================================
# BUS ENDPOINT C: PageRank Top 10 (GraphFrames)
# ============================================================

@app.get("/bus/pagerank")
async def get_pagerank():
    """
    Lấy Top 10 trạm xe bus quan trọng nhất phân tích bởi GraphFrames.
    """
    try:
        docs = list(db["batch_graph_pagerank"].find({}, {"_id": 0}).sort("importance", -1).limit(10))
        return docs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# BUS ENDPOINT B: ETA on-demand (khi user click một trạm)
# ============================================================

@app.get("/bus/arrivals/{bus_stop_code}")
async def get_bus_arrivals(bus_stop_code: str):
    """
    Lấy ETA xe buýt tại trạm — on-demand khi user click icon trạm.

    Luồng:
    1. Kiểm tra Redis cache (TTL 20 giây)
    2. Nếu cache miss → gọi thẳng LTA BusArrival API
    3. Parse kết quả → lưu Redis → trả về Frontend

    Tại sao không đọc MongoDB speed_bus?
    → speed_bus chỉ có ~50 trạm đông nhất (từ ingestor Big Data pipeline)
    → User có thể click bất kỳ trạm nào trong 5,000 trạm
    → On-demand đảm bảo mọi trạm đều có data, luôn fresh
    """
    cache_key = f"bus:arrivals:{bus_stop_code}"

    # 1. Thử Redis cache
    try:
        cached = r.get(cache_key)
        if cached:
            return {"source": "cache", "bus_stop_code": bus_stop_code,
                    "data": json.loads(cached)}
    except Exception:
        pass

    # 2. Gọi LTA BusArrival API trực tiếp
    lta_url = f"{LTA_BASE}/v3/BusArrival"
    lta_headers = {"AccountKey": LTA_KEY, "accept": "application/json"}

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(lta_url,
                                    headers=lta_headers,
                                    params={"BusStopCode": bus_stop_code})
            resp.raise_for_status()
            lta_data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(504, "LTA API timeout — thử lại sau vài giây")
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"LTA API error: {e.response.status_code}")
    except Exception as e:
        raise HTTPException(502, f"Cannot reach LTA API: {str(e)}")

    # 3. Parse kết quả LTA
    services = lta_data.get("Services", [])
    if not services:
        raise HTTPException(404, f"No buses found for stop {bus_stop_code}")

    now_ts = datetime.utcnow()
    formatted = []
    for svc in services:
        buses = []
        for bus_key in ["NextBus", "NextBus2", "NextBus3"]:
            bus = svc.get(bus_key, {})
            eta_str = bus.get("EstimatedArrival", "")
            if not eta_str:
                continue

            # Tính phút còn lại
            try:
                # Removed local import to fix UnboundLocalError
                eta_dt = datetime.fromisoformat(eta_str)
                if eta_dt.tzinfo is None:
                    eta_dt = eta_dt.replace(tzinfo=timezone.utc)
                now_aware = datetime.now(timezone.utc)
                eta_minutes = round((eta_dt - now_aware).total_seconds() / 60, 1)
                eta_minutes = max(0, eta_minutes)
            except Exception:
                eta_minutes = None

            load_map = {"SEA": "LOW", "SDA": "MEDIUM", "LSD": "HIGH"}
            buses.append({
                "eta_minutes": eta_minutes,
                "estimated_arrival": eta_str,
                "load": load_map.get(bus.get("Load", ""), "UNKNOWN"),
                "load_code": bus.get("Load", ""),
                "lat": float(bus.get("Latitude") or 0),
                "lng": float(bus.get("Longitude") or 0),
                "monitored": bus.get("Monitored", 0),
                "type": bus.get("Type", ""),
                "feature": bus.get("Feature", ""),
            })

        if buses:
            formatted.append({
                "service_no": svc.get("ServiceNo", ""),
                "operator":   svc.get("Operator", ""),
                "buses":      buses,
            })

    # 4. Cache 20 giây (ETA cập nhật mỗi 20s — cache quá lâu sẽ stale)
    try:
        r.setex(cache_key, 20, json.dumps(formatted))
    except Exception:
        pass

    return {
        "source":        "lta_api",
        "bus_stop_code": bus_stop_code,
        "services_count": len(formatted),
        "data":          formatted
    }


@app.get("/bus/stop/{bus_stop_code}/info")
async def get_stop_info(bus_stop_code: str):
    """Thông tin trạm: tên, địa chỉ, tuyến đi qua — từ static data"""
    stop = db.bus_stops_static.find_one(
        {"BusStopCode": bus_stop_code},
        {"_id": 0}
    )
    if not stop:
        raise HTTPException(404, f"Stop {bus_stop_code} not found")

    # Các tuyến đi qua trạm này
    routes = list(db.bus_routes_static.find(
        {"BusStopCode": bus_stop_code},
        {"_id": 0, "ServiceNo": 1, "Direction": 1, "StopSequence": 1}
    ).sort("ServiceNo", 1))

    return {
        "stop":    stop,
        "routes":  routes,
        "services": sorted(set(r["ServiceNo"] for r in routes))
    }


# ============================================================
# MRT ENDPOINTS — đọc từ speed_mrt (Spark Streaming ghi)
# ============================================================

@app.get("/mrt/crowd/{train_line}")
async def get_mrt_crowd(train_line: str):
    """Mật độ hành khách real-time theo tuyến MRT"""
    valid = ["NSL", "EWL", "NEL", "CCL", "DTL", "TEL", "BPL"]
    if train_line not in valid:
        raise HTTPException(400, f"Invalid line. Use: {valid}")

    docs = list(db.speed_mrt
        .find({"train_line": train_line}, {"_id": 0})
        .sort("ingested_at", -1).limit(50))

    # Chỉ lấy record mới nhất mỗi ga
    latest = {}
    for d in docs:
        s = d.get("station")
        if s and s not in latest:
            latest[s] = {
                "station":     s,
                "crowd_text":  d.get("crowd_text", "UNKNOWN"),
                "alert_level": d.get("alert_level", "NORMAL"),
                "updated_at":  str(d.get("ingested_at", ""))
            }

    return {
        "train_line": train_line,
        "stations":   list(latest.values()),
        "high_count": sum(1 for s in latest.values()
                          if s["crowd_text"] == "HIGH")
    }


@app.get("/mrt/alerts")
async def get_train_alerts():
    """Cảnh báo gián đoạn tàu điện"""
    docs = list(db.speed_mrt
        .find({"event_type": "train_alert"}, {"_id": 0})
        .sort("ingested_at", -1).limit(20))
    return {"alerts": docs, "count": len(docs)}


# ============================================================
# CARPARK ENDPOINTS — đọc từ speed_carpark
# ============================================================

@app.get("/carpark")
async def get_carparks(
    agency: Optional[str] = Query(None, description="HDB, LTA, URA"),
    min_lots: int = Query(0, description="Minimum available lots"),
    lot_type: str = Query("C", description="C=car, Y=motorcycle, H=heavy")
):
    """Danh sách bãi đỗ xe + số chỗ trống"""
    query = {"lot_type": lot_type}
    if agency:
        query["agency"] = agency.upper()
    if min_lots > 0:
        query["available_lots"] = {"$gte": min_lots}

    docs = list(db.speed_carpark
        .find(query, {"_id": 0})
        .sort("ingested_at", -1)
        .limit(1000))

    # Chỉ lấy record mới nhất mỗi carpark
    latest = {}
    for d in docs:
        cp = d.get("carpark_id")
        if cp and cp not in latest:
            # Parse Location field (e.g., "1.3521 103.8198")
            location_str = d.get("location") or d.get("Location") or ""
            lat, lng = None, None
            if location_str:
                try:
                    parts = location_str.split()
                    if len(parts) >= 2:
                        lat = float(parts[0])
                        lng = float(parts[1])
                except Exception:
                    pass

            latest[cp] = {
                "carpark_id":     cp,
                "development":    d.get("development", ""),
                "area":           d.get("area", ""),
                "agency":         d.get("agency", ""),
                "available_lots": d.get("available_lots", 0),
                "status":         d.get("status", "UNKNOWN"),
                "lot_type":       d.get("lot_type", "C"),
                "latitude":       lat or d.get("latitude") or d.get("Latitude"),
                "longitude":      lng or d.get("longitude") or d.get("Longitude"),
                "updated_at":     str(d.get("ingested_at", ""))
            }

    result = list(latest.values())
    # Sắp xếp: nhiều chỗ trống lên trên
    result.sort(key=lambda x: x["available_lots"], reverse=True)

    return {
        "count":     len(result),
        "carparks":  result,
        "full_count": sum(1 for c in result if c["available_lots"] == 0)
    }


# ============================================================
# EV CHARGING ENDPOINTS — đọc từ speed_ev
# ============================================================

@app.get("/ev/stations")
async def get_ev_stations(
    lat: Optional[float] = Query(None, description="Latitude người dùng"),
    lng: Optional[float] = Query(None, description="Longitude người dùng"),
    radius_km: float = Query(5.0, description="Bán kính tìm kiếm (km)")
):
    """
    Tìm trạm sạc EV.
    Nếu có lat/lng → lọc theo bán kính.
    Nếu không → trả về tất cả.
    """
    docs = list(db.speed_ev
        .find({}, {"_id": 0})
        .sort("ingested_at", -1)
        .limit(2000))

    # Chỉ lấy record mới nhất mỗi trạm
    latest = {}
    for d in docs:
        lid = d.get("location_id")
        if lid and lid not in latest:
            latest[lid] = d

    stations = list(latest.values())

    # Lọc theo bán kính nếu có tọa độ
    if lat is not None and lng is not None:
        def haversine(lat1, lng1, lat2, lng2):
            """Tính khoảng cách km giữa 2 tọa độ"""
            R = 6371
            dlat = math.radians(lat2 - lat1)
            dlng = math.radians(lng2 - lng1)
            a = math.sin(dlat/2)**2 + \
                math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
                math.sin(dlng/2)**2
            return R * 2 * math.asin(math.sqrt(a))

        stations = [
            s for s in stations
            if s.get("latitude") and s.get("longitude") and
               haversine(lat, lng, s["latitude"], s["longitude"]) <= radius_km
        ]
        # Sắp xếp theo khoảng cách
        stations.sort(key=lambda s: haversine(lat, lng, s["latitude"], s["longitude"]))

    return {
        "count":    len(stations),
        "stations": stations
    }


# ============================================================
# TAXI ENDPOINT — đọc từ speed_taxi (grid aggregation)
# ============================================================

@app.get("/taxi/positions")
async def get_taxi_positions():
    """
    Vị trí taxi rảnh theo lưới địa lý.
    Mỗi điểm trên map = 1 ô lưới (~1km²) với số lượng taxi rảnh.
    Frontend vẽ dots lên map — size dot tỷ lệ với taxi_count.
    """
    docs = list(db.speed_taxi
        .find({}, {"_id": 0})
        .sort("window_start", -1)
        .limit(500))

    # Chỉ lấy window mới nhất mỗi ô lưới
    latest_window = docs[0].get("window_start") if docs else None
    if latest_window:
        docs = [d for d in docs if d.get("window_start") == latest_window]

    grid_cells = [
        {
            "lat":         d.get("center_lat"),
            "lng":         d.get("center_lng"),
            "taxi_count":  d.get("taxi_count", 0),
        }
        for d in docs
        if d.get("center_lat") and d.get("center_lng")
    ]

    return {
        "total_nearby":  sum(c["taxi_count"] for c in grid_cells),
        "grid_count":    len(grid_cells),
        "grid_cells":    grid_cells,
        "updated_at":    str(latest_window) if latest_window else None
    }


# ============================================================
# ENDPOINTS BỔ SUNG CHO GRAFANA (JSON API)
# ============================================================

@app.get("/bus/speed_bus")
async def get_speed_bus():
    """Lấy dữ liệu speed_bus cho Grafana Panel 1"""
    docs = list(db.speed_bus.find({}, {"_id": 0}).sort("window_start", -1).limit(1000))
    return docs

@app.get("/batch/hourly_pivot")
async def get_batch_hourly_pivot():
    """Lấy dữ liệu batch_hourly_pivot cho Grafana Panel 5"""
    docs = list(db.batch_hourly_pivot.find({}, {"_id": 0}))
    return docs

@app.get("/taxi/speed_taxi")
async def get_speed_taxi():
    """Lấy dữ liệu speed_taxi cho Grafana Panel 4"""
    docs = list(db.speed_taxi.find({}, {"_id": 0}).sort("window_start", -1).limit(1000))
    return docs


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
async def health():
    status = {"api": "ok", "mongodb": "unknown", "redis": "unknown"}
    try:
        db.command("ping")
        status["mongodb"] = "ok"
    except Exception:
        status["mongodb"] = "error"
    try:
        r.ping()
        status["redis"] = "ok"
    except Exception:
        status["redis"] = "error"
    return status