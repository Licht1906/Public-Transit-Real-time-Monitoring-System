"""
FastAPI serving layer for the Singapore transit monitoring system.

Flows:
1. Static bus stops from MongoDB, cached in Redis for one hour.
2. Bus arrivals from LTA DataMall on demand, cached in Redis for 20 seconds.
3. MRT, carpark, EV, and taxi speed views from MongoDB.
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import redis
from bson import ObjectId
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo import MongoClient


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://root:Transit%402024@mongodb.data.svc.cluster.local:27017")
MONGO_DB = os.getenv("MONGODB_DATABASE", "transit_db")
REDIS_HOST = os.getenv("REDIS_HOST", "redis-master.data.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASS = os.getenv("REDIS_PASSWORD", "Redis@2024")
LTA_KEY = os.getenv("LTA_API_KEY", "")
LTA_BASE = os.getenv("LTA_BASE_URL", "https://datamall2.mytransport.sg/ltaodataservice")

mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2500)
db = mongo[MONGO_DB]
cache = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASS or None,
    decode_responses=True,
    socket_timeout=2,
)

app = FastAPI(title="SG Transit API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def jsonable(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: jsonable(item) for key, item in value.items()}
    return value


def cache_get(key: str) -> Any | None:
    try:
        raw = cache.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def cache_set(key: str, ttl_seconds: int, value: Any) -> None:
    try:
        cache.setex(key, ttl_seconds, json.dumps(jsonable(value)))
    except Exception:
        pass


def latest_by_key(docs: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    latest: dict[Any, dict[str, Any]] = {}
    for doc in docs:
        value = doc.get(key)
        if value and value not in latest:
            latest[value] = doc
    return list(latest.values())


@app.get("/")
async def root() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health() -> dict[str, str]:
    status = {"api": "ok", "mongodb": "unknown", "redis": "unknown"}
    try:
        db.command("ping")
        status["mongodb"] = "ok"
    except Exception:
        status["mongodb"] = "error"
    try:
        cache.ping()
        status["redis"] = "ok"
    except Exception:
        status["redis"] = "error"
    return status


@app.get("/bus/stops")
async def get_all_stops() -> dict[str, Any]:
    cached = cache_get("bus:stops:all")
    if cached:
        return {"source": "cache", **cached}

    projection = {
        "_id": 0,
        "BusStopCode": 1,
        "RoadName": 1,
        "Description": 1,
        "Latitude": 1,
        "Longitude": 1,
    }
    stops = list(db.bus_stops_static.find({}, projection).limit(5000))
    result = {"count": len(stops), "stops": jsonable(stops)}
    cache_set("bus:stops:all", 3600, result)
    return {"source": "mongodb", **result}


@app.get("/bus/arrivals/{bus_stop_code}")
async def get_bus_arrivals(bus_stop_code: str) -> dict[str, Any]:
    cache_key = f"bus:arrivals:{bus_stop_code}"
    cached = cache_get(cache_key)
    if cached is not None:
        return {
            "source": "cache",
            "bus_stop_code": bus_stop_code,
            "services_count": len(cached),
            "data": cached,
        }

    if not LTA_KEY or LTA_KEY == "your_key":
        raise HTTPException(status_code=500, detail="LTA_API_KEY is not configured.")

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            response = await client.get(
                f"{LTA_BASE}/v3/BusArrival",
                headers={"AccountKey": LTA_KEY, "accept": "application/json"},
                params={"BusStopCode": bus_stop_code},
            )
            response.raise_for_status()
            lta_data = response.json()
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="LTA API timeout") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"LTA API returned {exc.response.status_code}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Cannot reach LTA API: {exc}") from exc

    formatted = []
    for service in lta_data.get("Services", []):
        buses = []
        for bus_key in ("NextBus", "NextBus2", "NextBus3"):
            bus = service.get(bus_key) or {}
            eta_str = bus.get("EstimatedArrival")
            if not eta_str:
                continue
            try:
                eta_dt = datetime.fromisoformat(eta_str)
                if eta_dt.tzinfo is None:
                    eta_dt = eta_dt.replace(tzinfo=timezone.utc)
                eta_minutes = max(0, round((eta_dt - datetime.now(timezone.utc)).total_seconds() / 60, 1))
            except Exception:
                eta_minutes = None

            load_code = bus.get("Load", "")
            buses.append(
                {
                    "eta_minutes": eta_minutes,
                    "estimated_arrival": eta_str,
                    "load": {"SEA": "LOW", "SDA": "MEDIUM", "LSD": "HIGH"}.get(load_code, "UNKNOWN"),
                    "load_code": load_code,
                    "lat": float(bus.get("Latitude") or 0),
                    "lng": float(bus.get("Longitude") or 0),
                    "monitored": bus.get("Monitored", 0),
                    "type": bus.get("Type", ""),
                    "feature": bus.get("Feature", ""),
                }
            )
        if buses:
            formatted.append({"service_no": service.get("ServiceNo", ""), "operator": service.get("Operator", ""), "buses": buses})

    if not formatted:
        raise HTTPException(status_code=404, detail=f"No buses found for stop {bus_stop_code}")

    cache_set(cache_key, 20, formatted)
    return {"source": "lta_api", "bus_stop_code": bus_stop_code, "services_count": len(formatted), "data": formatted}


@app.get("/bus/stop/{bus_stop_code}/info")
async def get_stop_info(bus_stop_code: str) -> dict[str, Any]:
    stop = db.bus_stops_static.find_one({"BusStopCode": bus_stop_code}, {"_id": 0})
    if not stop:
        raise HTTPException(status_code=404, detail=f"Stop {bus_stop_code} not found")
    routes = list(
        db.bus_routes_static.find(
            {"BusStopCode": bus_stop_code},
            {"_id": 0, "ServiceNo": 1, "Direction": 1, "StopSequence": 1},
        ).sort("ServiceNo", 1)
    )
    return {"stop": jsonable(stop), "routes": jsonable(routes), "services": sorted({r["ServiceNo"] for r in routes if r.get("ServiceNo")})}


@app.get("/mrt/crowd/{train_line}")
async def get_mrt_crowd(train_line: str) -> dict[str, Any]:
    valid_lines = {"NSL", "EWL", "NEL", "CCL", "DTL", "TEL", "BPL"}
    line = train_line.upper()
    if line not in valid_lines:
        raise HTTPException(status_code=400, detail=f"Invalid line. Use: {sorted(valid_lines)}")
    docs = list(db.speed_mrt.find({"train_line": line}, {"_id": 0}).sort("ingested_at", -1).limit(200))
    stations = [
        {
            "station": doc.get("station"),
            "crowd_text": doc.get("crowd_text", "UNKNOWN"),
            "alert_level": doc.get("alert_level", "NORMAL"),
            "updated_at": jsonable(doc.get("ingested_at", "")),
        }
        for doc in latest_by_key(docs, "station")
    ]
    return {"train_line": line, "stations": stations, "high_count": sum(1 for s in stations if s["crowd_text"] == "HIGH")}


@app.get("/mrt/alerts")
async def get_train_alerts() -> dict[str, Any]:
    docs = list(db.speed_mrt.find({"event_type": "train_alert"}, {"_id": 0}).sort("ingested_at", -1).limit(20))
    return {"alerts": jsonable(docs), "count": len(docs)}


@app.get("/carpark")
async def get_carparks(
    agency: str | None = Query(None, description="HDB, LTA, URA"),
    min_lots: int = Query(0, ge=0),
    lot_type: str = Query("C", description="C=car, Y=motorcycle, H=heavy"),
) -> dict[str, Any]:
    query: dict[str, Any] = {"lot_type": lot_type.upper()}
    if agency:
        query["agency"] = agency.upper()
    if min_lots > 0:
        query["available_lots"] = {"$gte": min_lots}

    docs = list(db.speed_carpark.find(query, {"_id": 0}).sort("ingested_at", -1).limit(2000))
    carparks = []
    for doc in latest_by_key(docs, "carpark_id"):
        lat = doc.get("latitude") or doc.get("Latitude")
        lng = doc.get("longitude") or doc.get("Longitude")
        location = doc.get("location") or doc.get("Location") or ""
        if location and (lat is None or lng is None):
            try:
                lat, lng = [float(part) for part in location.split()[:2]]
            except Exception:
                pass
        carparks.append(
            {
                "carpark_id": doc.get("carpark_id"),
                "development": doc.get("development", ""),
                "area": doc.get("area", ""),
                "agency": doc.get("agency", ""),
                "available_lots": int(doc.get("available_lots") or 0),
                "status": doc.get("status", "UNKNOWN"),
                "lot_type": doc.get("lot_type", "C"),
                "latitude": lat,
                "longitude": lng,
                "updated_at": jsonable(doc.get("ingested_at", "")),
            }
        )
    carparks.sort(key=lambda item: item["available_lots"], reverse=True)
    return {"count": len(carparks), "carparks": jsonable(carparks), "full_count": sum(1 for cp in carparks if cp["available_lots"] == 0)}


@app.get("/ev/stations")
async def get_ev_stations(
    lat: float | None = Query(None),
    lng: float | None = Query(None),
    radius_km: float = Query(5.0, gt=0, le=50),
) -> dict[str, Any]:
    docs = list(db.speed_ev.find({}, {"_id": 0}).sort("ingested_at", -1).limit(3000))
    stations = latest_by_key(docs, "location_id")

    if lat is not None and lng is not None:
        def distance(station: dict[str, Any]) -> float:
            station_lat = float(station["latitude"])
            station_lng = float(station["longitude"])
            dlat = math.radians(station_lat - lat)
            dlng = math.radians(station_lng - lng)
            a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat)) * math.cos(math.radians(station_lat)) * math.sin(dlng / 2) ** 2
            return 6371 * 2 * math.asin(math.sqrt(a))

        nearby = []
        for station in stations:
            if station.get("latitude") is None or station.get("longitude") is None:
                continue
            try:
                dist = distance(station)
            except Exception:
                continue
            if dist <= radius_km:
                station["distance_km"] = round(dist, 2)
                nearby.append(station)
        stations = sorted(nearby, key=lambda item: item["distance_km"])

    return {"count": len(stations), "stations": jsonable(stations)}


@app.get("/taxi/positions")
async def get_taxi_positions() -> dict[str, Any]:
    docs = list(db.speed_taxi.find({}, {"_id": 0}).sort("window_start", -1).limit(1000))
    latest_window = docs[0].get("window_start") if docs else None
    if latest_window is not None:
        docs = [doc for doc in docs if doc.get("window_start") == latest_window]
    cells = [
        {"lat": doc.get("center_lat"), "lng": doc.get("center_lng"), "taxi_count": int(doc.get("taxi_count") or 0)}
        for doc in docs
        if doc.get("center_lat") is not None and doc.get("center_lng") is not None
    ]
    return {"total_nearby": sum(c["taxi_count"] for c in cells), "grid_count": len(cells), "grid_cells": cells, "updated_at": jsonable(latest_window)}


@app.get("/bus/speed_bus")
async def get_speed_bus() -> list[dict[str, Any]]:
    return jsonable(list(db.speed_bus.find({}, {"_id": 0}).sort("window_start", -1).limit(1000)))


@app.get("/batch/hourly_pivot")
async def get_batch_hourly_pivot() -> list[dict[str, Any]]:
    return jsonable(list(db.batch_hourly_pivot.find({}, {"_id": 0}).limit(1000)))


@app.get("/taxi/speed_taxi")
async def get_speed_taxi() -> list[dict[str, Any]]:
    return jsonable(list(db.speed_taxi.find({}, {"_id": 0}).sort("window_start", -1).limit(1000)))
