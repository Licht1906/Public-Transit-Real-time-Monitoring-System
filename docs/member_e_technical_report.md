# Member E Technical Report: Serving Layer, Web App, and Admin Dashboard

## 1. Introduction

This project builds a Singapore multi-modal transit monitoring system for bus, MRT, carpark, EV charging, and taxi availability. The goal is to provide a single operational view where end users can inspect live transport conditions while administrators can monitor aggregated system behavior.

The system follows Lambda Architecture. Streaming jobs produce low-latency speed views for operational screens, batch jobs produce historical analytics for reporting, and the serving layer exposes both kinds of data through FastAPI.

Singapore was selected because LTA DataMall provides stable public APIs, well-structured transport data, and predictable polling intervals. This makes the project suitable for a Big Data course because the architecture can focus on ingestion, streaming, batch processing, serving, caching, and visualization rather than manual data collection.

## 2. Architecture

The project contains three serving patterns:

1. Static data: bus stops and route metadata are loaded once into MongoDB and served through `/bus/stops`.
2. On-demand data: bus ETA is fetched directly from LTA DataMall when a user clicks a bus stop.
3. Speed views: MRT crowd, carpark, EV, and taxi data are read from MongoDB collections written by Spark Streaming.

This split is important because not all transport data has the same freshness and access pattern. Bus ETA changes frequently and users may click any of more than 5,000 stops. Polling every stop continuously would create a very high request volume. MRT crowd, carpark, EV, and taxi data can be processed in the background because the user experience tolerates a short delay.

The two main outputs are:

- Web App: an end-user interface with five tabs and interactive maps.
- Grafana: an admin dashboard with time series, heatmap, and aggregate panels.

## 3. Data Sources

The source system is LTA DataMall. The pipeline uses APIs for bus arrivals, bus stops, train crowd, carpark availability, EV charging, and taxi availability.

Raw data is ingested by Python services and published to Kafka topics. Spark Streaming reads Kafka, normalizes each transport mode, computes speed views, and writes the latest state into MongoDB. Spark Batch reads historical data from MinIO and writes analytical collections such as hourly bus ETA pivots.

MongoDB is used because the serving layer needs flexible JSON-like documents and simple query patterns. Redis is used for short-lived cache entries where low latency and TTL support matter.

## 4. On-Demand Bus ETA Pattern

Bus ETA is handled differently from other transport modes. The web app loads the static bus stop list once, displays the stops on a map, and calls `/bus/arrivals/{bus_stop_code}` only when the user requests one stop.

The request flow is:

1. The browser calls FastAPI with the selected bus stop code.
2. FastAPI checks Redis with key `bus:arrivals:{code}`.
3. If cached data exists, FastAPI returns it immediately.
4. If there is a cache miss, FastAPI calls LTA BusArrival API.
5. The parsed response is cached for 20 seconds and returned to the browser.

This design keeps full coverage of all bus stops without polling every stop. Redis also protects the LTA API when many users click the same stop during the same short time window.

| Criterion | Poll all 5,000 stops | On-demand |
|---|---:|---:|
| LTA requests when idle | Very high | 0 |
| Coverage | Full | Full |
| Freshness | High | High |
| Rate-limit risk | High | Low |
| Compute cost | High | Low |

## 5. FastAPI Serving Layer

The serving layer is implemented in `api/main.py`.

Important endpoints:

- `/health`: checks API, MongoDB, and Redis status.
- `/bus/stops`: returns up to 5,000 bus stops from `bus_stops_static`, cached for one hour.
- `/bus/arrivals/{bus_stop_code}`: returns live ETA from LTA DataMall with 20-second Redis cache.
- `/mrt/crowd/{train_line}`: returns latest MRT crowd level per station.
- `/mrt/alerts`: returns latest train alerts.
- `/carpark`: returns filtered carpark availability.
- `/ev/stations`: returns EV stations, optionally filtered by distance from user coordinates.
- `/taxi/positions`: returns latest taxi density grid.
- `/bus/speed_bus`, `/taxi/speed_taxi`, `/batch/hourly_pivot`: helper endpoints for Grafana.

The API uses `httpx.AsyncClient` for LTA calls so FastAPI does not block the event loop while waiting for network responses. MongoDB and Redis failures are handled gracefully where caching is optional, while missing LTA credentials return a clear error.

## 6. Web App

The web app is implemented in `api/static/index.html`. It is a single-page Leaflet application with five tabs:

- Bus: loads bus stops, supports search, draws markers, and displays ETA after a stop is selected.
- MRT: draws route and station markers from static MRT GeoJSON and overlays crowd levels from MongoDB.
- Carpark: shows availability with filters for agency and minimum lots.
- EV Charging: finds stations near a coordinate or browser geolocation.
- Taxi: displays density markers for the latest taxi grid window.

The frontend intentionally calls the FastAPI layer instead of calling LTA directly. This keeps API keys server-side, centralizes parsing logic, and allows Redis caching.

## 7. Streaming Layer Integration

Spark Streaming is responsible for transforming raw Kafka messages into query-ready MongoDB speed views:

- `speed_bus`: aggregate ETA windows for selected high-traffic stops.
- `speed_mrt`: station crowd levels and train alerts.
- `speed_carpark`: latest available lots by carpark.
- `speed_ev`: EV charging station availability.
- `speed_taxi`: taxi density by grid cell.

FastAPI reads these collections directly. This makes serving simple: each endpoint applies a small filter, deduplicates latest records by key where needed, and returns JSON.

## 8. Batch Layer Integration

The batch job reads historical data from MinIO, applies Spark window functions, enriches bus records with static metadata, and writes analytical views into MongoDB.

Key outputs:

- `batch_hourly_pivot`: average ETA by hour for Grafana heatmap.
- `batch_bus_daily`: daily reliability and congestion summaries.
- Optional ML output: GBT Regressor for ETA prediction.
- Optional graph output: hub station ranking when GraphFrames is available.

The project includes `spark/create_test_data.py` so the batch layer can be smoke-tested with a small Parquet dataset in MinIO before full ingestion history exists.

## 9. Storage and Caching

MongoDB stores static, speed, and batch views. It is appropriate for this project because records have mode-specific fields and the web app reads JSON directly.

Redis has two uses:

- Bus stop list cache: `bus:stops:all`, TTL 1 hour.
- Bus arrival cache: `bus:arrivals:{stop_code}`, TTL 20 seconds.

The 20-second arrival TTL matches the freshness characteristics of LTA bus arrival data. It reduces repeated calls without making the displayed ETA stale for long.

## 10. Kubernetes Deployment

The project targets local Minikube deployment. Core services are split across namespaces:

- `kafka`: Strimzi Kafka cluster and topics.
- `data`: MongoDB, Redis, and MinIO.
- `transit`: ingestion and FastAPI deployments.
- `spark-operator`: Spark Operator.
- `monitoring`: Grafana.

FastAPI is exposed through a NodePort service and can also be accessed with `kubectl port-forward svc/transit-api 8000:8000 -n transit`.

The API code and static frontend files are mounted through the `api-script` ConfigMap. Runtime secrets such as the LTA API key live in local `k8s/k8s-secrets.yaml`, created from the tracked template `k8s/k8s-secrets.example.yaml`.

## 11. Grafana Dashboard

The dashboard artifact is `grafana/transit_dashboard.json`. It contains five panels:

1. Bus ETA time series from `/bus/speed_bus`.
2. MRT crowd heatmap from `/mrt/crowd/NSL`.
3. Carpark availability bar chart from `/carpark`.
4. Taxi count time series from `/taxi/speed_taxi`.
5. Bus ETA by hour heatmap from `/batch/hourly_pivot`.

The dashboard refresh interval is 30 seconds. Grafana should use the FastAPI base URL as a JSON datasource.

## 12. Demo Checklist

Before presenting, verify:

- `/health` returns `api`, `mongodb`, and `redis` as `ok`.
- `/bus/stops` returns close to 5,000 records after static loading.
- `/bus/arrivals/83139` returns `source: lta_api` on first call.
- A second `/bus/arrivals/83139` call within 20 seconds returns `source: cache`.
- MRT, Carpark, EV, and Taxi endpoints return data from speed views.
- Web app opens at `http://localhost:8000` and all five tabs render.
- Grafana imports `grafana/transit_dashboard.json` and refreshes every 30 seconds.

## 13. Limitations and Next Steps

The current implementation is complete for local demo use. Remaining production improvements would include stronger authentication, persistent container images instead of installing Python packages at pod startup, automated integration tests against seeded MongoDB/Redis containers, and cloud deployment manifests if the target changes from Minikube to GKE.
