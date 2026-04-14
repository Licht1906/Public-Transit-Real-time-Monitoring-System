import requests
import json

# ==== CẤU HÌNH ====
API_KEY = "e825ByV2QYWgz/NY8/B2Bw=="  # Thay bằng key thật của bạn
BASE_URL = "https://datamall2.mytransport.sg/ltaodataservice"

headers = {
    "AccountKey": API_KEY,
    "accept": "application/json"
}

def fetch_mrt_crowd(train_line="NSL"):
    """
    Lấy mật độ hành khách real-time tại các ga MRT theo tuyến
    
    train_line options:
    NSL, EWL, NEL, CCL, CEL, CGL, DTL, TEL,
    BPL, SLRT, PLRT
    """
    url = f"{BASE_URL}/PCDRealTime"
    params = {"TrainLine": train_line}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e} — Status: {resp.status_code}")
    except requests.exceptions.Timeout:
        print("Request timed out")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
    return None


def fetch_train_alerts():
    """Lấy cảnh báo gián đoạn tàu điện"""
    url = f"{BASE_URL}/TrainServiceAlerts"

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()

    except Exception as e:
        print(f"Error: {e}")
    return None


# ==== CHẠY THỬ ====
if __name__ == "__main__":

    # Test 1: MRT crowd theo tuyến NSL
    print("=" * 50)
    print("TEST 1: Station Crowd Density — NSL")
    print("=" * 50)

    data = fetch_mrt_crowd("NSL")
    if data:
        stations = data.get("value", [])
        print(f"Số ga trả về: {len(stations)}")
        print(json.dumps(stations[:], indent=2))  # In 3 ga đầu

    # Test 2: Thử vài tuyến khác
    print("\n" + "=" * 50)
    print("TEST 2: So sánh tất cả tuyến")
    print("=" * 50)

    lines = ["NSL", "EWL", "NEL", "CCL", "DTL", "TEL"]
    for line in lines:
        result = fetch_mrt_crowd(line)
        count = len(result.get("value", [])) if result else 0
        print(f"  {line}: {count} ga")

    # Test 3: Train alerts
    print("\n" + "=" * 50)
    print("TEST 3: Train Service Alerts")
    print("=" * 50)

    alerts = fetch_train_alerts()
    if alerts:
        value = alerts.get("value", {})
        status = value.get("Status", "N/A")
        affected = value.get("AffectedSegments", [])
        print(f"Status: {status} ({'Normal' if status == 1 else 'DISRUPTED'})")
        print(f"Số tuyến bị ảnh hưởng: {len(affected)}")
        if affected:
            print(json.dumps(affected, indent=2))
        else:
            print("Không có gián đoạn hiện tại")