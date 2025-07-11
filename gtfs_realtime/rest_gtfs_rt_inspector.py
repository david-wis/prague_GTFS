import requests
import pandas as pd
import time
from datetime import datetime
import signal
import sys

API_URL = "https://api.golemio.cz/v2/public/vehiclepositions"
INTERVAL = 20  # seconds between requests
running = True
data = []

# Graceful shutdown on Ctrl+C
def handler(sig, frame):
    global running
    print("\nInterrupted. Saving file...")
    running = False

signal.signal(signal.SIGINT, handler)

# Read arguments
if len(sys.argv) < 2:
    print("Usage: python golemio_scraper.py <API_KEY> [duration_in_seconds]")
    sys.exit(1)

API_KEY = sys.argv[1]

try:
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 300  # default: 5 minutes
except ValueError:
    print("Invalid duration. Use an integer (seconds).")
    sys.exit(1)

headers = {
    "Accept": "application/json",
    "x-access-token": API_KEY
}

print(f"Fetching data from Golemio for {duration} seconds... Press Ctrl+C to stop early.")

start_time = time.time()

while running and (time.time() - start_time < duration):
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        response.raise_for_status()
        payload = response.json()
        timestamp = datetime.utcnow().isoformat()

        for feature in payload.get("features", []):
            coords = feature["geometry"]["coordinates"]
            props = feature["properties"]

            data.append({
                "longitude": coords[0],
                "latitude": coords[1],
                "timestamp": timestamp,
                "route_id": props.get("gtfs_route_short_name"),
                "trip_id": props.get("gtfs_trip_id"),
                "vehicle_id": props.get("vehicle_id")
            })

        print(f"{datetime.now()}: {len(payload['features'])} vehicles recorded.")
    except Exception as e:
        print(f"Error: {e}")

    time.sleep(INTERVAL)

# Remove duplicates ignoring timestamp
df = pd.DataFrame(data)
deduped_df = df.drop_duplicates(subset=["longitude", "latitude", "route_id", "trip_id", "vehicle_id"])

# Save with end timestamp in filename
end_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
filename = f"new_vehicle_positions_{end_timestamp}.parquet"
deduped_df.to_parquet(filename, index=False)

print(f"Original records: {len(df)}")
print(f"Unique records (ignoring timestamp): {len(deduped_df)}")
print(f"File saved as: {filename}")
