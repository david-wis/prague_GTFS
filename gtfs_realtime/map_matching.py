import pandas as pd
import geopandas as gpd
import requests
from polyline import decode
from shapely.geometry import LineString
from collections import defaultdict
import tqdm
from shapely.geometry import Point

VALHALLA_URL = "http://localhost:8002/trace_route"

def prepare_trips(gdf):
    gdf = gdf.sort_values(['vehicle_id', 'trip_id', 'timestamp'])
    trip_points = defaultdict(list)
    for _, row in gdf.iterrows():
        key = (row['vehicle_id'], row['trip_id'])
        point = {
            "lat": row['latitude'],
            "lon": row['longitude']
        }
        if pd.notna(row['bearing']):
            point["heading"] = int(row['bearing'])
        trip_points[key].append(point)
    return trip_points

def map_match_trip(points, failed_log):
    if len(points) < 2:
        return None

    payload = {
        "shape": points,
        "costing": "auto",
        "shape_match": "map_snap",
        "filters": {
            "action": "include",
            "attributes": ["matched_point", "edge_shape"]
        }
    }

    try:
        response = requests.post(VALHALLA_URL, json=payload)
        response.raise_for_status()
        data = response.json()
        if "trip" in data:
            shape_encoded = data["trip"]["legs"][0]["shape"]
            return decode(shape_encoded, precision=6)
    except requests.exceptions.HTTPError as e:
        print(f"Error HTTP {e.response.status_code}: {e.response.text}")
        failed_log.append({
            "error": {e.response.text},
            "points": points
        })
    except Exception as e:
        print(f"Error: {e}")
        return None

def run_map_matching(gdf):
    trip_points = prepare_trips(gdf)
    failed_log = []

    rows = []
    for (veh_id, trip_id), points in tqdm.tqdm(trip_points.items(), desc="Map matching"):
        matched = map_match_trip(points, failed_log)
        if matched:
            geom = LineString([(lon, lat) for lat, lon in matched])
            rows.append({
                "vehicle_id": veh_id,
                "trip_id": trip_id,
                "geometry": geom
            })

    return gpd.GeoDataFrame(rows, crs="EPSG:4326"), failed_log

# df = pd.read_parquet("modified-vehicle_positions_20250630_224819.parquet")
if __name__ == "__main__":
    # gdf = gpd.read_parquet("modified-vehicle_positions_20250630_224819.parquet")
    df = pd.read_parquet("modified-vehicle_positions_20250630_224819.parquet")

    geometry = [Point(lon, lat) for lat, lon in zip(df['latitude'], df['longitude'])]
    print(geometry[0])

    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    
    matched_gdf, failed_log = run_map_matching(gdf)

    matched_gdf.to_file("map_matched_trips.geojson", driver="GeoJSON")
    print("Map matching completed and saved to map_matched_trips.geojson")

    if failed_log:
        failed_df = pd.DataFrame(failed_log)
        failed_df.to_csv("map_matching_errors.csv", index=False)
        print(f"Map matching failed for {len(failed_log)} trips. Errors saved to map_matching_errors.csv")
