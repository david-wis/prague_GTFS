from unittest import result
import pandas as pd
import geopandas as gpd
import requests
from polyline import decode
from shapely.geometry import LineString, Point
from collections import defaultdict
import tqdm
import json

VALHALLA_URL = "http://localhost:8002/trace_route"
# VALHALLA_URL = "http://localhost:8002/trace_attributes"
# VALHALLA_URL = "https://valhalla1.openstreetmap.de/trace_route"

def prepare_trips(gdf):
    gdf = gdf.sort_values(['vehicle_id', 'trip_id', 'timestamp'])
    gdf['epoch_seconds'] = gdf['timestamp'].astype('int64') // 10**9
    trip_points = defaultdict(list)
    for _, row in gdf.iterrows():
        key = (row['vehicle_id'], row['trip_id'], row['route_id'])
        prev = trip_points[key][-1] if trip_points[key] else None
        if row['timestamp'] == prev['time'] if prev else None:
            if (row['latitude'] == prev['lat'] and row['longitude'] == prev['lon']) if prev else False:
                continue  # Skip duplicate points
            else:
                print("Inconsistency detected")

        point = {
            "lat": row['latitude'],
            "lon": row['longitude'],
            "time": row['timestamp']
            # "epoch_seconds": row['epoch_seconds']
            # "time": row['epoch_seconds']
        }
        trip_points[key].append(point)
    return trip_points

def append_error(failed_log, vehicle_id, trip_id, route_id, error_code, error_msg, points):
    failed_log.append({
        "vehicle_id": vehicle_id,
        "trip_id": trip_id,
        "route_id": route_id,
        "error_code": error_code,
        "error_msg": error_msg,
        "points": points
    })

def map_match_trip(points, failed_log, vehicle_id=None, trip_id=None, route_id=None):
    if len(points) < 2:
        append_error(failed_log, vehicle_id, trip_id, route_id, "InsufficientPoints", "Less than 2 points for map matching", points)
        return None, None

    payload = {
        "shape": points,
        "costing": "auto",
        "shape_match": "map_snap",
        "use_timestamps": True,
        "format": "osrm",
        "trace_options": {
            "search_radius": 100,
            "turn_penalty_factor": 500
        }
        # "filters": {
        #     "action": "include",
        #     "attributes": ["matched_point", "edge_shape"]
        # }    
    }

    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(VALHALLA_URL, headers=headers, json=payload)

        response.raise_for_status()
        data = response.json()
        # print("Json: ", json.dumps(data, indent=2))
        shape_encoded = data["matchings"][0].get("geometry")

        # print(f"Trip id: {trip_id}, Vehicle id: {vehicle_id}, Trace points length: {len(points)}")
        trace_points = data['tracepoints']
        result_points = []
        for i, tp in enumerate(trace_points):
            if tp is not None:
                result_points.append((tp['location'][0], tp['location'][1], points[i]['time']))
            else:
                print(f"Warning: Tracepoint {i} is None for trip {trip_id} and vehicle {vehicle_id}")
                append_error(failed_log, vehicle_id, trip_id, route_id, "TracepointNone", f"Tracepoint {i} is None", points)
                return None, None

        return result_points, decode(shape_encoded, precision=6)
    except requests.exceptions.HTTPError as e:
        print(f"Error HTTP {e.response.status_code}: {e.response.text}")
        append_error(failed_log, vehicle_id, trip_id, route_id, "HTTPError", str(e), points)
    except Exception as e:
        print(f"Error: {e}")
    return None, None

def run_map_matching(gdf):
    trip_points = prepare_trips(gdf)
    failed_log = []

    traj_rows = []
    point_rows = []
    shapes = []
    for (veh_id, trip_id, route_id), points in tqdm.tqdm(trip_points.items(), desc="Map matching"):
        matched, shape = map_match_trip(points, failed_log, vehicle_id=veh_id, trip_id=trip_id)
        if matched:
            geom = LineString(matched)
            traj_rows.append({
                "vehicle_id": veh_id,
                "trip_id": trip_id,
                "route_id": route_id,
                "geometry": geom
            })
            for lon, lat, time in matched:
                point_rows.append({
                    "vehicle_id": veh_id,
                    "trip_id": trip_id,
                    "route_id": route_id,
                    "latitude": lat,
                    "longitude": lon,
                    "timestamp": time
                })
            shapes.append({
                "vehicle_id": veh_id,
                "trip_id": trip_id,
                "route_id": route_id,
                "geometry": LineString([(lon, lat) for lat, lon in shape or []])
            })
                

    traj_df = gpd.GeoDataFrame(traj_rows, crs="EPSG:4326")
    point_df = pd.DataFrame(point_rows)
    shapes_df = gpd.GeoDataFrame(shapes, crs="EPSG:4326", geometry='geometry')
    return traj_df, failed_log, point_df, shapes_df


def save_failed_as_geojson(failed_log, filename="map_matching_errors.geojson"):
    rows = []
    for entry in failed_log:
        points = entry["points"]
        for p in points:
            geom = Point(p["lon"], p["lat"])
            rows.append({
                "vehicle_id": entry.get("vehicle_id"),
                "trip_id": entry.get("trip_id"),
                "route_id": entry.get("route_id"),
                "error_code": entry.get("error_code"),
                "error_msg": entry.get("error_msg"),
                "geometry": geom
            })
    if rows:
        failed_gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
        failed_gdf.to_file(filename, driver="GeoJSON")
    else:
        print("No failed points to save.")

parquet_file = "modified-vehicle_positions_20250708_175459.parquet"

if __name__ == "__main__":
    df = pd.read_parquet(parquet_file)

    geometry = [Point(lon, lat) for lat, lon in zip(df['latitude'], df['longitude'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    matched_gdf, failed_log, point_df, shapes_df = run_map_matching(gdf)

    # count distinct trips
    distinct_trips_len = matched_gdf['trip_id'].nunique()
    print(f"Number of distinct trips after map matching: {distinct_trips_len}")

    matched_gdf.to_file("map_matched_trips.geojson", driver="GeoJSON")
    print("Map matching completed.")

    if failed_log:
        failed_df = pd.DataFrame(failed_log)
        save_failed_as_geojson(failed_log)
        print(f"Failed map matching for {len(failed_log)} trips. Details saved to map_matching_errors.geojson")

    point_df.to_csv("map_matched_positions.csv", index=False)
    shapes_df.to_file("map_matched_shapes.geojson", driver="GeoJSON")
