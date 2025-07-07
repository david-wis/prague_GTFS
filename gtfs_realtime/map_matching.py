import pandas as pd
import geopandas as gpd
import requests
from polyline import decode
from shapely.geometry import LineString, Point
from collections import defaultdict
import tqdm

VALHALLA_URL = "http://localhost:8002/trace_route"

def prepare_trips(gdf):
    gdf = gdf.sort_values(['vehicle_id', 'trip_id', 'timestamp'])
    gdf['epoch_seconds'] = gdf['timestamp'].astype('int64') // 10**9
    trip_points = defaultdict(list)
    for _, row in gdf.iterrows():
        key = (row['vehicle_id'], row['trip_id'])
        point = {
            "lat": row['latitude'],
            "lon": row['longitude'],
            "epoch_seconds": row['epoch_seconds']
        }
        if pd.notna(row['bearing']):
            point["heading"] = int(row['bearing'])
        trip_points[key].append(point)
    return trip_points

def map_match_trip(points, failed_log, vehicle_id=None, trip_id=None):
    if len(points) < 2:
        return None

    payload = {
        "search_radius": 10,
        "shape": points,
        "costing": "auto",
        "shape_match": "map_snap",
        "use_timestamps": True,
        "format": "orsm",
        # "filters": {
        #     "action": "include",
        #     "attributes": ["matched_point", "edge_shape"]
        # }    
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
            "vehicle_id": vehicle_id,
            "trip_id": trip_id,
            "error_code": e.response.status_code,
            "error_msg": e.response.text,
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
        matched = map_match_trip(points, failed_log, vehicle_id=veh_id, trip_id=trip_id)
        if matched:
            geom = LineString([(lon, lat) for lat, lon in matched])
            rows.append({
                "vehicle_id": veh_id,
                "trip_id": trip_id,
                "geometry": geom
            })

    return gpd.GeoDataFrame(rows, crs="EPSG:4326"), failed_log

def projection(gdf, matched_gdf):
    gdf = gdf.dropna(subset=['vehicle_id', 'trip_id'])
    matched_gdf = matched_gdf.dropna(subset=['vehicle_id', 'trip_id'])

    merged = gdf.merge(
        matched_gdf[['vehicle_id', 'trip_id', 'geometry']], 
        on=['vehicle_id', 'trip_id'], 
        suffixes=('', '_line')
    )

    def linelocate(row):
        try:
            pos = row['geometry_line'].project(row['geometry'], normalized=True)
            return pos
        except Exception:
            return None

    def snap_point(row):
        try:
            if pd.isna(row['line_position']):
                return None, None
            snapped_point = row['geometry_line'].interpolate(row['line_position'], normalized=True)
            return snapped_point.y, snapped_point.x  # y=lat, x=lon
        except Exception:
            return None, None

    merged['line_position'] = merged.apply(linelocate, axis=1)
    merged[['snapped_latitude', 'snapped_longitude']] = merged.apply(
        lambda row: pd.Series(snap_point(row)), axis=1
    )

    cols = [
        "vehicle_id",
        "trip_id",
        "route_id",
        "latitude",
        "longitude",
        "snapped_latitude",
        "snapped_longitude",
        "bearing",
        "current_stop_sequence",
        "start_date", 
        "start_time", 
        "timestamp",
        "line_position"
    ]

    merged[cols].to_csv("vehicle_positions_with_linelocate.csv", index=False)


def save_failed_as_geojson(failed_log, filename="map_matching_errors.geojson"):
    rows = []
    for entry in failed_log:
        points = entry["points"]
        for p in points:
            geom = Point(p["lon"], p["lat"])
            rows.append({
                "vehicle_id": entry.get("vehicle_id"),
                "trip_id": entry.get("trip_id"),
                "error_code": entry.get("error_code"),
                "error_msg": entry.get("error_msg"),
                "geometry": geom
            })
    if rows:
        failed_gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
        failed_gdf.to_file(filename, driver="GeoJSON")
    else:
        print("No failed points to save.")

if __name__ == "__main__":
    df = pd.read_parquet("modified-vehicle_positions_20250630_224819.parquet")

    geometry = [Point(lon, lat) for lat, lon in zip(df['latitude'], df['longitude'])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    matched_gdf, failed_log = run_map_matching(gdf)

    # count distinct trips
    distinct_trips_len = matched_gdf['trip_id'].nunique()
    print(f"Number of distinct trips after map matching: {distinct_trips_len}")

    matched_gdf.to_file("map_matched_trips.geojson", driver="GeoJSON")
    print("Map matching completed.")

    if failed_log:
        failed_df = pd.DataFrame(failed_log)
        save_failed_as_geojson(failed_log)
        print(f"Failed map matching for {len(failed_log)} trips. Details saved to map_matching_errors.geojson")

    # projection(gdf, matched_gdf)
