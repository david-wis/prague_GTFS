import geopandas as gpd
import psycopg2
from psycopg2 import sql
import json
from collections import defaultdict

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'prague',
    'user': 'postgres',
    'port': '25432'
}

# GTFS route type mapping
GTFS_ROUTE_TYPES = {
    '0': 'Tram/Streetcar/Light Rail',
    '1': 'Subway/Metro',
    '2': 'Rail',
    '3': 'Bus',
    '4': 'Ferry',
    '5': 'Cable Tram',
    '6': 'Aerial Lift',
    '7': 'Funicular',
    '11': 'Trolleybus',
    '12': 'Monorail'
}

def get_route_types_from_db(trip_ids):
    """Fetch route types for a list of trip_ids from PostgreSQL"""
    route_types = {}
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Split into batches to avoid very large IN clauses
        batch_size = 1000
        for i in range(0, len(trip_ids), batch_size):
            batch = trip_ids[i:i + batch_size]
            
            query = sql.SQL("""
                SELECT t.trip_id, r.route_type
                FROM trips t
                JOIN routes r USING (route_id)
                WHERE t.trip_id IN ({})
            """).format(sql.SQL(',').join(map(sql.Literal, batch)))
            
            cursor.execute(query)
            route_types.update(dict(cursor.fetchall()))
        
        conn.close()
        return route_types
    
    except Exception as e:
        print(f"Database error: {e}")
        return {}

def analyze_vehicles(geojson_path):
    """Analyze vehicles with route types from database"""
    gdf = gpd.read_file(geojson_path)

    gdf = gdf.sort_values('trip_id').drop_duplicates('trip_id', keep='first')

    trip_ids = gdf['trip_id'].unique().tolist()

    print("Fetching route types from database...")
    route_types = get_route_types_from_db(trip_ids)
    
    gdf['route_type'] = gdf['trip_id'].map(route_types)
    gdf['route_type_name'] = gdf['route_type'].map(GTFS_ROUTE_TYPES)

    # if the route_type is NaN, check if the trip_id starts with 'Train' and assign 'Rail', idem for 'Metro'
    gdf.loc[gdf['route_type'].isna() & gdf['vehicle_id'].str.startswith('train'), 'route_type'] = '2'
    gdf.loc[gdf['route_type'].isna() & gdf['vehicle_id'].str.startswith('metro'), 'route_type'] = '1'

    # print(gdf['route_type_name'])
    
    type_counts = gdf['route_type_name'].value_counts().to_dict()
    
    unknown_count = gdf['route_type'].isna().sum()
    if unknown_count > 0:
        type_counts['Unknown'] = unknown_count
    
    results = {
        'counts': type_counts,
        'total_vehicles': len(gdf),
        'unique_trip_ids': len(trip_ids),
        'unknown_route_types': unknown_count,
        'sample_data': gdf.head(3)[['vehicle_id', 'trip_id', 'route_type_name', 'geometry']].to_dict('records')
    }
    
    return results, gdf

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python analyze_vehicles.py map_matching_errors.geojson")
        sys.exit(1)
    
    geojson_path = sys.argv[1]
    
    try:
        results, vehicle_gdf = analyze_vehicles(geojson_path)
        
        print("\nVehicle Counts by Route Type:")

        for route_type, count in results['counts'].items():
            print(f"{route_type}: {count}")
        
        print(f"\nTotal vehicles: {results['total_vehicles']}")

        # Buses
        # print("\nBuses:")
        # buses = vehicle_gdf[vehicle_gdf['route_type_name'] == 'Bus']
        # for _, row in buses.iterrows():
        #     print(f"Bus trip: {row[['trip_id', 'error_code', 'error_msg']].to_dict()}")


        print("\nUnknown route types:")
        unknown_trips = vehicle_gdf[vehicle_gdf['route_type'].isna()]
        for _, row in unknown_trips.iterrows():
            print(f"Unknown trip: {row[['trip_id', 'error_code', 'error_msg']].to_dict()}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)