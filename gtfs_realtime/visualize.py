import duckdb
import pandas as pd
import plotly.express as px
import webbrowser


# Path to the Parquet file
parquet_file = "vehicle_positions_20250630_224819.parquet"
modified_parquet_file = "modified-vehicle_positions_20250630_224819.parquet"

# Function to load the parquet file and query the trajectory of Tram 1
def query_tram1_trajectory(parquet_file):
    """
    Queries the trajectory of Tram 1 from the parquet file using DuckDB.
    
    Parameters:
    - parquet_file: Path to the Parquet file
    
    Returns:
    - DataFrame containing the trajectory of Tram 1
    """
    # Connect to DuckDB in-memory database
    con = duckdb.connect()

    # Load the parquet file and query for Tram 1 (Assuming Tram 1 is identified by route_id or trip_id)
    # Adjust the condition to match the identifier for Tram 1
    query = f"""
    SELECT * 
    FROM parquet_scan('{parquet_file}')
    WHERE trip_id LIKE '1%'  -- Adjust based on actual trip_id or route_id for Tram 1
    ORDER BY timestamp
    """

    # Execute the query and return the result as a pandas DataFrame
    df = con.execute(query).fetchdf()

    # Close the connection
    con.close()
    
    return df

import folium

def visualize_trajectory(df):
    """
    Visualizes the trajectory with OpenStreetMap tiles.
    
    Parameters:
    - df: DataFrame containing the trajectories
    """
    if df.empty:
        print("No data available.")
        return
    
    # Initialize a Folium map centered on Riga
    m = folium.Map(location=[50.073658, 14.418540], 
                   tiles="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                   attr="OpenStreetMap", zoom_start=12)

    # Add trajectory points to the map
    for i, row in df.iterrows():
        if i % 1000 == 0:  # Print every 100th point to avoid flooding the console
            print(f"Adding point {i+1}/{len(df)}: {row['latitude']}, {row['longitude']}")
        # Assign a color based on trip_id (using a hash for reproducibility)
        color = f"#{abs(hash(row['trip_id'])) % 0xFFFFFF:06x}"
        readable_timestamp = pd.to_datetime(row['timestamp'], unit='s').strftime('%Y-%m-%d %H:%M:%S')
        folium.CircleMarker(
            location=[row['latitude'], row['longitude']],
            radius=4,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            tooltip=f"Vehicle: {row['vehicle_id']}<br>Trip: {row['trip_id']}<br>Timestamp: {readable_timestamp}"
        ).add_to(m)

    return m


def convert_parquet():
    df = pd.read_parquet(parquet_file)
    # map all rows to vehicle positions (the file has a column vehicle with attributes position, trip, currentStopSequence)
    vehicle_postions_map = df['vehicle'].apply(lambda x: {
        'vehicle_id': x['vehicle']['id'],
        'trip_id': x['trip']['tripId'],
        'route_id': x['trip']['routeId'],
        'latitude': x['position']['latitude'],
        'longitude': x['position']['longitude'],
        'current_stop_sequence': x['currentStopSequence'],
        'start_date': x['trip']['startDate'],
        'start_time': x['trip']['startTime'],
        'timestamp': x['timestamp'],
    })
    # convert from dict to DataFrame
    vehicle_positions_df = pd.DataFrame(vehicle_postions_map.tolist())

    # filter rows with 'current_stop_sequence' NaN
    vehicle_positions_df = vehicle_positions_df[vehicle_positions_df['current_stop_sequence'].isna()]

    # save to parquet
    vehicle_positions_df.to_parquet(f"modified-{parquet_file}", index=False)

# Main script
# if __name__ == "__main__":
# convert_parquet()

    # Load and query the trajectory of Tram 1
convert_parquet()
tram1_df = query_tram1_trajectory(modified_parquet_file)

if not tram1_df.empty:
    # Visualize the trajectory of Tram 1
    m= visualize_trajectory(tram1_df)
    # m.display()
    m.save("tram1_trajectory.html")
    # open file in browser
    webbrowser.open("tram1_trajectory.html")
    
else:
    print("No data found for Tram 1.")