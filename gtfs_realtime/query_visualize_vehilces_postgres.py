import pandas as pd
import psycopg
import json
import numpy as np


def load_db_config(config_path="config.json"):
    with open(config_path, "r") as file:
        config = json.load(file)
    return config


def connect_to_postgres(config):
    try:
        conn = psycopg.connect(
            host=config["DB_HOST"],
            port=config["DB_PORT"],
            dbname=config["DB_NAME"],
            user=config["DB_USER"],
        )
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None, None


def create_vehicle_position_table(cur):
    cur.execute("DROP TABLE IF EXISTS vehiclePosition;")
    create_table_query = """
    CREATE TABLE vehiclePosition (
        vehicle_id TEXT,
        trip_id TEXT,
        route_id TEXT,
        latitude FLOAT8,
        longitude FLOAT8,
        current_stop_sequence INTEGER,
        start_date TEXT,
        start_time TEXT,
        timestamp BIGINT
    );
    """
    cur.execute(create_table_query)


parquet_file = "modified-vehicle_positions_20250630_224819.parquet"


def load_parquet_file(parquet_file):
    try:
        df = pd.read_parquet(parquet_file)
        return df
    except Exception as e:
        print(f"Error loading Parquet file: {e}")
        return pd.DataFrame()


def clean_current_stop_sequence(df):
    """
    Clean 'current_stop_sequence' to ensure all values are valid integers for PostgreSQL.
    NaN or invalid values become None (inserted as NULL in SQL).
    Any values out of INTEGER range are clipped.
    """
    # Convert to pandas nullable Int64
    col = pd.to_numeric(df["current_stop_sequence"], errors="coerce")
    # Clip values to PostgreSQL integer range
    int_min = -2147483648
    int_max = 2147483647
    col = col.clip(lower=int_min, upper=int_max)
    # Convert NaN to None (SQL NULL)
    df["current_stop_sequence"] = col.astype("Int64")
    return df


def insert_data_to_postgres(df, cur, conn):
    """
    Inserts data into the PostgreSQL 'vehiclePosition' table using only the correct columns.
    Handles missing or NULL current_stop_sequence as SQL NULL.
    """
    insert_query = """
    INSERT INTO vehiclePosition
    (vehicle_id, trip_id, route_id, latitude, longitude, current_stop_sequence, start_date, start_time, timestamp)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    # Use .where(pd.notnull()) to ensure None for SQL NULL
    rows = [
        (
            row["vehicle_id"],
            row["trip_id"],
            row["route_id"],
            row["latitude"],
            row["longitude"],
            (
                int(row["current_stop_sequence"])
                if pd.notnull(row["current_stop_sequence"])
                else None
            ),
            row["start_date"],
            row["start_time"],
            row["timestamp"],
        )
        for _, row in df.iterrows()
    ]
    cur.executemany(insert_query, rows)
    conn.commit()


if __name__ == "__main__":
    db_config = load_db_config("config.json")
    conn, cur = connect_to_postgres(db_config)
    if conn is None or cur is None:
        print("Failed to connect to the PostgreSQL database.")
        exit()
    print("Dropping and creating 'vehiclePosition' table...")
    create_vehicle_position_table(cur)
    print("Loading Parquet file...")
    vehicle_positions_df = load_parquet_file(parquet_file)
    if not vehicle_positions_df.empty:
        print(f"Loaded {len(vehicle_positions_df)} rows from the Parquet file.")
        print("Cleaning current_stop_sequence values...")
        vehicle_positions_df = clean_current_stop_sequence(vehicle_positions_df)
        print("Inserting data into PostgreSQL...")
        insert_data_to_postgres(vehicle_positions_df, cur, conn)
        print("Data insertion complete.")
    else:
        print("No data found in the Parquet file.")
    cur.close()
    conn.close()
    print("PostgreSQL connection closed.")
