import sys
import os
import time
import pandas as pd
import urllib.request as urllib
from google.protobuf.json_format import MessageToDict
from definitions import gtfs_realtime_pb2
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def fetch_gtfs_feed(server_url_prefix, feed_name):
    url = f'https://{server_url_prefix}/v2/vehiclepositions/gtfsrt/{feed_name}.pb'
    try:
        response = urllib.urlopen(url)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.read())
        return feed
    except Exception as e:
        logging.error(f"Error fetching feed: {e}")
        return None


def extract_vehicle_positions(feed):
    return [MessageToDict(entity) for entity in feed.entity]


def collect_vehicle_positions(server_url_prefix, feed_name, duration_minutes, interval_seconds):
    collected_data = []
    start_time = time.time()
    end_time = start_time + duration_minutes * 60
    next_save_time = start_time + 300  # 5 minutes

    while time.time() < end_time:
        feed = fetch_gtfs_feed(server_url_prefix, feed_name)
        if feed:
            positions = extract_vehicle_positions(feed)
            timestamp = datetime.utcnow().isoformat()
            for pos in positions:
                pos["fetch_time"] = timestamp
            collected_data.extend(positions)
        else:
            logging.warning("No data fetched.")

        # Save every 5 minutes
        if time.time() >= next_save_time:
            if collected_data:
                partial_df = pd.DataFrame(collected_data)
                ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
                if not os.path.exists('output'):
                    os.makedirs('output')
                fname = f"output/{feed_name}_partial_{ts}.parquet"
                save_to_parquet(partial_df, fname)
                for file in os.listdir('output'):
                    if file.startswith(f"{feed_name}_partial_") and file != os.path.basename(fname):
                        os.remove(os.path.join('output', file))
                logging.info(f"Partial data saved to {fname}")
            else:
                logging.info("No data collected yet to save.")
            next_save_time += 300  # next 5-minute mark

        time.sleep(interval_seconds)

    return pd.DataFrame(collected_data)


def save_to_parquet(df, file_name):
    if not df.empty:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_name)
    else:
        logging.warning("No data to save.")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        logging.error("Usage: python gtfs_rt_inspector.py <server_url_prefix> <feed_name> <duration_minutes>")
        sys.exit(1)

    server_url_prefix = sys.argv[1]
    feed_name = sys.argv[2]
    duration_minutes = int(sys.argv[3])
    interval_seconds = 20  # Still hardcoded, but configurable if desired

    logging.info(f"Starting data collection from feed '{feed_name}' for {duration_minutes} minutes...")
    logging.info(f"Current time: {datetime.utcnow().isoformat()}")
    logging.info(f"Data will be collected every {interval_seconds} seconds.")
    logging.info(f"Expected end time: {datetime.utcnow() + timedelta(minutes=duration_minutes)}")

    df = collect_vehicle_positions(server_url_prefix, feed_name, duration_minutes, interval_seconds)

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    output_file = f"{feed_name}_{timestamp}.parquet"
    save_to_parquet(df, output_file)
