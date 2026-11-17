import logging
import argparse
from datetime import datetime, timezone, timedelta
from ingestion.snowflake_client import SnowflakeClient
from ingestion import config
import requests
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    force=True
)

logger=logging.getLogger(__name__)

def run_pipeline(mode):
    sf=SnowflakeClient()
    try:
        # 1. CHECK DATABASE SCHEMA, TABLE, WH
        sf.setup_tables('sql/setup.sql')


        # 2. GET EXTRACTION WINDOW
        end_time = datetime.utcnow()
        last_run = sf.get_watermark(config.PIPELINE_NAME)

        if mode == 'full' or last_run is None:
            start_time = end_time - timedelta(days=config.FULL_LOAD_DAYS)
            logger.info('Running full load')
        else:
            start_time = last_run - timedelta(hours = config.LOOKBACK_HOURS)
            logger.info(f'Running incremental with {config.LOOKBACK_HOURS} lookback window from {last_run}')


        # 3. EXTRACT FROM API FOR THE WINDOW
        events = fetch_data(start_time, end_time)
        if not events:
            logger.info('No events found')
            return
        

        # 4. LOAD INTO WH 
        batch_id = end_time.strftime('%Y%m%d_%H%M%S')
        sf.load_into_bronze(batch_id, events)

        # 5. LOG WATERMARK
        latest_event_time = get_latest_event_time(events)
        if last_run is None or latest_event_time > last_run:
            sf.update_watermark(config.PIPELINE_NAME, latest_event_time)
        else:
            logger.info("No newer events than existing watermark.")

    finally:
        sf.close()

def fetch_data(start_time, end_time):
    params = {
        "format": "geojson",
        "starttime": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "minmagnitude": 1.0,
    }
    logger.info(f"Fetching events from {params['starttime']} to {params['endtime']}")
    response = requests.get(config.API_URL, params=params, timeout=30)
    response.raise_for_status()

    events = response.json().get("features", [])
    logger.info(f"Fetched {len(events)} events from API.")
    return events


def get_latest_event_time(events):
    latest_ms = max(event["properties"]["time"] for event in events)
    return datetime.utcfromtimestamp(latest_ms / 1000.0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Parser to log pipeline"
    )

    parser.add_argument('--mode', choices=['incremental', 'full'], default="incremental", help='mode to decide extraction type')

    args = parser.parse_args()
    run_pipeline(args.mode)