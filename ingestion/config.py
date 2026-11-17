import os
from dotenv import load_dotenv

load_dotenv()

SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "ELT_WH")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "SEISMIC_ELT_DB")

API_HOST = "earthquake.usgs.gov"
API_PATH = "/fdsnws/event/1/query"
API_URL = f"https://{API_HOST}{API_PATH}"

PIPELINE_NAME = "usgs_earthquake_ingestion"
LOOKBACK_HOURS = 24
FULL_LOAD_DAYS = 7
