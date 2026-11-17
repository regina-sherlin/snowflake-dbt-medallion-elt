# End-to-End Seismic ELT Pipeline (Python, Snowflake, dbt & GitHub Actions)

An incremental ELT data pipeline extracting live earthquake event data from the USGS GeoJSON API into Snowflake using a **Medallion Architecture (Bronze → Silver → Gold)**, transformed and tested with **dbt**, and automated via **GitHub Actions CI/CD**.

## Architecture & Key Concepts
* **Extract & Load (`ingestion/`):** Python script (`requests`, `snowflake-connector-python`) supporting `--mode full` and `--mode incremental`. Uses a `META.PIPELINE_WATERMARKS` state table with a **24-hour lookback window** to capture late-arriving and revised events.
* **Bronze Layer (`BRONZE.RAW_EARTHQUAKE_EVENTS`):** Stores immutable raw JSON payloads in Snowflake `VARIANT` columns alongside batch metadata.
* **Silver Layer (`SILVER.STG_EARTHQUAKE_EVENTS`):** dbt incremental model using `incremental_strategy='merge'` (**Upsert**) and `QUALIFY ROW_NUMBER()` deduplication to clean timestamps, standardize nulls, and guarantee pipeline idempotency.
* **Gold Layer (`GOLD.FCT_DAILY_SEISMIC_ACTIVITY`):** Aggregated daily seismic activity metrics by severity category.
* **CI/CD (`.github/workflows/`):** Pull requests from `env-dev` to `main` validate Python syntax and `dbt compile`; merges to `main` execute incremental ingestion and `dbt build`.

## How to Run Locally
1. Clone the repo and install dependencies: `pip install -r requirements.txt`
2. Copy `.env.example` to `.env` and add your Snowflake credentials.
3. Run ingestion: `python -m ingestion.extract_load --mode full`
4. Run transformations & tests: `cd dbt_project && dbt build`
