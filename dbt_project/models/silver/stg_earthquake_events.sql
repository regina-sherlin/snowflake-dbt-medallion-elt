{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

WITH raw_events AS (
    SELECT
        SOURCE_EVENT_ID AS event_id,
        RAW_PAYLOAD,
        INGESTED_AT
    FROM {{ source('bronze_seismic', 'raw_earthquake_events') }}

    {% if is_incremental() %}
        WHERE INGESTED_AT > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    TRIM(event_id)::VARCHAR AS event_id,
    ROUND(RAW_PAYLOAD:properties:mag::FLOAT, 2) AS magnitude,
    TRIM(RAW_PAYLOAD:properties:place::VARCHAR) AS place,
    COALESCE(LOWER(TRIM(RAW_PAYLOAD:properties:alert::VARCHAR)), 'none') AS alert_level,
    TO_TIMESTAMP_NTZ(RAW_PAYLOAD:properties:time::NUMBER / 1000) AS event_time_utc,
    CASE
        WHEN RAW_PAYLOAD:properties:mag::FLOAT >= 5.0 THEN 'MAJOR'
        WHEN RAW_PAYLOAD:properties:mag::FLOAT >= 3.0 THEN 'MODERATE'
        ELSE 'MINOR'
    END AS severity_category,
    INGESTED_AT AS ingested_at
FROM raw_events
WHERE event_id IS NOT NULL
  AND RAW_PAYLOAD:properties:mag IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) = 1
