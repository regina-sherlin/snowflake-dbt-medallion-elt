SELECT
    DATE(event_time_utc) AS activity_date,
    severity_category,
    COUNT(DISTINCT event_id) AS total_earthquakes,
    ROUND(AVG(magnitude), 2) AS avg_magnitude,
    MAX(magnitude) AS max_magnitude
FROM {{ ref('stg_earthquake_events') }}
GROUP BY 1, 2
ORDER BY activity_date DESC, total_earthquakes DESC
