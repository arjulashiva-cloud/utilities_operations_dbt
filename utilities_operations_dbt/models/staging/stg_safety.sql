-- stg_safety.sql
-- Purpose: Clean and standardize raw safety observation data

{{ config(materialized='view') }}

SELECT
    obs_id,
    obs_date,
    city                                        AS city_name,
    crew_id,
    crew_type,
    observation_type,
    severity,
    root_cause,
    days_to_close,
    weather_risk_score,
    season,

    -- Negative vs positive observation
    CASE
        WHEN observation_type = 'Positive Observation' THEN 'Positive'
        ELSE 'Negative'
    END AS observation_category,

    -- Severity score for aggregations (Low=1 → Critical=4)
    CASE severity
        WHEN 'Low'      THEN 1
        WHEN 'Medium'   THEN 2
        WHEN 'High'     THEN 3
        WHEN 'Critical' THEN 4
        ELSE 0
    END AS severity_score,

    -- Closing speed flag
    CASE
        WHEN days_to_close <= 5  THEN 'Fast'
        WHEN days_to_close <= 14 THEN 'Normal'
        WHEN days_to_close <= 30 THEN 'Slow'
        ELSE 'Overdue'
    END AS closure_speed

FROM UTILITIES_RAW.PUBLIC.RAW_SAFETY