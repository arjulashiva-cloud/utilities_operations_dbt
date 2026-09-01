-- stg_workforce.sql
-- Purpose: Clean and standardize raw workforce data

{{ config(materialized='view') }}

SELECT
    record_id,
    work_date,
    city                                        AS city_name,
    crew_id,
    crew_type,
    job_type,
    hours_worked,
    is_overtime,
    hourly_rate,
    labor_cost,
    risk_score                                  AS weather_risk_score,

    -- Flag overtime clearly
    CASE
        WHEN is_overtime = 1 THEN 'Overtime'
        ELSE 'Regular'
    END AS shift_type,

    -- Cost tier per crew
    CASE
        WHEN labor_cost < 400   THEN 'Low Cost'
        WHEN labor_cost < 700   THEN 'Medium Cost'
        WHEN labor_cost < 1000  THEN 'High Cost'
        ELSE 'Premium Cost'
    END AS cost_tier,

    -- Emergency flag
    CASE
        WHEN job_type ILIKE '%emergency%'
          OR job_type ILIKE '%storm%'
          OR job_type ILIKE '%restoration%'
        THEN 1 ELSE 0
    END AS is_emergency_deployment

FROM UTILITIES_RAW.PUBLIC.RAW_WORKFORCE