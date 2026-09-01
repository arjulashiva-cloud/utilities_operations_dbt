-- stg_outages.sql
-- Purpose: Clean and standardize raw outage data

{{ config(materialized='view') }}

SELECT
    outage_id,
    outage_date,
    city                                        AS city_name,
    circuit                                     AS circuit_id,
    cause                                       AS outage_cause,
    duration_hours,
    customers_affected,
    weather_risk_score,

    -- Classify outage duration
    CASE
        WHEN duration_hours < 1   THEN 'Under 1 Hour'
        WHEN duration_hours < 4   THEN '1-4 Hours'
        WHEN duration_hours < 8   THEN '4-8 Hours'
        WHEN duration_hours < 16  THEN '8-16 Hours'
        ELSE 'Over 16 Hours'
    END AS duration_bucket,

    -- Flag planned vs unplanned
    CASE
        WHEN cause ILIKE '%planned%' THEN 'Planned'
        ELSE 'Unplanned'
    END AS outage_type,

    -- Customer impact tier
    CASE
        WHEN customers_affected < 500   THEN 'Low'
        WHEN customers_affected < 2000  THEN 'Medium'
        WHEN customers_affected < 5000  THEN 'High'
        ELSE 'Critical'
    END AS customer_impact_tier

FROM UTILITIES_RAW.PUBLIC.RAW_OUTAGES