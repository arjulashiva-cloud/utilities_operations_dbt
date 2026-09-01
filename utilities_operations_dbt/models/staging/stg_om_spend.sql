-- stg_om_spend.sql
-- Purpose: Clean and standardize raw O&M spend data

{{ config(materialized='view') }}

SELECT
    spend_id,
    spend_year,
    spend_month,
    city                                        AS city_name,
    asset_type,
    cost_category,
    amount_usd,
    avg_risk_score,

    -- Readable month name
    CASE spend_month
        WHEN 1  THEN 'January'
        WHEN 2  THEN 'February'
        WHEN 3  THEN 'March'
        WHEN 4  THEN 'April'
        WHEN 5  THEN 'May'
        WHEN 6  THEN 'June'
        WHEN 7  THEN 'July'
        WHEN 8  THEN 'August'
        WHEN 9  THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_name,

    -- Spend tier
    CASE
        WHEN amount_usd < 5000   THEN 'Low'
        WHEN amount_usd < 15000  THEN 'Medium'
        WHEN amount_usd < 30000  THEN 'High'
        ELSE 'Very High'
    END AS spend_tier,

    -- Infrastructure vs operational
    CASE
        WHEN asset_type IN ('Transformer', 'Substation', 'Underground Cable')
        THEN 'Infrastructure'
        ELSE 'Operational'
    END AS asset_category

FROM UTILITIES_RAW.PUBLIC.RAW_OM_SPEND