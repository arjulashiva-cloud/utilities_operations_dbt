-- mart_outages_operations.sql
-- Purpose: Outage analytics with weather context and KPIs

{{ config(
    materialized='table',
    schema='MARTS'
) }}

with outages as (
    select * from {{ ref('stg_outages') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

final as (
    select
        -- === Keys ===
        o.outage_id,
        o.outage_date,
        o.city_name,
        o.circuit_id,

        -- === Outage Details ===
        o.outage_cause,
        o.outage_type,
        o.duration_hours,
        o.duration_bucket,
        o.customers_affected,
        o.customer_impact_tier,
        o.weather_risk_score,

        -- === Weather Context (joined) ===
        w.temp_max_f,
        w.temp_min_f,
        w.temp_avg_f,
        w.precipitation_inches,
        w.wind_speed_max_mph,
        w.weather_description,

        -- === Calculated KPIs ===
        -- Customer Minutes Lost (standard utility metric)
        round(o.customers_affected * o.duration_hours * 60, 0)
            as customer_minutes_lost,

        -- Outage severity score (1-5)
        case
            when o.customers_affected < 500  and o.duration_hours < 2  then 1
            when o.customers_affected < 2000 and o.duration_hours < 4  then 2
            when o.customers_affected < 5000 and o.duration_hours < 8  then 3
            when o.customers_affected < 8000 and o.duration_hours < 16 then 4
            else 5
        end as outage_severity_score,

        -- Weather-driven flag
        case
            when o.outage_cause in (
                'Wind Damage', 'Tree Contact', 'Downed Line',
                'Ice on Lines', 'Frozen Equipment', 'Lightning Strike',
                'Flooding', 'Pipe Burst'
            ) then 1
            else 0
        end as is_weather_driven,

        -- Date parts for slicing
        year(o.outage_date)  as year,
        month(o.outage_date) as month_num,
        dayofweek(o.outage_date) as day_of_week

    from outages o
    left join weather w
        on o.outage_date = w.weather_date
        and o.city_name  = w.city_name
)

select * from final