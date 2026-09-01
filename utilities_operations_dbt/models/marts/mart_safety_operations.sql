-- mart_safety_operations.sql
-- Purpose: Safety analytics with severity and closure KPIs

{{ config(
    materialized='table',
    schema='MARTS'
) }}

with safety as (
    select * from {{ ref('stg_safety') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

final as (
    select
        -- === Keys ===
        s.obs_id,
        s.obs_date,
        s.city_name,
        s.crew_id,
        s.crew_type,

        -- === Observation Details ===
        s.observation_type,
        s.observation_category,
        s.severity,
        s.severity_score,
        s.root_cause,
        s.days_to_close,
        s.closure_speed,
        s.weather_risk_score,
        s.season,

        -- === Weather Context ===
        w.temp_max_f,
        w.wind_speed_max_mph,
        w.weather_description,

        -- === Calculated KPIs ===
        -- Safety risk index (severity × weather risk)
        round(s.severity_score * (s.weather_risk_score + 1), 1)
            as safety_risk_index,

        -- Overdue flag (taking too long to close)
        case
            when s.severity = 'Critical' and s.days_to_close > 14 then 1
            when s.severity = 'High'     and s.days_to_close > 30 then 1
            else 0
        end as is_overdue,

        -- Incident flag (only real incidents, not observations)
        case
            when s.observation_type in ('Incident', 'Near Miss') then 1
            else 0
        end as is_incident,

        -- Date parts
        year(s.obs_date)  as year,
        month(s.obs_date) as month_num

    from safety s
    left join weather w
        on s.obs_date  = w.weather_date
        and s.city_name = w.city_name
)

select * from final