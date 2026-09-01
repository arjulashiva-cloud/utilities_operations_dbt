-- mart_workforce_operations.sql
-- Purpose: Workforce analytics with cost and deployment KPIs

{{ config(
    materialized='table',
    schema='MARTS'
) }}

with workforce as (
    select * from {{ ref('stg_workforce') }}
),

weather as (
    select * from {{ ref('stg_weather') }}
),

final as (
    select
        -- === Keys ===
        w.record_id,
        w.work_date,
        w.city_name,
        w.crew_id,

        -- === Crew Details ===
        w.crew_type,
        w.job_type,
        w.shift_type,
        w.is_emergency_deployment,
        w.hours_worked,
        w.is_overtime,
        w.hourly_rate,
        w.labor_cost,
        w.cost_tier,
        w.weather_risk_score,

        -- === Weather Context ===
        wt.temp_max_f,
        wt.wind_speed_max_mph,
        wt.weather_description,

        -- === Calculated KPIs ===
        -- Overtime premium cost
        case
            when w.is_overtime = 1
            then round(w.labor_cost - (w.hours_worked * w.hourly_rate), 2)
            else 0
        end as overtime_premium_cost,

        -- Productivity bucket (hours per shift)
        case
            when w.hours_worked >= 12 then 'Extended Shift'
            when w.hours_worked >= 8  then 'Full Shift'
            else 'Partial Shift'
        end as shift_length_bucket,

        -- Date parts
        year(w.work_date)  as year,
        month(w.work_date) as month_num,
        week(w.work_date)  as week_num

    from workforce w
    left join weather wt
        on w.work_date  = wt.weather_date
        and w.city_name = wt.city_name
)

select * from final