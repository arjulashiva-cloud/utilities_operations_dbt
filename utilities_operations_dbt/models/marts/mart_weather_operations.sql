{{ config(
    materialized='table',
    schema='MARTS'
) }}

with source as (

    select * from {{ ref('stg_weather') }}

),

weather_with_flags as (

    select
        -- === Dimensions ===
        weather_date,
        city_name,
        weather_description,

        -- === Raw Metrics ===
        temp_max_f,
        temp_min_f,
        temp_avg_f,
        precipitation_inches,
        wind_speed_max_mph,

        -- === Energy Demand Indicators ===
        case
            when temp_avg_f < 65 then round(65 - temp_avg_f, 1)
            else 0
        end as heating_degree_days,

        case
            when temp_avg_f > 65 then round(temp_avg_f - 65, 1)
            else 0
        end as cooling_degree_days,

        -- === Operational Risk Flags ===
        case when temp_min_f < 32  then 1 else 0 end as is_freeze_risk,
        case when temp_max_f > 95  then 1 else 0 end as is_heat_alert,
        case when wind_speed_max_mph > 45 then 1 else 0 end as is_high_wind,
        case when precipitation_inches > 0.5 then 1 else 0 end as is_heavy_precip,

        -- === Season & Date Parts ===
        case
            when month(weather_date) in (12, 1, 2) then 'Winter'
            when month(weather_date) in (3, 4, 5)  then 'Spring'
            when month(weather_date) in (6, 7, 8)  then 'Summer'
            when month(weather_date) in (9, 10, 11) then 'Fall'
        end as season,

        year(weather_date)  as year,
        month(weather_date) as month_num,
        week(weather_date)  as week_num

    from source

),

final as (

    select
        *,
        (is_freeze_risk + is_heat_alert + is_high_wind + is_heavy_precip)
            as ops_risk_score,

        case
            when (is_freeze_risk + is_heat_alert + is_high_wind + is_heavy_precip) = 0
                then 'Normal Operations'
            when (is_freeze_risk + is_heat_alert + is_high_wind + is_heavy_precip) = 1
                then 'Monitor'
            when (is_freeze_risk + is_heat_alert + is_high_wind + is_heavy_precip) = 2
                then 'Elevated Risk'
            else 'High Alert'
        end as ops_risk_label

    from weather_with_flags

)

select * from final