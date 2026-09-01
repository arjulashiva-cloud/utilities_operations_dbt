{{ config(materialized='table', schema='MARTS') }}

with demand as (
    select * from {{ ref('stg_eia_demand') }}
),

-- Aggregate stg_weather to state level (city-level rows → one row per day)
-- Also derive CDD and HDD from temp data
weather as (
    select
        weather_date,
        round(avg(temp_avg_f), 1)                               as temp_avg_f,
        round(avg(temp_max_f), 1)                               as temp_max_f,
        round(avg(temp_min_f), 1)                               as temp_min_f,
        round(avg(wind_speed_max_mph), 1)                       as wind_speed_max_mph,
        round(avg(precipitation_inches), 2)                     as avg_precipitation_inches,

        -- Cooling Degree Days: how far above 65°F the avg temp is
        -- (standard utility measure of A/C demand)
        round(greatest(avg(temp_avg_f) - 65, 0), 1)            as cooling_degree_days,

        -- Heating Degree Days: how far below 65°F the avg temp is
        -- (standard utility measure of heating demand)
        round(greatest(65 - avg(temp_avg_f), 0), 1)            as heating_degree_days,

        -- Most common weather description across cities
        mode(weather_description)                               as dominant_weather

    from {{ ref('stg_weather') }}
    group by weather_date
),

joined as (
    select
        -- Demand fields
        d.demand_date,
        d.respondent,
        d.respondent_name,
        d.total_demand_mwh,
        d.peak_demand_mwh,
        d.avg_demand_mwh,
        d.min_demand_mwh,
        d.daily_demand_swing_mwh,
        d.load_tier,
        d.is_critical_peak,
        d.is_high_load,
        d.day_of_week,
        d.is_weekend,
        d.month,
        d.year,
        d.season,

        -- Weather fields
        w.temp_avg_f,
        w.temp_max_f,
        w.temp_min_f,
        w.wind_speed_max_mph,
        w.avg_precipitation_inches,
        w.cooling_degree_days,
        w.heating_degree_days,
        w.dominant_weather,

        -- Demand efficiency vs weather stress
        round(
            case
                when w.cooling_degree_days > 0
                then d.peak_demand_mwh / w.cooling_degree_days
                else null
            end, 1
        )                                                       as mwh_per_cooling_degree,

        round(
            case
                when w.heating_degree_days > 0
                then d.peak_demand_mwh / w.heating_degree_days
                else null
            end, 1
        )                                                       as mwh_per_heating_degree,

        -- Demand pressure score: 0-10 scale
        -- 10,642 MW = half of Colorado's 21,607 MW fleet (realistic peak)
        round(d.peak_demand_mwh / 10642 * 10, 2)               as demand_pressure_score,

        -- Operational condition combining weather + demand
        case
            when w.temp_avg_f >= 85 and d.peak_demand_mwh >= 5500
                then 'Heat Wave Stress'
            when w.temp_avg_f <= 20 and d.peak_demand_mwh >= 5500
                then 'Cold Snap Stress'
            when d.is_critical_peak = 1
                then 'Grid Critical'
            when d.is_high_load = 1
                then 'High Demand'
            else 'Normal Operations'
        end                                                     as operational_condition,

        current_timestamp()                                     as created_at

    from demand d
    left join weather w
        on d.demand_date = w.weather_date
)

select * from joined