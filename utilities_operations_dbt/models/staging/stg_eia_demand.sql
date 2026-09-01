{{ config(materialized='view') }}

with source as (
    select * from UTILITIES_RAW.PUBLIC.RAW_EIA_DEMAND
),

final as (
    select
        date                                        as demand_date,
        respondent,
        respondent_name,
        total_demand_mwh,
        peak_demand_mwh,
        avg_demand_mwh,
        min_demand_mwh,
        hours_reported,
        load_tier,
        day_of_week,
        is_weekend,
        month,
        year,

        -- Derived metrics
        round(peak_demand_mwh - min_demand_mwh, 1) as daily_demand_swing_mwh,

        case
            when load_tier = 'Critical Peak' then 1 else 0
        end                                         as is_critical_peak,

        case
            when load_tier in ('Critical Peak', 'High Load') then 1 else 0
        end                                         as is_high_load,

        case
            when month in (12, 1, 2) then 'Winter'
            when month in (3, 4, 5)  then 'Spring'
            when month in (6, 7, 8)  then 'Summer'
            else                          'Fall'
        end                                         as season,

        created_at

    from source
)

select * from final