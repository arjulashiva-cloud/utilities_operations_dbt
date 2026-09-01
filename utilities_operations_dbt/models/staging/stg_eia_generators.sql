{{ config(materialized='view') }}

with source as (
    select * from UTILITIES_RAW.PUBLIC.RAW_EIA_GENERATORS
),

final as (
    select
        -- Keys
        period,
        state,
        plant_name,
        generator_id,

        -- Fuel classification
        energy_source_code,
        fuel_type,
        technology,

        -- Capacity
        net_summer_capacity_mw,
        nameplate_capacity_mw,

        -- Efficiency ratio — how much of nameplate is usable in summer peak
        round(
            case
                when nameplate_capacity_mw > 0
                then net_summer_capacity_mw / nameplate_capacity_mw * 100
                else null
            end, 1
        ) as capacity_efficiency_pct,

        -- GPS
        latitude,
        longitude,

        -- Clean energy flag
        case
            when fuel_type in ('Wind', 'Solar PV', 'Hydro', 'Battery Storage', 'Geothermal')
            then 1 else 0
        end as is_clean_energy,

        -- Dispatchable flag (can ramp up/down on demand)
        case
            when fuel_type in ('Natural Gas', 'Coal', 'Petroleum', 'Diesel', 'Battery Storage')
            then 1 else 0
        end as is_dispatchable,

        -- Size tier
        case
            when net_summer_capacity_mw >= 500 then 'Utility Scale (500+ MW)'
            when net_summer_capacity_mw >= 100 then 'Large (100-499 MW)'
            when net_summer_capacity_mw >= 10  then 'Medium (10-99 MW)'
            else                                    'Small (< 10 MW)'
        end as generator_size_tier,

        -- Fuel category for reporting
        case
            when fuel_type in ('Wind', 'Solar PV', 'Hydro', 'Geothermal')
            then 'Renewables'
            when fuel_type = 'Battery Storage'
            then 'Storage'
            when fuel_type in ('Natural Gas', 'Other Gas')
            then 'Gas'
            when fuel_type in ('Coal', 'Subbituminous Coal', 'Bituminous Coal')
            then 'Coal'
            when fuel_type in ('Petroleum', 'Diesel')
            then 'Oil'
            else 'Other'
        end as fuel_category,

        created_at

    from source
)

select * from final