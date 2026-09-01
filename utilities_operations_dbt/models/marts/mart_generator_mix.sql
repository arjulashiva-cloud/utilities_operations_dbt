{{ config(materialized='table', schema='MARTS') }}

with generators as (
    select * from {{ ref('stg_eia_generators') }}
),

-- Fuel category rollup (the Power BI summary card)
fuel_summary as (
    select
        fuel_category,
        fuel_type,
        count(*)                                    as generator_count,
        round(sum(net_summer_capacity_mw), 0)       as total_net_capacity_mw,
        round(sum(nameplate_capacity_mw), 0)        as total_nameplate_capacity_mw,
        round(avg(capacity_efficiency_pct), 1)      as avg_efficiency_pct,
        round(avg(net_summer_capacity_mw), 1)       as avg_generator_size_mw,
        max(net_summer_capacity_mw)                 as largest_generator_mw,

        -- Share of total Colorado fleet
        round(
            sum(net_summer_capacity_mw)
            / sum(sum(net_summer_capacity_mw)) over ()
            * 100, 1
        )                                           as fleet_share_pct,

        -- Clean / dispatchable flags
        max(is_clean_energy)                        as is_clean_energy,
        max(is_dispatchable)                        as is_dispatchable,

        -- State (always CO here)
        state,
        period

    from generators
    group by fuel_category, fuel_type, state, period
),

final as (
    select
        fuel_category,
        fuel_type,
        generator_count,
        total_net_capacity_mw,
        total_nameplate_capacity_mw,
        avg_efficiency_pct,
        avg_generator_size_mw,
        largest_generator_mw,
        fleet_share_pct,
        is_clean_energy,
        is_dispatchable,

        -- Cumulative capacity share (for waterfall charts in Power BI)
        round(
            sum(fleet_share_pct) over (
                order by total_net_capacity_mw desc
                rows between unbounded preceding and current row
            ), 1
        )                                           as cumulative_share_pct,

        -- Risk label — what happens if this fuel type goes offline?
        case
            when fleet_share_pct >= 25 then 'Critical Dependency'
            when fleet_share_pct >= 10 then 'Significant Dependency'
            when fleet_share_pct >= 5  then 'Moderate Dependency'
            else                            'Low Dependency'
        end                                         as grid_dependency_label,

        state,
        period,
        current_timestamp()                         as created_at

    from fuel_summary
)

select * from final