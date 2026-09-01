-- mart_om_spend.sql
-- Purpose: O&M spend analytics with risk and asset KPIs

{{ config(
    materialized='table',
    schema='MARTS'
) }}

with spend as (
    select * from {{ ref('stg_om_spend') }}
),

final as (
    select
        -- === Keys ===
        spend_id,
        spend_year,
        spend_month,
        month_name,
        city_name,

        -- === Spend Details ===
        asset_type,
        asset_category,
        cost_category,
        amount_usd,
        spend_tier,
        avg_risk_score,

        -- === Calculated KPIs ===
        -- Cost per risk point (how much does each risk unit cost?)
        case
            when avg_risk_score > 0
            then round(amount_usd / avg_risk_score, 2)
            else amount_usd
        end as cost_per_risk_point,

        -- Season derived from month
        case
            when spend_month in (12, 1, 2) then 'Winter'
            when spend_month in (3, 4, 5)  then 'Spring'
            when spend_month in (6, 7, 8)  then 'Summer'
            when spend_month in (9, 10, 11) then 'Fall'
        end as season,

        -- High spend flag
        case
            when amount_usd > 20000 then 1
            else 0
        end as is_high_spend

    from spend
)

select * from final