{{ config(materialized='table', schema='MARTS') }}

with sales as (
    select * from {{ ref('stg_eia_supply_disposition') }}
),

-- Latest 12 months for trend context
latest_period as (
    select max(sale_month) as max_month from sales
),

sector_monthly as (
    select
        s.sale_month,
        s.year,
        s.month,
        s.month_name,
        s.season,
        s.sector,
        s.sector_name,

        -- Raw metrics
        s.customers,
        s.revenue_usd,
        s.sales_mwh,
        s.price_cents_per_kwh,
        s.price_usd_per_kwh,
        s.revenue_per_customer_usd,

        -- Month-over-month sales change
        s.sales_mwh - lag(s.sales_mwh) over (
            partition by s.sector
            order by s.sale_month
        )                                                   as sales_mwh_mom_change,

        -- Year-over-year sales change
        s.sales_mwh - lag(s.sales_mwh, 12) over (
            partition by s.sector
            order by s.sale_month
        )                                                   as sales_mwh_yoy_change,

        -- Price trend (is electricity getting cheaper or more expensive?)
        s.price_cents_per_kwh - lag(s.price_cents_per_kwh, 12) over (
            partition by s.sector
            order by s.sale_month
        )                                                   as price_yoy_change_cents,

        -- Is this a peak demand month for this sector?
        case
            when s.sales_mwh = max(s.sales_mwh) over (partition by s.sector)
            then 1 else 0
        end                                                 as is_sector_peak_month,

        -- Sector label for Power BI display
        case s.sector
            when 'RES' then 'Residential 🏠'
            when 'COM' then 'Commercial 🏢'
            when 'IND' then 'Industrial 🏭'
            when 'TRA' then 'Transportation 🚗'
            when 'OTH' then 'Other'
            else s.sector_name
        end                                                 as sector_display,

        -- Price tier for each sector (industrial gets cheapest rates)
        case
            when s.price_cents_per_kwh >= 15 then 'Premium Rate'
            when s.price_cents_per_kwh >= 12 then 'Standard Rate'
            when s.price_cents_per_kwh >= 9  then 'Discounted Rate'
            else                                  'Industrial Rate'
        end                                                 as price_tier,

        -- Flag last 12 months for dashboard default view
        case
            when s.sale_month >= dateadd(month, -12, l.max_month)
            then 1 else 0
        end                                                 as is_last_12_months,

        current_timestamp()                                 as created_at

    from sales s
    cross join latest_period l
)

select * from sector_monthly