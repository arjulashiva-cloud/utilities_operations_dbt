{{ config(materialized='view') }}

with source as (
    select * from UTILITIES_RAW.PUBLIC.RAW_EIA_SUPPLY_DISPOSITION
),

-- Pivot from long format to wide (one row per period + sector)
pivoted as (
    select
        period,
        state_id,
        state_name,
        sector,
        sector_name,

        max(case when data_type = 'customers' then value end) as customers,
        max(case when data_type = 'revenue'   then value end) as revenue_million_usd,
        max(case when data_type = 'sales'     then value end) as sales_million_kwh,
        max(case when data_type = 'price'     then value end) as price_cents_per_kwh

    from source
    group by period, state_id, state_name, sector, sector_name
),

final as (
    select
        to_date(period, 'YYYY-MM')                          as sale_month,
        state_id,
        state_name,
        sector,
        sector_name,
        customers,
        revenue_million_usd,
        sales_million_kwh,
        price_cents_per_kwh,

        -- Convert to more intuitive units
        round(sales_million_kwh * 1000, 0)                  as sales_mwh,
        round(revenue_million_usd * 1000000, 0)             as revenue_usd,
        round(price_cents_per_kwh / 100, 4)                 as price_usd_per_kwh,

        -- Revenue per customer (monthly)
        round(
            case
                when customers > 0
                then (revenue_million_usd * 1000000) / customers
                else null
            end, 2
        )                                                   as revenue_per_customer_usd,

        -- Date parts for easy filtering in Power BI
        year(to_date(period, 'YYYY-MM'))                    as year,
        month(to_date(period, 'YYYY-MM'))                   as month,
        monthname(to_date(period, 'YYYY-MM'))               as month_name,

        case
            when month(to_date(period, 'YYYY-MM')) in (12,1,2) then 'Winter'
            when month(to_date(period, 'YYYY-MM')) in (3,4,5)  then 'Spring'
            when month(to_date(period, 'YYYY-MM')) in (6,7,8)  then 'Summer'
            else 'Fall'
        end                                                 as season,

        current_timestamp()                                 as created_at

    from pivoted
    where sector != 'ALL'   -- exclude the aggregate row; marts will sum it themselves
)

select * from final