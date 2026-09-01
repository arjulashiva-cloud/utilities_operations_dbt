-- mart_operations_summary.sql
-- Purpose: Master daily operations summary combining all domains
-- Grain: One row per city per day

{{ config(
    materialized='table',
    schema='MARTS'
) }}

with weather as (
    select * from {{ ref('mart_weather_operations') }}
),

-- Aggregate outages to city + day level
outages_agg as (
    select
        outage_date                             as summary_date,
        city_name,
        count(outage_id)                        as total_outages,
        sum(customers_affected)                 as total_customers_affected,
        sum(customer_minutes_lost)              as total_customer_minutes_lost,
        round(avg(duration_hours), 2)           as avg_outage_duration_hrs,
        max(outage_severity_score)              as max_outage_severity,
        sum(is_weather_driven)                  as weather_driven_outages,
        sum(case when outage_type = 'Unplanned'
            then 1 else 0 end)                  as unplanned_outages
    from {{ ref('mart_outages_operations') }}
    group by outage_date, city_name
),

-- Aggregate workforce to city + day level
workforce_agg as (
    select
        work_date                               as summary_date,
        city_name,
        count(record_id)                        as total_crews_deployed,
        sum(hours_worked)                       as total_hours_worked,
        round(sum(labor_cost), 2)               as total_labor_cost,
        round(sum(overtime_premium_cost), 2)    as total_overtime_cost,
        sum(is_emergency_deployment)            as emergency_deployments,
        sum(is_overtime)                        as overtime_shifts
    from {{ ref('mart_workforce_operations') }}
    group by work_date, city_name
),

-- Aggregate safety to city + day level
safety_agg as (
    select
        obs_date                                as summary_date,
        city_name,
        count(obs_id)                           as total_observations,
        sum(is_incident)                        as total_incidents,
        round(avg(severity_score), 2)           as avg_severity_score,
        max(safety_risk_index)                  as max_safety_risk_index,
        sum(is_overdue)                         as overdue_closures,
        sum(case when observation_category = 'Positive'
            then 1 else 0 end)                  as positive_observations
    from {{ ref('mart_safety_operations') }}
    group by obs_date, city_name
),

-- Join everything together
final as (
    select
        -- === Date & City ===
        w.weather_date                          as summary_date,
        w.city_name,
        w.season,
        w.year,
        w.month_num,
        w.week_num,

        -- === Weather ===
        w.temp_max_f,
        w.temp_min_f,
        w.temp_avg_f,
        w.precipitation_inches,
        w.wind_speed_max_mph,
        w.weather_description,
        w.heating_degree_days,
        w.cooling_degree_days,
        w.ops_risk_score,
        w.ops_risk_label,

        -- === Outage KPIs ===
        coalesce(o.total_outages, 0)                    as total_outages,
        coalesce(o.total_customers_affected, 0)         as total_customers_affected,
        coalesce(o.total_customer_minutes_lost, 0)      as total_customer_minutes_lost,
        coalesce(o.avg_outage_duration_hrs, 0)          as avg_outage_duration_hrs,
        coalesce(o.max_outage_severity, 0)              as max_outage_severity,
        coalesce(o.weather_driven_outages, 0)           as weather_driven_outages,
        coalesce(o.unplanned_outages, 0)                as unplanned_outages,

        -- === Workforce KPIs ===
        coalesce(wf.total_crews_deployed, 0)            as total_crews_deployed,
        coalesce(wf.total_hours_worked, 0)              as total_hours_worked,
        coalesce(wf.total_labor_cost, 0)                as total_labor_cost,
        coalesce(wf.total_overtime_cost, 0)             as total_overtime_cost,
        coalesce(wf.emergency_deployments, 0)           as emergency_deployments,
        coalesce(wf.overtime_shifts, 0)                 as overtime_shifts,

        -- === Safety KPIs ===
        coalesce(s.total_observations, 0)               as total_observations,
        coalesce(s.total_incidents, 0)                  as total_incidents,
        coalesce(s.avg_severity_score, 0)               as avg_severity_score,
        coalesce(s.max_safety_risk_index, 0)            as max_safety_risk_index,
        coalesce(s.overdue_closures, 0)                 as overdue_closures,
        coalesce(s.positive_observations, 0)            as positive_observations,

        -- === Cross-Domain KPIs ===
        -- Cost per outage
        case
            when coalesce(o.total_outages, 0) > 0
            then round(wf.total_labor_cost / o.total_outages, 2)
            else 0
        end as labor_cost_per_outage,

        -- Incident rate (incidents per crew deployed)
        case
            when coalesce(wf.total_crews_deployed, 0) > 0
            then round(coalesce(s.total_incidents, 0) * 1.0
                 / wf.total_crews_deployed, 4)
            else 0
        end as incident_rate_per_crew,

        -- Overall operational stress score (0-10)
        round(
            (w.ops_risk_score * 1.5) +
            (coalesce(o.max_outage_severity, 0) * 1.0) +
            (coalesce(s.avg_severity_score, 0) * 1.0)
        , 1) as operational_stress_score

    from weather w
    left join outages_agg  o  on w.weather_date = o.summary_date
                              and w.city_name   = o.city_name
    left join workforce_agg wf on w.weather_date = wf.summary_date
                               and w.city_name   = wf.city_name
    left join safety_agg   s  on w.weather_date = s.summary_date
                              and w.city_name   = s.city_name
)

select * from final