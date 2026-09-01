{{ config(materialized='view') }}

with nodes as (
    select
        node_id,
        node_type,
        city,
        voltage_kv,
        capacity_mw,
        latitude,
        longitude,
        degree_centrality,
        betweenness_centrality,
        created_at,

        -- Classify criticality based on betweenness
        case
            when betweenness_centrality >= 0.5  then 'Critical'
            when betweenness_centrality >= 0.2  then 'Important'
            when betweenness_centrality >= 0.05 then 'Standard'
            else 'Peripheral'
        end as criticality_tier,

        -- Classify voltage level
        case
            when voltage_kv >= 345 then 'Extra High Voltage'
            when voltage_kv >= 230 then 'High Voltage'
            when voltage_kv >= 115 then 'Medium Voltage'
            else 'Low Voltage'
        end as voltage_class,

        -- Flag top hubs
        case when degree_centrality >= 0.3 then 1 else 0 end as is_major_hub

    from UTILITIES_RAW.PUBLIC.RAW_GRID_NODES
),

edges as (
    select
        source_node,
        target_node,
        distance_miles,
        capacity_mw,
        line_type,
        created_at,

        -- Classify line capacity
        case
            when capacity_mw >= 1000 then 'High Capacity'
            when capacity_mw >= 500  then 'Medium Capacity'
            else 'Low Capacity'
        end as capacity_tier,

        -- Distance bucket
        case
            when distance_miles <= 10  then 'Local'
            when distance_miles <= 40  then 'Regional'
            else 'Interstate'
        end as distance_bucket

    from UTILITIES_RAW.PUBLIC.RAW_GRID_EDGES
)

select * from nodes