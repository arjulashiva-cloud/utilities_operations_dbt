{{ config(materialized='table', schema='MARTS') }}

with nodes as (
    select * from {{ ref('stg_grid') }}
),

edges as (
    select
        source_node,
        target_node,
        distance_miles,
        capacity_mw,
        line_type,

        case
            when capacity_mw >= 1000 then 'High Capacity'
            when capacity_mw >= 500  then 'Medium Capacity'
            else 'Low Capacity'
        end as capacity_tier,

        case
            when distance_miles <= 10  then 'Local'
            when distance_miles <= 40  then 'Regional'
            else 'Interstate'
        end as distance_bucket

    from UTILITIES_RAW.PUBLIC.RAW_GRID_EDGES
),

-- Aggregate edge stats per node (how many lines connect to each node)
node_edge_stats as (
    select
        source_node as node_id,
        count(*)                    as connected_lines,
        sum(capacity_mw)            as total_edge_capacity_mw,
        round(avg(distance_miles),1) as avg_line_distance_miles,
        max(capacity_mw)            as max_single_line_capacity

    from edges
    group by source_node

    union all

    select
        target_node as node_id,
        count(*),
        sum(capacity_mw),
        round(avg(distance_miles),1),
        max(capacity_mw)
    from edges
    group by target_node
),

-- Deduplicate (each node appears as both source and target)
node_stats as (
    select
        node_id,
        sum(connected_lines)             as connected_lines,
        sum(total_edge_capacity_mw)      as total_edge_capacity_mw,
        round(avg(avg_line_distance_miles),1) as avg_line_distance_miles,
        max(max_single_line_capacity)    as max_single_line_capacity
    from node_edge_stats
    group by node_id
),

final as (
    select
        n.node_id,
        n.node_type,
        n.city,
        n.voltage_kv,
        n.voltage_class,
        n.capacity_mw,
        n.latitude,
        n.longitude,
        n.degree_centrality,
        n.betweenness_centrality,
        n.criticality_tier,
        n.is_major_hub,

        -- Edge aggregates
        coalesce(s.connected_lines, 0)              as connected_lines,
        coalesce(s.total_edge_capacity_mw, 0)       as total_edge_capacity_mw,
        coalesce(s.avg_line_distance_miles, 0)      as avg_line_distance_miles,
        coalesce(s.max_single_line_capacity, 0)     as max_single_line_capacity,

        -- Resilience score (higher = more resilient)
        -- Nodes with more connections and higher betweenness need redundancy
        round(
            (n.degree_centrality * 50) +
            (n.betweenness_centrality * 50),
        1) as network_importance_score,

        -- Capacity utilization proxy
        round(
            coalesce(s.total_edge_capacity_mw, 0) / nullif(n.capacity_mw, 0) * 100,
        1) as throughput_ratio_pct,

        n.created_at

    from nodes n
    left join node_stats s on n.node_id = s.node_id
)

select * from final
order by betweenness_centrality desc