{{ config(
    materialized='table',
    tags=['marts', 'equity', 'business_intelligence']
) }}

with fact_sla as (
    -- We pull from the transaction fact we just built.
    -- For performance and relevance, we might restrict this to the last rolling 12 months.
    select * from {{ ref('fct_sla_performance') }}
),

citywide_baseline as (
    -- Step 1: Calculate the macro average for each request type across the entire city
    select
        request_type_id,
        avg(resolution_days) as citywide_avg_resolution_days,
        count(service_request_number) as citywide_total_requests
    from fact_sla
    group by 1
),

area_performance as (
    -- Step 2: Calculate the micro average for each specific community area + request type
    select
        community_area_id,
        request_type_id,
        avg(resolution_days) as area_avg_resolution_days,
        count(service_request_number) as area_total_requests,
        sum(case when is_breached then 1 else 0 end) as area_breached_requests
    from fact_sla
    group by 1, 2
),

equity_calculation as (
    -- Step 3: Combine micro and macro to calculate the index
    select
        a.community_area_id,
        a.request_type_id,
        a.area_total_requests,
        a.area_breached_requests,
        -- Use SAFE_DIVIDE to prevent Division by Zero errors
        safe_divide(a.area_breached_requests, a.area_total_requests) as area_breach_rate,
        a.area_avg_resolution_days,
        c.citywide_avg_resolution_days,
        c.citywide_total_requests,
        
        -- The Equity Index Formula
        safe_divide(a.area_avg_resolution_days, c.citywide_avg_resolution_days) as equity_index

    from area_performance a
    join citywide_baseline c
        on a.request_type_id = c.request_type_id
        
    -- Senior Data Quality Check: Filter out micro-sample sizes. 
    -- If an area only had 1 Pothole ticket and it took 100 days, it skews the index.
    where a.area_total_requests >= 10
),

final_presentation as (
    -- Step 4: Rejoin to the dimensions to provide human-readable strings for the BI tool
    select
        -- Geographic Context
        geo.community_area_name,
        geo.city_side,
        
        -- Request Context
        req.request_type_name,
        req.priority_level,
        
        -- Metrics
        e.area_total_requests,
        e.area_breach_rate,
        e.area_avg_resolution_days,
        e.citywide_avg_resolution_days,
        round(e.equity_index, 2) as equity_index,
        
        -- Business Classification (This creates the color-coding for your map)
        case
            when e.equity_index >= 1.25 then 'Severely Underserved (25%+ Slower)'
            when e.equity_index >= 1.10 then 'Underserved (10-24% Slower)'
            when e.equity_index <= 0.90 then 'Over-served (Faster than Average)'
            else 'Equitable (Average)'
        end as equity_status

    from equity_calculation e
    left join {{ ref('dim_geography') }} geo 
        on e.community_area_id = geo.community_area_id
    left join {{ ref('dim_request_type') }} req 
        on e.request_type_id = req.request_type_id
)

select * from final_presentation