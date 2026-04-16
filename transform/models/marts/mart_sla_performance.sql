{{ config(
    materialized='table',
    tags=['marts', 'equity', 'business_intelligence']
) }}

with fact_sla as (
    select * from {{ ref('fct_sla_performance') }}
),

citywide_baseline as (
    select
        request_type_id,
        avg(resolution_days) as citywide_avg_resolution_days,
        count(service_request_number) as citywide_total_requests
    from fact_sla
    group by 1
),

area_performance as (
    select
        community_area_id,
        request_type_id,
        department_id, -- 1. WE ADD DEPARTMENT HERE
        avg(resolution_days) as area_avg_resolution_days,
        count(service_request_number) as area_total_requests,
        sum(case when is_breached then 1 else 0 end) as area_breached_requests
    from fact_sla
    group by 1, 2, 3 -- 2. WE MUST ADD IT TO THE GROUP BY
),

equity_calculation as (
    select
        a.community_area_id,
        a.request_type_id,
        a.department_id, -- 3. PASS IT THROUGH THE MATH CTE
        a.area_total_requests,
        a.area_breached_requests,
        safe_divide(a.area_breached_requests, a.area_total_requests) as area_breach_rate,
        a.area_avg_resolution_days,
        c.citywide_avg_resolution_days,
        c.citywide_total_requests,
        safe_divide(a.area_avg_resolution_days, c.citywide_avg_resolution_days) as equity_index

    from area_performance a
    join citywide_baseline c
        on a.request_type_id = c.request_type_id
    where a.area_total_requests >= 10
),

final_presentation as (
    select
        geo.community_area_name,
        geo.city_side,
        req.request_type_name,
        req.priority_level,
        
        dep.department_name,
        dep.bureau,
        
        e.area_total_requests,
        e.area_breached_requests,
        e.area_breach_rate,
        e.area_avg_resolution_days,
        e.citywide_avg_resolution_days,
        round(e.equity_index, 2) as equity_index,
        
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
    -- 5. AND JOIN ON THE EXACT ID
    left join {{ ref('dim_department') }} dep
        on e.department_id = dep.department_id
)

select * from final_presentation