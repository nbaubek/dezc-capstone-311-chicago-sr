-- Operational mart for daily operations dashboard
-- Focuses on current state of open requests, backlogs, and recent breaches

{{ config(
    materialized='table',
    tags=['marts', 'operations']
) }}

with sla_performance as (
    select * from {{ ref('int_sla_performance') }}
),

open_requests as (
    select
        service_request_number,
        created_date,
        sr_type,
        current_status,
        owner_department,
        community_area,
        ward,
        days_open,
        sla_target_days,
        is_sla_at_risk,
        dbt_loaded_at
    from sla_performance
    where current_status not like '%Completed%'
),

department_backlog as (
    select
        owner_department,
        count(*) as open_request_count,
        count(case when is_sla_at_risk then 1 end) as overdue_count,
        avg(days_open) as avg_days_open,
        max(days_open) as max_days_open
    from open_requests
    group by owner_department
),

type_backlog as (
    select
        sr_type,
        count(*) as open_request_count,
        count(case when is_sla_at_risk then 1 end) as overdue_count,
        avg(days_open) as avg_days_open
    from open_requests
    group by sr_type
),

recent_breaches as (
    select
        service_request_number,
        created_date,
        closed_date,
        sr_type,
        owner_department,
        resolution_days,
        sla_target_days,
        days_from_sla_target,
        dbt_loaded_at
    from sla_performance
    where
        is_sla_breach = true
        and closed_date >= date_sub(current_date(), interval 7 day)
)

-- Union the three operational views
select 'department_backlog' as view_type, null as service_request_number, * except(view_type)
from department_backlog

union all

select 'type_backlog' as view_type, null as service_request_number, * except(view_type)
from type_backlog

union all

select 'recent_breaches' as view_type, * except(view_type)
from recent_breaches
