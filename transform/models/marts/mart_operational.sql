{{ config(
    materialized='view',
    tags=['marts', 'operations', 'dispatch']
) }}

with open_requests as (
    -- We only care about tickets currently sitting in the backlog
    select *
    from {{ ref('fct_service_requests') }}
    where closed_timestamp is null
),

enrichment as (
    select
        -- Core Ticket Info
        f.service_request_number,
        f.created_date,
        f.last_modified_date,

        -- Geographic Context (Pinpoint + Neighborhood)
        geo.community_area_name,
        geo.ward,
        f.latitude,
        f.longitude,

        -- Organization Context
        dep.department_name,
        dep.bureau,

        -- Request Details & SLA Targets
        req.request_type_name,
        req.priority_level,
        req.sla_target_days,
        stat.status_name,

        -- Operational Math: How many calendar days has this been open?
        date_diff(current_date(), f.created_date, DAY) as days_open

    from open_requests f
    left join {{ ref('dim_geography') }} geo 
        on f.community_area_id = geo.community_area_id
    left join {{ ref('dim_request_type') }} req 
        on f.request_type_id = req.request_type_id
    left join {{ ref('dim_department') }} dep 
        on f.department_id = dep.department_id
    left join {{ ref('dim_status') }} stat 
        on f.status_id = stat.status_id
),

triage_logic as (
    select
        *,
        -- Negative numbers mean the ticket is overdue
        (sla_target_days - days_open) as days_until_breach,

        -- Business Logic: The Dispatch Triage Bucket
        case
            when (sla_target_days - days_open) < 0 then '1 - Overdue'
            when (sla_target_days - days_open) = 0 then '2 - Due Today'
            when (sla_target_days - days_open) between 1 and 2 then '3 - At Risk'
            else '4 - On Track'
        end as triage_status

    from enrichment
)

select * from triage_logic