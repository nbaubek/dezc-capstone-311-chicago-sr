{{ config(
    materialized='incremental',
    unique_key='service_request_number',
    partition_by={
      "field": "closed_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['community_area_id', 'request_type_id'],
    tags=['facts', 'performance']
) }}

with staging_requests as (
    -- We only pull tickets that have reached a terminal 'Completed' state
    select *
    from {{ ref('stg_service_requests') }}
    where current_status = 'Completed'
    
    {% if is_incremental() %}
        -- Append-only logic: We only grab tickets that were closed since our last run.
        -- We use closed_date instead of last_modified_date because closure is the transaction event.
        and date(closed_date) >= (select max(closed_date) from {{ this }})
    {% endif %}
),

dim_requests as (
    select request_type_id, sla_target_days
    from {{ ref('dim_request_type') }}
),

transaction_build as (
    select
        -- Primary Key
        sr.service_request_number,

        -- Surrogate Keys
        farm_fingerprint(sr.sr_type) as request_type_id,
        farm_fingerprint(sr.owner_department) as department_id,
        sr.community_area as community_area_id,

        -- Transaction Event Dates
        date(sr.closed_date) as closed_date,
        sr.closed_date as closed_timestamp,
        date(sr.created_date) as created_date,

        -- Performance Metrics
        sr.resolution_days,
        dr.sla_target_days,
        
        -- Variance Math (Positive means it breached, Negative means it was fast)
        (sr.resolution_days - dr.sla_target_days) as days_over_sla,

        -- Boolean Flags for easy dashboard filtering
        case 
            when sr.resolution_days > dr.sla_target_days then true
            else false 
        end as is_breached,

        current_timestamp() as dbt_updated_at

    from staging_requests sr
    left join dim_requests dr
        on farm_fingerprint(sr.sr_type) = dr.request_type_id
)

select * from transaction_build