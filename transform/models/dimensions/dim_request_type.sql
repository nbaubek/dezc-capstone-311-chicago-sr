{{ config(
    materialized='table',
    tags=['dimensions']
) }}

with staging_requests as (
    -- Extract unique request types and their short codes
    select distinct
        sr_type,
        sr_short_code
    from {{ ref('stg_service_requests') }}
    where sr_type is not null
),

sla_seed as (
    -- Pull the SLA targets and priorities from the seed file
    select
        sr_type,
        cast(target_days as INT64) as target_days,
        priority as priority_level,
        sla_target_source
    from {{ ref('sla_targets') }}
),

enriched_request_types as (
    select
        -- Generate a unique integer ID based on the request type string
        farm_fingerprint(sr.sr_type) as request_type_id,
        
        -- Natural Keys / Base Attributes
        sr.sr_type as request_type_name,
        sr.sr_short_code,

        -- SLA Metrics (Joined from Seed)
        sl.target_days as sla_target_days,
        sl.priority_level,
        sl.sla_target_source,

        -- Standard Dimension Metadata
        current_timestamp() as dbt_updated_at

    from staging_requests sr
    left join sla_seed sl 
        on sr.sr_type = sl.sr_type
)

select * from enriched_request_types