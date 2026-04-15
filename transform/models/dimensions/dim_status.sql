{{ config(
    materialized='table',
    tags=['dimensions']
) }}

with staging_statuses as (
    -- Extract unique ticket statuses from the raw service requests
    select distinct
        status
    from {{ ref('stg_service_requests') }}
    where status is not null
),

status_dimension as (
    select
        -- Generate a unique integer ID based on the status string
        farm_fingerprint(status) as status_id,
        
        -- Natural Key / Attribute
        status as status_name,

        -- Standard Dimension Metadata
        current_timestamp() as dbt_updated_at

    from staging_statuses
)

select * from status_dimension