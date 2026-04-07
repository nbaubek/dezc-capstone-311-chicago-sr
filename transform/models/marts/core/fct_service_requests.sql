-- Fact table for service requests
-- Core accumulating snapshot fact table for the data warehouse

{{ config(
    materialized='table',
    tags=['marts', 'core']
) }}

with sla_performance as (
    select * from {{ ref('int_sla_performance') }}
),

-- Add surrogate key
with_surrogate_key as (
    select
        {{ dbt_utils.surrogate_key(['service_request_number']) }} as request_key,
        *
    from sla_performance
)

select
    request_key,
    service_request_number,
    created_date,
    closed_date,
    sr_type,
    current_status,
    owner_department,
    created_year,
    created_month,
    community_area,
    ward,
    latitude,
    longitude,
    zip_code,
    days_open,
    resolution_days,
    sla_target_days,
    sla_category,
    sla_target_source,
    is_sla_breach,
    is_sla_at_risk,
    days_from_sla_target,
    dbt_loaded_at
from with_surrogate_key
