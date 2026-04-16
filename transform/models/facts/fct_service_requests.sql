{{ config(
    materialized='incremental',
    unique_key='service_request_number',
    partition_by={
      "field": "created_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['request_type_id', 'department_id', 'community_area_id', 'status_id'],
    tags=['facts']
) }}

with staging_requests as (
    select 
        *,
        -- Standardize dates for partitioning and joining
        date(created_date) as created_date_key,
        date(closed_date) as closed_date_key
    from {{ ref('stg_service_requests') }}
    
    {% if is_incremental() %}
        -- The core accumulating snapshot logic: 
        -- Only process rows that were created or updated since the last dbt run.
        -- We add a 3-hour lookback buffer to catch any late-arriving Socrata API syncs.
        where last_modified_date >= timestamp_sub(
            (select max(last_modified_date) from {{ this }}), 
            interval 3 hour
        )
    {% endif %}
),

-- Pull in the dimension SLA targets to calculate breaches on the fly
dim_requests as (
    select request_type_id, sla_target_days
    from {{ ref('dim_request_type') }}
),

fact_build as (
    select
        -- Primary Key
        sr.service_request_number,

        -- Surrogate Keys (Foreign Keys to Dimensions)
        farm_fingerprint(sr.sr_type) as request_type_id,
        farm_fingerprint(sr.owner_department) as department_id,
        sr.community_area as community_area_id,
        farm_fingerprint(sr.current_status) as status_id,

        -- Dates & Timestamps
        sr.created_date_key as created_date,
        sr.closed_date_key as closed_date,
        sr.created_date as created_timestamp,
        sr.closed_date as closed_timestamp,
        sr.last_modified_date,

        -- Location Coordinates (Keeping raw Lat/Lon in Fact for pinpoint mapping,
        -- while keeping the heavy Multipolygon in the Geography Dimension)
        sr.latitude,
        sr.longitude,

        -- Ward (added directly to fact; do NOT join from dim_geography which has
        -- multiple rows per community_area and would cause a Cartesian fan-out)
        sr.ward,

        -- Core Fact Metrics
        sr.resolution_days,
        
        -- Business Logic: SLA Performance
        case 
            when sr.current_status = 'Completed' and sr.resolution_days <= dr.sla_target_days then true
            when sr.current_status = 'Completed' and sr.resolution_days > dr.sla_target_days then false
            else null -- Not completed yet
        end as is_sla_met,

        case
            when sr.current_status = 'Completed' and sr.resolution_days > dr.sla_target_days then true
            else false 
        end as is_sla_breached,

        -- Audit
        current_timestamp() as dbt_updated_at

    from staging_requests sr
    left join dim_requests dr
        on farm_fingerprint(sr.sr_type) = dr.request_type_id
)

select * from fact_build