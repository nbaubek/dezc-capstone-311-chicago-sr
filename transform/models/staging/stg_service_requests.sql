-- Staging model for Chicago 311 service requests
-- Cleans, casts, and renames columns from the raw Iceberg source

{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('ice_lakehouse', 'service_requests') }}
),

renamed as (
    select
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
        duplicate,
        legacy_record,
        last_modified_date,
        -- Calculate days open for still-open requests
        case
            when closed_date is null then date_diff(current_date(), date(created_date), day)
            else null
        end as days_open,
        -- Calculate resolution time for closed requests
        case
            when closed_date is not null then date_diff(date(closed_date), date(created_date), day)
            else null
        end as resolution_days,
        -- Add load timestamp for lineage
        current_timestamp() as dbt_loaded_at
    from source
),

filtered as (
    select *
    from renamed
    where
        -- Filter out legacy records (pre-2018 system)
        not coalesce(legacy_record, false)
        -- Filter out duplicates for fact tables
        and not coalesce(duplicate, false)
)

select * from filtered
