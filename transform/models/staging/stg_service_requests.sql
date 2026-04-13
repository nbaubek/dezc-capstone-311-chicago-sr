-- Staging model for Chicago 311 service requests
-- Cleans, casts, and renames columns from the raw Iceberg source

{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (

    select * from {{ source('ice_lakehouse', 'service_requests') }}

),

renamed_and_filtered as (

    select
        -- Primary key
        service_request_number,

        -- Request type info
        sr_type,
        sr_short_code,

        -- Status and ownership
        current_status,
        owner_department,
        origin,

        -- Dates
        created_date,
        last_modified_date,
        closed_date,

        -- Geography
        street_address,
        zip_code,
        street_number,
        street_direction,
        street_name,
        street_type,
        community_area,
        ward,
        police_sector,
        police_district,
        police_beat,
        precinct,
        latitude,
        longitude,
        x_coordinate,
        y_coordinate,

        -- Flags
        duplicate,
        legacy_record,

        -- Derived temporal columns (generated during Polars ingestion)
        created_year,
        created_month,
        created_hour,
        created_day_of_week,

        -- Additional fields
        created_department,
        electrical_district,
        electricity_grid,
        parent_sr_number

    from source
    -- BR-002: Legacy record exclusion. Pre-2018 records are filtered out at the staging layer.
    where legacy_record = false

),

calculated as (

    select
        *,

        -- BR-006: Open request age. Calculated as calendar days between current date and creation date for open requests.
        case
            when closed_date is null then date_diff(current_date(), date(created_date), day)
            else null
        end as days_open,

        -- BR-004: Resolution time calculation. Calculated as calendar days between creation and closure.
        case
            when closed_date is not null then date_diff(date(closed_date), date(created_date), day)
            else null
        end as resolution_days,

        -- Lineage: Audit timestamp for when dbt processed this specific run
        current_timestamp() as dbt_loaded_at

    from renamed_and_filtered

)

select * from calculated
