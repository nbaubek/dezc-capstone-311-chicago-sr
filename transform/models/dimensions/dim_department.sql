{{ config(
    materialized='table',
    tags=['dimensions']
) }}

with staging_departments as (
    -- Extract unique departments from the raw service requests
    select distinct
        owner_department
    from {{ ref('stg_service_requests') }}
    where owner_department is not null
),

department_seed as (
    -- Pull the clean names and bureau groupings from your seed file
    select
        owner_department,
        display_name,
        bureau
    from {{ ref('department_metadata') }}
),

enriched_departments as (
    select
        -- Generate the high-performance surrogate key
        farm_fingerprint(sd.owner_department) as department_id,
        
        -- The raw string as it appears in Socrata (for lineage/debugging)
        sd.owner_department as raw_department_name,

        -- The enriched, clean presentation names (fallback to raw if missing from seed)
        coalesce(ds.display_name, sd.owner_department) as department_name,
        ds.bureau,

        current_timestamp() as dbt_updated_at

    from staging_departments sd
    left join department_seed ds
        on sd.owner_department = ds.owner_department
)

select * from enriched_departments