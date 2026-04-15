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

department_dimension as (
    select
        -- Generate a unique integer ID based on the department string
        farm_fingerprint(owner_department) as department_id,
        
        -- Natural Key / Attributes
        owner_department as department_name,

        -- Standard Dimension Metadata
        current_timestamp() as dbt_updated_at

    from staging_departments
)

select * from department_dimension