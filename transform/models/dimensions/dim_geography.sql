{{ config(
    materialized='table',
    tags=['dimensions']
) }}

-- IMPORTANT: dim_geography must have exactly ONE row per community_area_id.
-- A community area spans multiple wards/police districts — those attributes must NOT
-- be in this dimension, or every fact table join will fan out (Cartesian explosion).
-- Ward is now in fct_service_requests directly; do NOT re-add it here.

with community_area_seed as (
    select
        cast(area_number as INT64) as area_number,
        area_name,
        side as city_side,
        ST_GEOGFROMTEXT(the_geom) as area_boundary,
        cast(replace(cast(shape_area as STRING), ',', '') as FLOAT64) as shape_area,
        cast(replace(cast(shape_len as STRING), ',', '') as FLOAT64) as shape_length
    from {{ ref('community_areas') }}
),

geography as (
    select
        area_number as community_area_id,
        area_name as community_area_name,
        city_side,
        area_boundary,
        shape_area,
        shape_length,
        current_timestamp() as dbt_updated_at
    from community_area_seed
)

select * from geography
