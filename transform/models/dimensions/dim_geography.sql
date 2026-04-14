{{ config(
    materialized='table',
    tags=['dimensions']
) }}

with staging_locations as (
    -- Extract unique geographic combinations from the raw service requests
    select distinct
        community_area,
        ward,
        police_sector,
        police_district,
        police_beat,
        precinct
    from {{ ref('stg_service_requests') }}
    where community_area is not null
),

community_area_seed as (
    -- Pull the enriched boundaries, names, and metrics from the updated seed file
    select
        cast(area_number as INT64) as area_number,
        area_name,
        side as city_side,
        
        -- Convert the Socrata text boundary into a native BigQuery Geography object
        ST_GEOGFROMTEXT(the_geom) as area_boundary,
        
        -- Strip out the string commas from the CSV before casting to Float
        cast(replace(shape_area, ',', '') as FLOAT64) as shape_area,
        cast(replace(shape_len, ',', '') as FLOAT64) as shape_length
        
    from {{ ref('community_areas') }}
),

enriched_geography as (
    select
        -- Primary Geographic Grain
        sl.community_area as community_area_id,
        cas.area_name as community_area_name,
        cas.city_side,

        -- Administrative Boundaries (Attributes of the Community Area)
        sl.ward,
        sl.police_sector,
        sl.police_district,
        sl.police_beat,
        sl.precinct,

        -- Spatial Data for BI Mapping & Density Analytics
        cas.area_boundary,
        cas.shape_area,
        cas.shape_length,

        -- Standard Dimension Metadata
        current_timestamp() as dbt_updated_at

    from staging_locations sl
    left join community_area_seed cas
        on sl.community_area = cas.area_number
)

select * from enriched_geography