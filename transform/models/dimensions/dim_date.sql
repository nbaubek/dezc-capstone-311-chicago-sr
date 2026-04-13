{{ config(
    materialized='table',
    tags=['dimensions']
) }}

with date_spine as (
    -- Generates one row per day from Jan 1, 2024 to Dec 31, 2026
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
),

raw_dates as (
    select cast(date_day as date) as date_day
    from date_spine
),

illinois_holidays as (
    -- Explicitly defined Illinois State Holidays for the project scope (2024-2026)
    -- Includes New Year, MLK, Lincoln's B-day, Presidents, Pulaski, Memorial, 
    -- Juneteenth, Independence, Labor, Columbus, Election (even years), Veterans, Thanksgiving + Day After, Christmas
    select date_array as holiday_date 
    from unnest([
        -- 2024
        date('2024-01-01'), date('2024-01-15'), date('2024-02-12'), date('2024-02-19'), 
        date('2024-03-04'), date('2024-05-27'), date('2024-06-19'), date('2024-07-04'), 
        date('2024-09-02'), date('2024-10-14'), date('2024-11-05'), date('2024-11-11'), 
        date('2024-11-28'), date('2024-11-29'), date('2024-12-25'),
        -- 2025
        date('2025-01-01'), date('2025-01-20'), date('2025-02-12'), date('2025-02-17'), 
        date('2025-03-03'), date('2025-05-26'), date('2025-06-19'), date('2025-07-04'), 
        date('2025-09-01'), date('2025-10-13'), date('2025-11-11'), date('2025-11-27'), 
        date('2025-11-28'), date('2025-12-25'),
        -- 2026
        date('2026-01-01'), date('2026-01-19'), date('2026-02-12'), date('2026-02-16'), 
        date('2026-03-02'), date('2026-05-25'), date('2026-06-19'), date('2026-07-03'), 
        date('2026-09-07'), date('2026-10-12'), date('2026-11-03'), date('2026-11-11'), 
        date('2026-11-26'), date('2026-11-27'), date('2026-12-25')
    ]) as date_array
),

enriched_dates as (
    select
        d.date_day,
        
        -- Standard Calendar Attributes
        extract(year from d.date_day) as date_year,
        extract(month from d.date_day) as date_month,
        format_date('%B', d.date_day) as month_name,
        extract(day from d.date_day) as day_of_month,
        extract(dayofweek from d.date_day) as day_of_week_num,
        format_date('%A', d.date_day) as day_of_week_name,
        extract(quarter from d.date_day) as date_quarter,
        extract(dayofyear from d.date_day) as day_of_year,

        -- Business Logic Flags
        -- BigQuery dayofweek: 1 = Sunday, 7 = Saturday
        case 
            when extract(dayofweek from d.date_day) in (1, 7) then true 
            else false 
        end as is_weekend,

        case 
            when h.holiday_date is not null then true 
            else false 
        end as is_illinois_holiday

    from raw_dates d
    left join illinois_holidays h 
        on d.date_day = h.holiday_date
)

select
    *,
    -- The ultimate master flag for BR-004 logic downstream
    case 
        when is_weekend = false and is_illinois_holiday = false then true
        else false
    end as is_business_day
from enriched_dates