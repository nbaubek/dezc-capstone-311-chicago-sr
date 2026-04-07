-- Intermediate model for SLA performance calculations
-- Joins staging data with SLA targets and calculates breach status

{{ config(
    materialized='table',
    tags=['intermediate']
) }}

with stg_requests as (
    select * from {{ ref('stg_service_requests') }}
),

sla_targets as (
    select * from {{ ref('sla_targets_seed') }}
),

with_sla_targets as (
    select
        stg.*,
        coalesce(sla.target_days, 14) as sla_target_days,
        coalesce(sla.category, 'Uncategorized') as sla_category,
        case
            when sla.target_days is null then 'default'
            else 'configured'
        end as sla_target_source
    from stg_requests stg
    left join sla_targets sla
        on stg.sr_type = trim(sla.sr_type)
),

with_breach_status as (
    select
        *,
        case
            when resolution_days is not null and resolution_days > sla_target_days then true
            else false
        end as is_sla_breach,
        case
            when resolution_days is not null
                then least(resolution_days - sla_target_days, 0)  -- Negative = under SLA
            else null
        end as days_from_sla_target,
        -- For open requests, flag if at risk of breach
        case
            when days_open is not null and days_open > sla_target_days then true
            else false
        end as is_sla_at_risk
    from with_sla_targets
)

select * from with_breach_status
