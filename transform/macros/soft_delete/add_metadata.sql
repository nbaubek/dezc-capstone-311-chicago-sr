-- Add metadata columns for soft delete pattern
-- Use with models that need to support SCD Type 2 soft deletes

{% macro add_metadata(model_name) %}

    {{ model_name }}_with_metadata as (
        select
            *,
            'current' as dbt_record_status,
            current_timestamp() as dbt_valid_from,
            null as dbt_valid_to,
            '{{ dbt_current_timestamp() }}' as dbt_updated_at
        from {{ model_name }}
    )

{% endmacro %}
