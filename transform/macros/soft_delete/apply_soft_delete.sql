-- Apply soft delete pattern to a model
-- Updates existing records with dbt_valid_to timestamp when they no longer appear in source

{% macro apply_soft_delete(source_relation, target_relation, key_columns) %}

    {% set key_condition = key_columns
        | map('dbt_utils.render_column')
        | map('replace', '.', '__')
        | join(' = ') %}

    merge into {{ target_relation }} as target
    using {{ source_relation }} as source
    on {{ key_condition }}

    -- Mark records as deleted if they no longer exist in source
    when matched and target.dbt_record_status = 'current'
        and source.source_key is null then
        update set
            dbt_record_status = 'deleted',
            dbt_valid_to = current_timestamp(),
            dbt_updated_at = current_timestamp()

{% endmacro %}
