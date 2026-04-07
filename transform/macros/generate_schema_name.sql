{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

{% macro default_schema() -%}
    {%- if target.name == 'prod' -%}
        {{ target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ target.name }}
    {%- endif -%}
{%- endmacro %}
