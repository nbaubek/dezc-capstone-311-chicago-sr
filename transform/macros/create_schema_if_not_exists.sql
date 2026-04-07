{% macro create_schema_if_not_exists(database, schema) -%}
    {% if database %}
        {{ log("Creating schema " ~ database ~ "." ~ schema ~ " if it does not exist", info=True) }}
        {% call statement() -%}
            create schema if not exists {{ database }}.{{ schema }}
        {%- endcall %}
    {% else %}
        {{ log("Creating schema " ~ schema ~ " if it does not exist", info=True) }}
        {% call statement() -%}
            create schema if not exists {{ schema }}
        {%- endcall %}
    {% endif %}
{%- endmacro %}
