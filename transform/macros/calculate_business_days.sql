{% macro calculate_business_days(start_date, end_date) -%}

    -- Calculate business days between two dates, excluding weekends
    -- For a complete implementation, this would also exclude holidays from a date dimension
    -- This is a simplified version that only excludes weekends

    date_diff(
        {{ end_date }},
        {{ start_date }},
        day
    ) -
    -- Subtract weekends
    (7 * (
        date_diff(
            {{ end_date }},
            {{ start_date }},
            week
        )
    )) +
    case
        when dayofweek({{ start_date }}) = 1 then 1  -- Sunday
        when dayofweek({{ start_date }}) = 7 then 1  -- Saturday
        else 0
    end -
    case
        when dayofweek({{ end_date }}) = 1 then 1  -- Sunday
        when dayofweek({{ end_date }}) = 7 then 1  -- Saturday
        else 0
    end

{%- endmacro %}
