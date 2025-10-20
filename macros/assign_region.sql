{% macro assign_region(source_column, alias_name) %}
(
    case
        when {{ source_column }} in ('Pacific', 'Mountain') then 'West'
        when {{ source_column }} in ('West South Central', 'East South Central', 'South Atlantic') then 'South'
        when {{ source_column }} in ('West North Central', 'East North Central') then 'Midwest'
        when {{ source_column }} in ('Middle Atlantic', 'New England') then 'Northeast'
        else 'Unknown'
    end
) as {{ alias_name }}
{% endmacro %}
