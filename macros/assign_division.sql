{% macro assign_division(source_column, alias_name) %}
(
    case
        when {{ source_column }} in ('AK', 'CA', 'HI', 'OR', 'WA') then 'Pacific'
        when {{ source_column }} in ('AZ', 'CO', 'ID', 'MT', 'NV', 'NM', 'UT', 'WY') then 'Mountain'
        when {{ source_column }} in ('AR', 'LA', 'OK', 'TX') then 'West South Central'
        when {{ source_column }} in ('AL', 'KY', 'MS', 'TN') then 'East South Central'
        when {{ source_column }} in ('DE', 'FL', 'GA', 'MD', 'NC','SC', 'VA', 'WA', 'WV', 'DC') then 'South Atlantic'
        when {{ source_column }} in ('IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD') then 'West North Central'
        when {{ source_column }} in ('IL', 'IN', 'MI', 'OH', 'WI') then 'East North Central'
        when {{ source_column }} in ('NJ', 'NY', 'PA') then 'Middle Atlantic'
        when {{ source_column }} in ('CT', 'ME', 'MA', 'NH', 'RI', 'VT') then 'New England'
        else 'Unknown'
    end
) as {{ alias_name }}
{% endmacro %}
