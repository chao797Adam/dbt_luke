{% macro parse_salary(part) -%}
    {%- if part == 'min' -%}
        {# Split by any kind of dash or tilde, take the first part #}
        {%- set segment = "split(regexp_replace(job_salary, '[-~–]', '|'), '[|]')[0]" -%}
    {%- else -%}
        {# Take the last part of the split #}
        {%- set segment = "element_at(split(regexp_replace(job_salary, '[-~–]', '|'), '[|]'), -1)" -%}
    {%- endif -%}

    -- Use TRY_CAST to prevent pipeline crashes from dirty text
    try_cast(
        replace(
            regexp_extract({{ segment }}, '([0-9][0-9,.]*)', 1), ',', ''
        ) as decimal(15, 2)
    ) * case
        upper(regexp_extract({{ segment }}, '[0-9][0-9,.]*([KkMm])', 1))
        when 'K'
        then 1000
        when 'M'
        then 1000000
        else 1
    end
{%- endmacro %}
