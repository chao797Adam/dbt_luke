{% macro unpack_extensions(array_col, keywords) %}
    {% for kw in keywords %}
        coalesce(
            array_contains({{ array_col }}, '{{ kw }}'), false
        ) as has_{{ kw | replace(' ', '_') | lower }}
        {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
{% endmacro %}
