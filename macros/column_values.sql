-- macros/get_column_values_from_query.sql
{% macro get_column_values_from_query(query) -%}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {% set column_values_sql %}
    with cte as (
        {{ query }}
    )
    select
        *
    from cte
    {% endset %}

    {%- set results = run_query(column_values_sql) %}
    {% set results_list = results.rows %}
    {{ return(results_list) }}

{%- endmacro %}