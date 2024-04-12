{% macro get_partition_sql(target_cols_csv, sql, partition_field, period, start_partition, stop_partition, offset) -%}
    {{ return(adapter.dispatch('get_partition_sql')(target_cols_csv, sql, partition_field, period, start_partition, stop_partition, offset)) }}
{% endmacro %}

{% macro default__get_partition_sql(target_cols_csv, sql, partition_field, period, start_partition, stop_partition, offset) -%}

  {%- set partition_filter -%}
    ("{{partition_field}}" >  {{start_partition}} + {{offset}} * {{period}} and
     "{{partition_field}}" <= {{start_partition}} + {{offset}} * {{period}} + {{period}} and
     "{{partition_field}}" <=  {{stop_partition}})
  {%- endset -%}

  {%- set filtered_sql = sql | replace("__PERIOD_FILTER__", partition_filter) -%}

  select
    {{target_cols_csv}}
  from (
    {{filtered_sql}}
  ) target_cols

{%- endmacro %}