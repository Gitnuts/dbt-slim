{% macro get_partition_boundaries(target_schema, target_table, partition_field, start_partition, stop_condition, period) -%}
    {{ return(adapter.dispatch('get_partition_boundaries')(target_schema, target_table, partition_field, start_partition, stop_condition, period)) }}
{% endmacro %}

{% macro default__get_partition_boundaries(target_schema, target_table, partition_field, start_partition, stop_condition, period) -%}

  {% set condition = stop_condition %}
  {% call statement('partition_boundaries', fetch_result=True) -%}
    with data as (
      select
          coalesce(max("{{partition_field}}"), {{start_partition}}) as start_partition,
          {{ get_column_values_from_query(condition)[0][0] }} as stop_partition
      from "{{target_schema}}"."{{target_table}}"
    )

    select
      start_partition,
      stop_partition,
      (stop_partition - start_partition) / {{period}} + 1 as num_partitions
    from data
  {%- endcall %}

{%- endmacro %}