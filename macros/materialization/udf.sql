{% materialization udf, adapter="postgres" %}
{%- set target_relation = api.Relation.create(
        identifier=this.identifier, schema=schema, database=database) -%}

{%- set parameter_list=config.get('parameter_list') -%}
{%- set ret=config.get('returns') -%}

{%- set create_sql -%}
CREATE OR REPLACE FUNCTION {{ schema }}.{{ this.identifier }}({{ parameter_list }})
RETURNS {{ ret }}
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
  {{ sql }};
END;
$$ LANGUAGE plpgsql;
{%- endset -%}

{% call statement('main') -%}
  {{ create_sql }}
{%- endcall %}

{% do adapter.commit() %}

{{ return({'relations': []}) }}

{% endmaterialization %}