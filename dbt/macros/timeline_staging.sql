{# The timeline's incremental models (strategies timeline_upsert and
   timeline_replace) stage their new
   rows in an unlogged table of their own schema, not in the temporary table dbt
   uses for an incremental build. A build that reads everything again (an
   upgrade, a replaced input, more loads than one statement lists) stages every
   row; in a temporary table that spends local buffers (temp_buffers, 8 MB by
   default), and PostgreSQL 18 then fails with "no empty local buffer available"
   where 17 did not (decision 040). An unlogged table uses shared buffers and
   spills like any table. It is created in the model's transaction and dropped
   by the upsert, so a failed build leaves none behind. Every other model keeps
   dbt's own behaviour. #}

{% macro timeline_staged() -%}
    {{- return(config.get('incremental_strategy') in ('timeline_upsert', 'timeline_replace')) -}}
{%- endmacro %}

{% macro postgres__make_temp_relation(base_relation, suffix) %}
    {% set relation = postgres__make_relation_with_suffix(base_relation, suffix, dstring=True) %}
    {% if timeline_staged() %}
        {{ return(relation) }}
    {% endif %}
    {{ return(relation.incorporate(path={"schema": none, "database": none})) }}
{% endmacro %}

{# dbt-postgres 1.11's postgres__create_table_as, with the staged case first. #}
{% macro postgres__create_table_as(temporary, relation, sql) -%}
  {%- if temporary and timeline_staged() -%}
  create unlogged table {{ relation }} as (
    {{ sql }}
  );
  {%- else -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}
    temporary
  {%- elif unlogged -%}
    unlogged
  {%- endif %} table {{ relation }}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {% endif -%}
  {% if contract_config.enforced and (not temporary) -%}
      {{ get_table_columns_and_constraints() }} ;
    insert into {{ relation }} (
      {{ adapter.dispatch('get_column_names', 'dbt')() }}
    )
    {%- set sql = get_select_subquery(sql) %}
  {% else %}
    as
  {% endif %}
  (
    {{ sql }}
  );
  {%- endif -%}
{%- endmacro %}
