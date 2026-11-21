{# Shared pieces of the timeline package (decision 040). #}

{# The server's read-only role, or none. The catalogue validates it; this check
   keeps an unquoted grantee from ever reaching GRANT unvalidated. #}
{% macro timeline_reader_role() -%}
    {%- set role = var('timeline_reader_role', none) -%}
    {%- if role and not modules.re.fullmatch('[a-z_][a-z0-9_]{0,62}', role) -%}
        {{ exceptions.raise_compiler_error('Invalid timeline_reader_role') }}
    {%- endif -%}
    {{- return(role or none) -}}
{%- endmacro %}

{# SELECT for the reader role. An empty list (not an empty dict) still makes dbt
   revoke a role that was granted before and is no longer set. #}
{% macro timeline_grants() -%}
    {%- set role = timeline_reader_role() -%}
    {{- return({'select': [role] if role else []}) -}}
{%- endmacro %}

{# dbt grants cover tables only; the reader also needs USAGE on the schema.
   One model grants it: two GRANTs on one schema at once fail. #}
{% macro timeline_schema_usage() -%}
    {%- set role = timeline_reader_role() -%}
    {%- if role -%}grant usage on schema {{ adapter.quote(this.schema) }} to {{ role }}{%- endif -%}
{%- endmacro %}

{# The connection linked to a role, as a SQL literal. #}
{% macro timeline_connection(role) -%}
    {%- set id = var('miroir_connections', {}).get(role) -%}
    {%- if id -%}'{{ id | replace("'", "''") }}'::text{%- else -%}null::text{%- endif -%}
{%- endmacro %}

{# The first value a query returns, as text ('' for none), read when the model
   runs. Watermarks are inlined as literals so the planner estimates them from
   the index and reads only the new rows. #}
{% macro timeline_scalar(sql) -%}
    {%- if not execute -%}{{ return('') }}{%- endif -%}
    {%- set value = run_query(sql).columns[0].values()[0] -%}
    {{- return('' if value is none else value | string) -}}
{%- endmacro %}

{% macro timeline_literal(value) -%}
    '{{ value | replace("'", "''") }}'::text
{%- endmacro %}

{# The newest load of a source that dlt lists as complete. A load's rows are all
   visible once it is listed in _dlt_loads, so every row of a load up to this one
   can be read; rows of a load still running wait for the next build. #}
{% macro timeline_loaded_through(source_name) -%}
    {%- set sql -%}
        select max(load_id) from (
            {{ optional_input(source_name, '_dlt_loads', {'load_id': 'text', 'status': 'bigint'}, ['load_id', 'status']) }}
        ) loads where status = 0
    {%- endset -%}
    {{- return(timeline_scalar(sql)) -}}
{%- endmacro %}

{# The newest load a model's own rows were built through: where it resumes. #}
{% macro timeline_resume(relation, where='true') -%}
    {{- return(timeline_scalar('select max(_loaded_through) from ' ~ relation ~ ' where ' ~ where)) -}}
{%- endmacro %}

{# Indexes on raw tables that incremental builds look rows up by; dlt creates
   none but on _dlt_id. Created once, when missing, by a pre-hook. #}
{% macro timeline_raw_indexes(source_name, indexes) -%}
    {%- set statements = [] -%}
    {%- if execute and source_name not in var('miroir_unlinked_sources', []) -%}
        {%- for table_name, columns in indexes.items() -%}
            {%- set raw = source(source_name, table_name) -%}
            {%- set relation = adapter.get_relation(database=raw.database, schema=raw.schema, identifier=raw.identifier) -%}
            {%- if relation is not none -%}
                {%- for column in columns -%}
                    {%- set name = 'miroir_' ~ raw.identifier ~ '_' ~ column.strip('_') -%}
                    {%- set found = run_query("select to_regclass('" ~ adapter.quote(raw.schema) ~ "." ~ adapter.quote(name) ~ "') is not null") -%}
                    {%- if not found.columns[0].values()[0] -%}
                        {%- do statements.append('create index if not exists ' ~ adapter.quote(name) ~ ' on ' ~ relation ~ ' (' ~ adapter.quote(column) ~ ')') -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {{- statements | join(';\n') -}}
{%- endmacro %}

{# Whether a time falls in one of the ranges [lo, hi), given as text. #}
{% macro timeline_in_ranges(column, ranges) -%}
    {%- for lo, hi in ranges -%}
        ({{ column }} >= {{ timeline_literal(lo) }}::timestamptz and {{ column }} < {{ timeline_literal(hi) }}::timestamptz){% if not loop.last %} or {% endif %}
    {%- else -%}false{%- endfor -%}
{%- endmacro %}

{# A time's local date in the analysis's timezone. #}
{% macro timeline_local_day(column) -%}
    ({{ column }} at time zone '{{ var('timezone') | replace("'", "''") }}')::date
{%- endmacro %}

{# Readable name and colour for an app's bundle identifier, for bundles the
   known list lacks: the last part of the identifier, and a colour picked from
   a fixed palette by a stable hash. #}
{% macro timeline_app_name(bundle, known_name) -%}
    coalesce({{ known_name }}, nullif(initcap(regexp_replace(regexp_replace({{ bundle }}, '^.*\.', ''), '[-_]+', ' ', 'g')), ''), {{ bundle }})
{%- endmacro %}

{% macro timeline_app_colour(bundle, known_colour) -%}
    coalesce({{ known_colour }}, (array['#4e79a7', '#f28e2b', '#e15759', '#76b7b2', '#59a14f',
        '#edc948', '#b07aa1', '#ff9da7', '#9c755f', '#bab0ac'])[
        ('x' || substr(md5({{ bundle }}), 1, 7))::bit(28)::int % 10 + 1])
{%- endmacro %}

{# Incremental strategy: new rows replace every row with their key. Where the new
   rows have `_deleted` (a column of theirs, not of the table), those that are
   true only remove. The new rows are analysed first so the delete goes through
   the key's index rather than reading the table. #}
{% macro get_incremental_timeline_upsert_sql(arg_dict) -%}
    {%- set target = arg_dict['target_relation'] -%}
    {%- set source = arg_dict['temp_relation'] -%}
    {%- set keys = arg_dict['unique_key'] if arg_dict['unique_key'] is not string else [arg_dict['unique_key']] -%}
    {%- set columns = get_quoted_csv(arg_dict['dest_columns'] | map(attribute='name')) -%}
    {%- set removes = '_deleted' in (adapter.get_columns_in_relation(source) | map(attribute='name') | list) -%}
    analyze {{ source }};
    delete from {{ target }} as t using {{ source }} as s
    where {% for key in keys %}t.{{ adapter.quote(key) }} = s.{{ adapter.quote(key) }}{% if not loop.last %} and {% endif %}{% endfor %};
    insert into {{ target }} ({{ columns }})
    select {{ columns }} from {{ source }}{% if removes %} where not _deleted{% endif %}
{%- endmacro %}
