{# Media's macros (decision 033; package split, step 11): the only way a model
   joins `media_objects`, the records the worker writes of the files rows point
   at. A record is what is known of a stored file: its thumbnail status and the
   sizes made. No record means unknown, not absent: the file may still be
   queued, or not derived yet.

   media_ref(role, kind, key_column) describes the files of one media kind that
   the input linked as `role` points at, through `key_column`, and returns SQL
   pieces a model puts where it needs them, so each model keeps its own query
   shape:

   - kind: the kind, as a literal for a `media_kind` column.
   - records: this connection's records of that kind, for a CTE (`media`); the
     dataset could hold another connection's. `_dlt_load_id` is kept, so the
     model reads only records of completed loads.
   - record: a lateral subquery, the first record of `key_column`'s file among
     `records` (named `media`) whose load completed, needing the model's
     `completed` CTE (timeline_completed_row). Its columns are a record's:
     thumbnail_status (a row's media_status), thumbnail_w360, thumbnail_w1280.

   The kind must be one the role's connector declares (a catalog test reads
   every call). #}
{% macro media_ref(role, kind, key_column) -%}
    {%- if not modules.re.fullmatch('[a-z][a-z0-9_]*', kind) -%}
        {{ exceptions.raise_compiler_error('Invalid media kind ' ~ kind) }}
    {%- endif -%}
    {%- set source_name = var('miroir_roles', {}).get(role) -%}
    {%- if not source_name and execute -%}
        {{ exceptions.raise_compiler_error('No input role ' ~ role ~ ' for media') }}
    {%- elif not source_name -%}
        {#- Parsed for another package's build, which gives no roles. -#}
        {{- return({'kind': "'" ~ kind ~ "'::text", 'records': 'select null where false',
            'record': '(select null)'}) -}}
    {%- endif -%}
    {%- set records -%}
    select * from (
        {{ optional_input(source_name, 'media_objects', {
            'connection_id': 'text', 'kind': 'text', 'key': 'text',
            'thumbnail_status': 'text', 'thumbnail_w360': 'boolean',
            'thumbnail_w1280': 'boolean', '_dlt_load_id': 'text'
        }, ['connection_id', 'kind', 'key', 'thumbnail_status']) }}
    ) m
    where m.connection_id = {{ timeline_connection(role) }} and m.kind = '{{ kind }}'
    {%- endset -%}
    {%- set record -%}
    (select * from media m where m.key = {{ key_column }} and {{ timeline_completed_row('m') }} limit 1)
    {%- endset -%}
    {{- return({'kind': "'" ~ kind ~ "'::text", 'records': records, 'record': record}) -}}
{%- endmacro %}
