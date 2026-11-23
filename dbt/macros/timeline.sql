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

{# The newest stamp a model's source rows carry: where a downstream model resumes.
   Stamps are the build's clock, so they grow from build to build. #}
{% macro timeline_resume(relation, where='true') -%}
    {{- return(timeline_scalar('select max(_stamp) from ' ~ relation ~ ' where ' ~ where)) -}}
{%- endmacro %}

{% macro timeline_new_stamp() -%}
    {{- return(timeline_scalar("select to_char(clock_timestamp() at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')")) -}}
{%- endmacro %}

{# Each input's identity: its connection and raw dataset, '' when unlinked. Rows
   carry it as `_input`; rows of another identity belong to an input no longer
   linked, or replaced, and are removed before the build. #}
{% macro timeline_input(role) -%}
    {%- set sources = {'screen': 'screenmemory', 'browser': 'browser_history', 'messages': 'apple_messages'} -%}
    {%- set connection = var('miroir_connections', {}).get(role) -%}
    {%- if not connection -%}{{ return('') }}{%- endif -%}
    {{- return(connection ~ '@' ~ var('miroir_sources', {}).get(sources[role], '')) -}}
{%- endmacro %}

{% macro timeline_foreign_rows(roles, by_source=false) -%}
    {%- for role in roles -%}
        {#- Two ranges, not "is distinct from", so the index on _input answers. -#}
        ({% if by_source %}source = '{{ role }}' and {% endif %}(_input < {{ timeline_literal(timeline_input(role)) }} or _input > {{ timeline_literal(timeline_input(role)) }})){% if not loop.last %} or {% endif %}
    {%- endfor -%}
{%- endmacro %}

{# Roles whose rows in this incremental table belong to another identity. #}
{% macro timeline_replaced(roles, by_source=false) -%}
    {%- set replaced = [] -%}
    {%- if execute and is_incremental() -%}
        {%- for role in roles -%}
            {%- if run_query('select 1 from ' ~ this ~ ' where ' ~ timeline_foreign_rows([role], by_source) ~ ' limit 1').rows | length > 0 -%}
                {%- do replaced.append(role) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {{- return(replaced) -}}
{%- endmacro %}

{# Pre-hook: remove the rows of inputs no longer linked as they were. #}
{% macro timeline_remove_foreign(roles, by_source=false) -%}
    {%- if is_incremental() -%}delete from {{ this }} where {{ timeline_foreign_rows(roles, by_source) }}{%- endif -%}
{%- endmacro %}

{# Completed loads of a source: dlt lists a load once all its rows are written. #}
{% macro timeline_completed(source_name) -%}
    select load_id, inserted_at from (
        {{ optional_input(source_name, '_dlt_loads', {'load_id': 'text', 'status': 'bigint', 'inserted_at': 'timestamptz'}, ['load_id', 'status', 'inserted_at']) }}
    ) loads where status = 0
{%- endmacro %}

{# Which loads a raw-reading model reads this build. Its cursor is the latest
   completion time it has read (`_cursor` on its rows). It reads the loads that
   completed after that, less a margin, so a load completing out of order is read
   as long as the workers' clocks agree within the margin; a load read again
   inside the margin only rewrites its rows. A first build, a replaced input, or
   more new loads than one statement should list reads every completed load.
   Returns {rebuild, loads, cursor}; the load IDs are inlined in the query. #}
{% macro timeline_load_cursor(source_name, replaced) -%}
    {%- set result = {'rebuild': replaced or not is_incremental(), 'loads': [], 'cursor': ''} -%}
    {%- if not execute -%}{{ return(result) }}{%- endif -%}
    {%- set completed = '(' ~ timeline_completed(source_name) ~ ')' -%}
    {%- if not result.rebuild -%}
        {%- set lower = timeline_scalar('select max(_cursor)::text from ' ~ this) -%}
        {%- set found = timeline_scalar(
            "select string_agg(load_id, ',' order by inserted_at, load_id) || '|' || max(inserted_at)::text from (select * from "
            ~ completed ~ " l where inserted_at > coalesce(nullif(" ~ timeline_literal(lower) ~ ", '')::timestamptz, '-infinity') - interval '"
            ~ timeline_cursor_margin() ~ "' order by inserted_at, load_id limit " ~ (timeline_cursor_limit() + 1) ~ ") n") -%}
        {%- if found -%}
            {%- set parts = found.split('|') -%}
            {%- set ids = parts[0].split(',') -%}
            {%- if ids | length > timeline_cursor_limit() -%}
                {%- do result.update({'rebuild': true}) -%}
            {%- else -%}
                {%- do result.update({'loads': ids, 'cursor': parts[1]}) -%}
            {%- endif -%}
        {%- else -%}
            {%- do result.update({'cursor': lower}) -%}
        {%- endif -%}
    {%- endif -%}
    {%- if result.rebuild -%}
        {%- do result.update({'cursor': timeline_scalar('select max(inserted_at)::text from ' ~ completed ~ ' l')}) -%}
    {%- endif -%}
    {{- return(result) -}}
{%- endmacro %}

{# The variable exists for tests, which need loads outside the margin. #}
{% macro timeline_cursor_margin() %}{{ return(var('timeline_cursor_margin', '10 minutes') | replace("'", '')) }}{% endmacro %}
{% macro timeline_cursor_limit() %}{{ return(5000) }}{% endmacro %}

{# Rows of the loads read this build. #}
{% macro timeline_in_loads(column, loads) -%}
    {%- if loads -%}{{ column }} in ({% for id in loads %}{{ timeline_literal(id) }}{% if not loop.last %}, {% endif %}{% endfor %}){%- else -%}false{%- endif -%}
{%- endmacro %}

{# A raw row whose load has completed; needs the model's `completed` CTE. #}
{% macro timeline_completed_row(alias) -%}
    exists (select 1 from completed c where c.load_id = {{ alias }}._dlt_load_id)
{%- endmacro %}

{# Post-hook: leave exactly the owner and, on a readable table, the configured
   reader with SELECT, whatever default or earlier privileges said. It runs in
   the model's transaction, so no other state is ever visible. #}
{% macro timeline_exact_acl(readable) -%}
    {%- set role = timeline_reader_role() if readable else none -%}
    do $acl$
    declare grantee oid;
    begin
        for grantee in
            select distinct a.grantee from pg_class c, aclexplode(c.relacl) a
            where c.oid = '{{ this }}'::regclass and a.grantee <> c.relowner
        loop
            if grantee = 0 then
                execute 'revoke all on {{ this }} from public';
            else
                execute format('revoke all on {{ this }} from %s', grantee::regrole);
            end if;
        end loop;
        {%- if role %}
        execute 'grant select on {{ this }} to {{ role }}';
        {%- endif %}
    end
    $acl$
{%- endmacro %}

{# Indexes on raw tables that incremental builds look rows up by; dlt creates
   none but on _dlt_id. A pre-hook outside the model's transaction: each index is
   created once, when missing, and committed at once, so the lock that holds
   writes to that raw table lasts only while the index is built. An invalid one
   (an interrupted build) is rebuilt. A table the build's role does not own is
   left alone: reads stay correct, only slower. An entry is a column name, or
   (name, column, expression) for an index on an expression of that column; an
   entry whose column the table lacks is skipped. #}
{% macro timeline_raw_indexes(source_name, indexes) -%}
    {%- set statements = [] -%}
    {%- if execute and source_name not in var('miroir_unlinked_sources', []) -%}
        {%- for table_name, columns in indexes.items() -%}
            {%- set raw = source(source_name, table_name) -%}
            {%- set relation = adapter.get_relation(database=raw.database, schema=raw.schema, identifier=raw.identifier) -%}
            {%- if relation is not none -%}
                {%- set owned = run_query("select pg_has_role(current_user, relowner, 'USAGE') from pg_class where oid = '" ~ relation ~ "'::regclass").columns[0].values()[0] -%}
                {%- if not owned -%}
                    {%- do log('Not indexing ' ~ relation ~ ': owned by another role', info=true) -%}
                {%- endif -%}
                {%- set present = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list if owned else [] -%}
                {%- for entry in columns if owned -%}
                    {%- if entry is string -%}
                        {%- set suffix, column, expression = entry.strip('_'), entry, adapter.quote(entry) -%}
                    {%- else -%}
                        {%- set suffix, column, expression = entry[0], entry[1], '(' ~ entry[2] ~ ')' -%}
                    {%- endif -%}
                    {%- set name = 'miroir_' ~ raw.identifier ~ '_' ~ suffix -%}
                    {%- set qualified = adapter.quote(raw.schema) ~ '.' ~ adapter.quote(name) -%}
                    {%- if column in present -%}
                        {%- set state = run_query("select coalesce((select indisvalid::text from pg_index where indexrelid = to_regclass('" ~ qualified ~ "')), 'missing')").columns[0].values()[0] -%}
                        {%- if state == 'missing' -%}
                            {%- do statements.append('create index if not exists ' ~ adapter.quote(name) ~ ' on ' ~ relation ~ ' (' ~ expression ~ ')') -%}
                        {%- elif state == 'false' -%}
                            {%- do statements.append('reindex index ' ~ qualified) -%}
                        {%- endif -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {#- Each index commits on its own. The checks opened a transaction, which the
        last commit ends before the model's own begins. -#}
    {{- statements | join(';\ncommit;\n') ~ ';\ncommit' if statements else 'commit' -}}
{%- endmacro %}

{# How far every model that reads this one's previous positions has read it:
   the least of their newest stamps, looked up by name (no dependency between
   the models), or '' while one of them has not been built. #}
{% macro timeline_consumed(readers) -%}
    {%- set stamps = [] -%}
    {%- for identifier, where in readers -%}
        {%- set relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=identifier) if execute else none -%}
        {%- do stamps.append(timeline_scalar('select max(_stamp) from ' ~ relation ~ ' where ' ~ where) if relation is not none else '') -%}
    {%- endfor -%}
    {{- return(stamps | min if stamps else '') -}}
{%- endmacro %}

{# The times a row has left that its readers may not have recounted yet: the
   ones it carried, until a build of every reader has read the row (its stamp is
   at most `consumed`), and its old time if it moved now. A build that failed
   part way therefore loses none: they stay until they have been used. `t` is
   the row before this build; null for a new row. #}
{% macro timeline_previous(column, old, new, consumed) -%}
    nullif(
        (case when t._stamp <= {{ timeline_literal(consumed) }} then '{}'::timestamptz[]
            else coalesce(t.{{ column }}, '{}'::timestamptz[]) end)
        || (case when {{ old }} <> {{ new }} then array[{{ old }}] else '{}'::timestamptz[] end),
        '{}'::timestamptz[])
{%- endmacro %}

{# Whether a time falls in one of the ranges [lo, hi), given as text. #}
{% macro timeline_in_ranges(column, ranges) -%}
    {%- for lo, hi in ranges -%}
        ({{ column }} >= {{ timeline_literal(lo) }}::timestamptz and {{ column }} < {{ timeline_literal(hi) }}::timestamptz){% if not loop.last %} or {% endif %}
    {%- else -%}false{%- endfor -%}
{%- endmacro %}

{# The local day a time belongs to in the analysis's timezone: the day whose
   bounds hold it, a day running from (day::timestamp at time zone tz) to the
   next. Where clocks go back at midnight Postgres reads the repeated midnight as
   the later one, so the hour before it belongs to the previous day; elsewhere
   this is the local date. The server uses the same rule (src/core/timeline.py). #}
{% macro timeline_local_day(column) -%}
    {%- set tz = var('timezone') | replace("'", "''") -%}
    (case when {{ column }} < (({{ column }} at time zone '{{ tz }}')::date::timestamp at time zone '{{ tz }}')
        then ({{ column }} at time zone '{{ tz }}')::date - 1
        else ({{ column }} at time zone '{{ tz }}')::date end)
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

{# What the worker recorded of a row's stored file (media_objects, decision 040):
   its thumbnail status and which sizes exist. Null where it has no record, which
   means unknown, not absent: the file may still be queued, or not derived yet.
   media_withheld: the frame the file belongs to is withheld, which wins over any
   record (the file may have been stored before a newer rule withheld it). #}
{% macro timeline_media_columns() -%}
    {{- return({'media_status': 'text', 'thumbnail_w360': 'boolean', 'thumbnail_w1280': 'boolean', 'media_withheld': 'boolean'}) -}}
{%- endmacro %}

{# Whether an incremental table already has the media columns. A table built by
   code before them lacks them: its rows predate the join and are rebuilt once. #}
{% macro timeline_has_media_columns() -%}
    {%- if not (execute and is_incremental()) -%}{{ return(true) }}{%- endif -%}
    {%- set names = adapter.get_columns_in_relation(this) | map(attribute='name') | list -%}
    {{- return(timeline_media_columns().keys() | reject('in', names) | list | length == 0) -}}
{%- endmacro %}

{# Pre-hook: add the media columns to a table built before them. Nullable with no
   default, so the change touches no row. dbt's on_schema_change is not used: it
   would also add the `_deleted` column of the new rows to the table. #}
{% macro timeline_add_media_columns() -%}
    {%- if is_incremental() -%}
    alter table {{ this }}
    {%- for name, type in timeline_media_columns().items() %}
        add column if not exists {{ adapter.quote(name) }} {{ type }}{% if not loop.last %},{% endif %}
    {%- endfor %}
    {%- endif -%}
{%- endmacro %}

{# A conversation's opaque id: the first 128 bits of SHA-256 over the package's
   own random key (timeline_secret) and the conversation key, in hex. Stable for
   as long as the key and the conversation key are; never derivable from a
   phone number without the key, which only the owner may read. #}
{% macro timeline_conversation_id(key) -%}
    encode(substr(sha256((select s.secret from {{ ref('timeline_secret') }} s limit 1)
        || convert_to('conversation:' || {{ key }}, 'UTF8')), 1, 16), 'hex')
{%- endmacro %}

{# A time as fixed ISO 8601 text in UTC, for JSON: never the session's zone. #}
{% macro timeline_utc_text(column) -%}
    to_char({{ column }} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
{%- endmacro %}

{# As timeline_previous, for a time that can be null (an undated message): a
   time left for no time, or for another, is kept; no time is never kept. #}
{% macro timeline_previous_moved(column, old, new, consumed) -%}
    nullif(
        (case when t._stamp <= {{ timeline_literal(consumed) }} then '{}'::timestamptz[]
            else coalesce(t.{{ column }}, '{}'::timestamptz[]) end)
        || (case when {{ old }} is not null and {{ old }} is distinct from {{ new }}
            then array[{{ old }}] else '{}'::timestamptz[] end),
        '{}'::timestamptz[])
{%- endmacro %}

{# Pre-hook of messages: after a change of the analysis's timezone, give every
   message the local day of the new one, in place (an index range, nothing when
   none differ). Reading every message again would cost far more. #}
{% macro timeline_messages_retime() -%}
    {%- if is_incremental() -%}
    {%- set tz = timeline_literal(var('timezone')) -%}
    update {{ this }} set day = {{ timeline_local_day('sent_at') }}, timezone = {{ tz }}
    where timezone < {{ tz }} or timezone > {{ tz }}
    {%- endif -%}
{%- endmacro %}
