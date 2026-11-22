{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='image_path',
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['image_path'], 'unique': True},
        {'columns': ['_input']},
        {'columns': ['captured_at']},
        {'columns': ['_stamp']},
        {'columns': ['_cursor']},
        {'columns': ["(to_tsvector('simple', \"text\"))"], 'type': 'gin'},
    ],
    grants=timeline_grants(),
    pre_hook=[
        {'sql': "{{ timeline_raw_indexes('screenmemory', {'frames': ['_dlt_load_id', 'ts', 'image_path'], 'app_usage': ['_dlt_load_id', 'ts'], '_dlt_loads': ['load_id', 'inserted_at']}) }}", 'transaction': false},
        "{{ timeline_remove_foreign(['screen']) }}",
    ],
    post_hook=["{{ timeline_exact_acl(true) }}", "{{ timeline_schema_usage() }}"],
) }}
-- This model alone grants the reader USAGE on the schema: two GRANTs on one
-- schema at once fail.
--
-- One row per frame: its second, its app and its text. The app is the app usage
-- row of the same second (the first by name if there are several); a frame
-- without one has no app. A withheld frame keeps no text, whatever its row says.
-- Search queries must use the indexed expression to_tsvector('simple', text).
--
-- Only rows of loads dlt lists as complete are read (timeline_load_cursor). An
-- incremental build reads the frames of the loads completed since its cursor and
-- the frames of the seconds their app usage rows name: a resent frame replaces its
-- row, and a frame whose app arrives late gets it. A frame that moved keeps its
-- previous times in previous_captured_at, for the models that count by time,
-- until both have read the row (timeline_previous).
-- The index on the raw image_path also serves dlt's merge of resent frames.
{% set replaced = timeline_replaced(['screen']) | length > 0 %}
{% set cursor = timeline_load_cursor('screenmemory', replaced) %}
{% set stamp = timeline_new_stamp() %}
{% set previous = is_incremental() and not replaced %}
{% set consumed = timeline_consumed([('timeline_screen', 'true'), ('days', "source = 'screen'")]) %}
with completed as not materialized (
    {{ timeline_completed('screenmemory') }}
),
frames as not materialized (
    {{ optional_input('screenmemory', 'frames', {
        'image_path': 'text', 'ts': 'bigint', 'text': 'text', 'withheld': 'text',
        'masked': 'bigint', '_dlt_load_id': 'text'
    }, ['image_path', 'ts']) }}
),
app_usage as not materialized (
    {{ optional_input('screenmemory', 'app_usage', {
        'ts': 'bigint', 'bundle_id': 'text', '_dlt_load_id': 'text'
    }, ['ts', 'bundle_id']) }}
),
{% if not cursor.rebuild %}
new_seconds as (
    select distinct ts from app_usage where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
),
changed as (
    select * from frames where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
    union all
    select f.* from new_seconds n cross join lateral (
        select * from frames f where f.ts = n.ts and {{ timeline_completed_row('f') }}
    ) f
),
picked as (
    select distinct on (image_path) * from changed order by image_path
),
apps as (
    select p.ts, a.bundle_id
    from (select distinct ts from picked) p
    cross join lateral (
        select min(u.bundle_id) as bundle_id from app_usage u
        where u.ts = p.ts and {{ timeline_completed_row('u') }}
    ) a
)
{% else %}
picked as (select * from frames f where {{ timeline_completed_row('f') }}),
apps as (
    select ts, min(bundle_id) as bundle_id from app_usage u
    where {{ timeline_completed_row('u') }}
    group by ts
)
{% endif %}
select
    f.image_path,
    to_timestamp(f.ts) as captured_at,
    a.bundle_id,
    case when f.withheld is null then f.text end as text,
    f.withheld,
    f.masked,
    {{ timeline_connection('screen') }} as connection,
    {% if previous %}
    {{ timeline_previous('previous_captured_at', 't.captured_at', 'to_timestamp(f.ts)', consumed) }}
    {% else %}null::timestamptz[]{% endif %} as previous_captured_at,
    {{ timeline_literal(timeline_input('screen')) }} as _input,
    nullif({{ timeline_literal(cursor.cursor) }}, '')::timestamptz as _cursor,
    {{ timeline_literal(stamp) }} as _stamp
from picked f
left join apps a on a.ts = f.ts
{% if previous %}left join {{ this }} t on t.image_path = f.image_path{% endif %}
