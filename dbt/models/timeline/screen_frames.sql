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
        {'columns': ['(' ~ timeline_search_vector('"text"') ~ ')'], 'type': 'gin'},
    ],
    grants=timeline_grants(),
    pre_hook=[
        {'sql': "{{ timeline_raw_indexes('screenmemory', {'frames': ['_dlt_load_id', 'ts', 'image_path'], 'app_usage': ['_dlt_load_id', 'ts'], 'media_objects': ['_dlt_load_id', 'key'], '_dlt_loads': ['load_id', 'inserted_at']}) }}", 'transaction': false},
        "{{ timeline_remove_foreign(['screen']) }}",
        "{{ timeline_add_media_columns() }}",
        "{{ timeline_media_pending_index('image_path') }}",
        "{{ timeline_search_index('text') }}",
    ],
    post_hook=[
        "{{ timeline_media_settle('withheld is not null') }}",
        "{{ timeline_exact_acl(true) }}",
        "{{ timeline_schema_usage() }}",
    ],
) }}
-- This model alone grants the reader USAGE on the schema: two GRANTs on one
-- schema at once fail.
--
-- One row per frame: its second, its app and its text. The app is the app usage
-- row of the same second (the first by name if there are several); a frame
-- without one has no app. A withheld frame keeps no text, whatever its row says.
-- Search queries must use the indexed expression, timeline_search_vector('text').
--
-- Only rows of loads dlt lists as complete are read (timeline_load_cursor). An
-- incremental build reads the frames of the loads completed since its cursor and
-- the frames of the seconds their app usage rows name: a resent frame replaces its
-- row, and a frame whose app arrives late gets it. A frame that moved keeps its
-- previous times in previous_captured_at, for the models that count by time,
-- until both have read the row (timeline_previous).
-- The index on the raw image_path also serves dlt's merge of resent frames.
--
-- What the worker recorded of each frame's screenshot comes from media_objects in
-- the same dataset, when it exists (timeline_media_columns; no record is unknown).
-- Its loads are listed in the same _dlt_loads, so an incremental build also reads
-- again the frames whose record a newly completed load wrote. A table holding
-- rows built before these columns reads every frame once (timeline_media_pending:
-- by its rows, not its columns, so a failed first attempt is not forgotten).
{% set replaced = timeline_replaced(['screen']) | length > 0 %}
{% set cursor = timeline_load_cursor('screenmemory', replaced or timeline_media_pending()) %}
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
-- This connection's screenshots only; the dataset could hold another's.
media as not materialized (
    select * from (
        {{ optional_input('screenmemory', 'media_objects', {
            'connection_id': 'text', 'kind': 'text', 'key': 'text',
            'thumbnail_status': 'text', 'thumbnail_w360': 'boolean',
            'thumbnail_w1280': 'boolean', '_dlt_load_id': 'text'
        }, ['connection_id', 'kind', 'key', 'thumbnail_status']) }}
    ) m
    where m.connection_id = {{ timeline_connection('screen') }} and m.kind = 'screenshots'
),
{% if not cursor.rebuild %}
new_seconds as (
    select distinct ts from app_usage where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
),
new_media as (
    select distinct key from media where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
),
changed as (
    select * from frames where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
    union all
    select f.* from new_seconds n cross join lateral (
        select * from frames f where f.ts = n.ts and {{ timeline_completed_row('f') }}
    ) f
    union all
    select f.* from new_media n cross join lateral (
        select * from frames f where f.image_path = n.key and {{ timeline_completed_row('f') }}
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
),
stored as (
    select p.image_path as key, m.thumbnail_status, m.thumbnail_w360, m.thumbnail_w1280
    from picked p
    cross join lateral (
        select * from media m
        where m.key = p.image_path and {{ timeline_completed_row('m') }}
        limit 1
    ) m
)
{% else %}
picked as (select * from frames f where {{ timeline_completed_row('f') }}),
apps as (
    select ts, min(bundle_id) as bundle_id from app_usage u
    where {{ timeline_completed_row('u') }}
    group by ts
),
stored as (
    select distinct on (key) key, thumbnail_status, thumbnail_w360, thumbnail_w1280
    from media m where {{ timeline_completed_row('m') }}
    order by key
)
{% endif %}
select
    f.image_path,
    to_timestamp(f.ts) as captured_at,
    a.bundle_id,
    case when f.withheld is null then f.text end as text,
    f.withheld,
    f.masked,
    m.thumbnail_status as media_status,
    m.thumbnail_w360,
    m.thumbnail_w1280,
    f.withheld is not null as media_withheld,
    {{ timeline_connection('screen') }} as connection,
    {% if previous %}
    {{ timeline_previous('previous_captured_at', 't.captured_at', 'to_timestamp(f.ts)', consumed) }}
    {% else %}null::timestamptz[]{% endif %} as previous_captured_at,
    {{ timeline_literal(timeline_input('screen')) }} as _input,
    nullif({{ timeline_literal(cursor.cursor) }}, '')::timestamptz as _cursor,
    {{ timeline_literal(stamp) }} as _stamp
from picked f
left join apps a on a.ts = f.ts
left join stored m on m.key = f.image_path
{% if previous %}left join {{ this }} t on t.image_path = f.image_path{% endif %}
