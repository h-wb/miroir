{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='image_path',
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['image_path'], 'unique': True},
        {'columns': ['captured_at']},
        {'columns': ['_loaded_through']},
        {'columns': ["(to_tsvector('simple', \"text\"))"], 'type': 'gin'},
    ],
    grants=timeline_grants(),
    pre_hook="{{ timeline_raw_indexes('screenmemory', {'frames': ['_dlt_load_id', 'ts', 'image_path'], 'app_usage': ['_dlt_load_id', 'ts'], '_dlt_loads': ['load_id']}) }}",
    post_hook="{{ timeline_schema_usage() }}",
) }}
-- This model alone grants the reader USAGE on the schema: two GRANTs on one
-- schema at once fail.
--
-- One row per frame: its second, its app and its text. The app is the app usage
-- row of the same second (the first by name if there are several); a frame
-- without one has no app. Search queries must use the indexed expression
-- to_tsvector('simple', text).
--
-- An incremental build reads the frames of loads completed since the last build,
-- and the frames of the seconds that newly loaded app usage rows name: a resent
-- frame replaces its row, and a frame whose app arrives late gets it. The index
-- on the raw image_path also serves dlt's merge of resent frames.
{% set upper = timeline_loaded_through('screenmemory') %}
{% set lower = timeline_resume(this) if is_incremental() else '' %}
with frames as not materialized (
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
{% if is_incremental() %}
new_seconds as (
    select distinct ts from app_usage
    where _dlt_load_id > {{ timeline_literal(lower) }} and _dlt_load_id <= {{ timeline_literal(upper) }}
),
changed as (
    select * from frames
    where _dlt_load_id > {{ timeline_literal(lower) }} and _dlt_load_id <= {{ timeline_literal(upper) }}
    union all
    select f.* from new_seconds n cross join lateral (
        select * from frames f where f.ts = n.ts
    ) f
),
picked as (
    select distinct on (image_path) * from changed order by image_path
),
apps as (
    select p.ts, a.bundle_id
    from (select distinct ts from picked) p
    cross join lateral (
        select min(u.bundle_id) as bundle_id from app_usage u where u.ts = p.ts
    ) a
)
{% else %}
picked as (select * from frames),
apps as (select ts, min(bundle_id) as bundle_id from app_usage group by ts)
{% endif %}
select
    f.image_path,
    to_timestamp(f.ts) as captured_at,
    a.bundle_id,
    f.text,
    f.withheld,
    f.masked,
    {{ timeline_connection('screen') }} as connection,
    {{ timeline_literal(upper) }} as _loaded_through
from picked f
left join apps a on a.ts = f.ts
