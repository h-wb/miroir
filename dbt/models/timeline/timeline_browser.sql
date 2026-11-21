{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['event_id'], 'unique': True},
        {'columns': ['_loaded_through']},
    ],
    pre_hook="{{ timeline_raw_indexes('browser_history', {'zen_historyvisits': ['_dlt_load_id', 'place_id'], 'zen_places': ['_dlt_load_id', 'id'], '_dlt_loads': ['load_id']}) }}",
) }}
-- Browser events: one row per visit to a page, keyed by the visit's id. Redirect
-- hops, reloads, embeds and framed links are not visits here; a visit of that kind,
-- or one whose page has not arrived, is a row with deleted = true, so events drops
-- it (Places reuses the ids of cleared visits, decision 032).
--
-- An incremental build reads the visits of loads completed since the last build
-- and the visits of pages loaded since then (a changed title).
{% set upper = timeline_loaded_through('browser_history') %}
{% set lower = timeline_resume(this) if is_incremental() else '' %}
with visits as not materialized (
    {{ optional_input('browser_history', 'zen_historyvisits', {
        'id': 'bigint', 'visit_date': 'bigint', 'from_visit': 'bigint', 'place_id': 'bigint',
        'visit_type': 'bigint', '_dlt_load_id': 'text'
    }, ['id', 'place_id', 'visit_date']) }}
),
places as not materialized (
    {{ optional_input('browser_history', 'zen_places', {
        'id': 'bigint', 'url': 'text', 'title': 'text', '_dlt_load_id': 'text'
    }, ['id', 'url']) }}
),
{% if is_incremental() %}
changed as (
    select * from visits
    where _dlt_load_id > {{ timeline_literal(lower) }} and _dlt_load_id <= {{ timeline_literal(upper) }}
    union all
    select v.* from places p
    cross join lateral (select * from visits v where v.place_id = p.id) v
    where p._dlt_load_id > {{ timeline_literal(lower) }} and p._dlt_load_id <= {{ timeline_literal(upper) }}
),
picked as (
    select distinct on (id) * from changed order by id
)
{% else %}
picked as (select * from visits)
{% endif %}
select
    'browser'::text as source,
    {{ timeline_connection('browser') }} as connection,
    'browser:' || v.id as event_id,
    to_timestamp(v.visit_date / 1000000.0) as started_at,
    to_timestamp(v.visit_date / 1000000.0) as ended_at,
    'visit'::text as kind,
    coalesce(nullif(p.title, ''), p.url) as title,
    p.url as body,
    null::text as media_kind,
    null::text as media_key,
    nullif(regexp_replace(lower(substring(
        p.url from '^[A-Za-z][A-Za-z0-9+.-]*://(?:[^/?#@]*@)?([^/?#:]+)'
    )), '^www\.', ''), '') as group_key,
    jsonb_build_object(
        'visit_id', v.id, 'place_id', v.place_id, 'from_visit_id', v.from_visit,
        'visit_type', v.visit_type
    ) as payload,
    -- 4 embed, 5 and 6 redirects, 8 framed link, 9 reload
    (p.id is null or v.visit_date is null or coalesce(v.visit_type in (4, 5, 6, 8, 9), false)) as deleted,
    {{ timeline_literal(upper) }} as _loaded_through
from picked v
left join places p on p.id = v.place_id
