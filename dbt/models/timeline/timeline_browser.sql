{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['event_id'], 'unique': True},
        {'columns': ['_input']},
        {'columns': ['_stamp']},
        {'columns': ['_cursor']},
    ],
    grants={'select': []},
    pre_hook=[
        {'sql': "{{ timeline_raw_indexes('browser_history', {'zen_historyvisits': ['_dlt_load_id', 'place_id'], 'zen_places': ['_dlt_load_id', 'id'], '_dlt_loads': ['load_id', 'inserted_at']}) }}", 'transaction': false},
        "{{ timeline_remove_foreign(['browser']) }}",
    ],
    post_hook="{{ timeline_exact_acl(false) }}",
) }}
-- Browser events: one row per visit to a page, keyed by the visit's id. Redirect
-- hops, reloads, embeds and framed links are not visits here; a visit of that kind,
-- or one whose page has not arrived, is a row with deleted = true, so events drops
-- it (Places reuses the ids of cleared visits, decision 032). A visit whose time
-- changed keeps the previous one in previous_started_at, for days.
-- Internal: the reader has no access (an empty grant list makes dbt revoke).
--
-- Only rows of loads dlt lists as complete are read. An incremental build reads
-- the visits of the loads completed since its cursor and the visits of pages
-- those loads changed (a new title).
{% set replaced = timeline_replaced(['browser']) | length > 0 %}
{% set cursor = timeline_load_cursor('browser_history', replaced) %}
{% set stamp = timeline_new_stamp() %}
{% set previous = is_incremental() and not replaced %}
with completed as not materialized (
    {{ timeline_completed('browser_history') }}
),
visits as not materialized (
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
{% if not cursor.rebuild %}
changed as (
    select * from visits where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
    union all
    select v.* from places p
    cross join lateral (
        select * from visits v where v.place_id = p.id and {{ timeline_completed_row('v') }}
    ) v
    where {{ timeline_in_loads('p._dlt_load_id', cursor.loads) }}
),
picked as (
    select distinct on (id) * from changed order by id
)
{% else %}
picked as (select * from visits v where {{ timeline_completed_row('v') }})
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
    {% if previous %}
    case when t.started_at <> to_timestamp(v.visit_date / 1000000.0) then t.started_at end
    {% else %}null::timestamptz{% endif %} as previous_started_at,
    {{ timeline_literal(timeline_input('browser')) }} as _input,
    nullif({{ timeline_literal(cursor.cursor) }}, '')::timestamptz as _cursor,
    {{ timeline_literal(stamp) }} as _stamp
from picked v
left join (
    select * from places p where {{ timeline_completed_row('p') }}
) p on p.id = v.place_id
{% if previous %}left join {{ this }} t on t.event_id = 'browser:' || v.id{% endif %}
