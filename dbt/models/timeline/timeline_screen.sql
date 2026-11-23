{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['event_id'], 'unique': True},
        {'columns': ['_input']},
        {'columns': ['deleted', 'started_at']},
        {'columns': ['deleted', 'group_key']},
        {'columns': ['_stamp']},
    ],
    grants={'select': []},
    pre_hook=[
        "{{ timeline_remove_foreign(['screen']) }}",
        "{{ timeline_add_media_columns() }}",
        "{{ timeline_media_pending_index('event_id', 'not deleted') }}",
    ],
    post_hook=[
        "{{ timeline_media_settle('false', 'not deleted') }}",
        "{{ timeline_exact_acl(false) }}",
    ],
) }}
-- Screen events: one row per app session, a run of frames of the same app with
-- no pause longer than five minutes between them. Frames without an app neither
-- start nor end a session. A session is named after its first frame.
--
-- A changed frame can only move the session boundaries next to it. An
-- incremental build recomputes, for each changed second, the frames from the
-- start of the session before it to the start of the first session beginning
-- after the next frame with an app: boundaries no change can move. A session
-- that no longer exists stays as a row with deleted = true, so events drops it.
-- Frames of one second share their app and are never split between sessions. A
-- frame that moved counts as changed at its old time too. A session whose start
-- moved keeps the previous ones in previous_started_at until days has read it.
-- Internal: the reader has no access (an empty grant list makes dbt revoke).
-- The windows are found first, by a query of their own, and inlined as
-- literal ranges, so frames and sessions are read through their time indexes.
-- A session's media is its first frame's, with what the worker recorded of it;
-- a frame whose record arrives later is a changed frame of screen_frames. A
-- table holding sessions built before the media columns recomputes every
-- session once (timeline_media_pending, by its rows).
{% set gap_seconds = 300 %}
{% set upper = timeline_resume(ref('screen_frames')) %}
{% set replaced = timeline_replaced(['screen']) | length > 0 %}
{% set incremental = is_incremental() and not replaced %}
{% set consumed = timeline_consumed([('days', "source = 'screen'")]) %}
{% set ns = namespace(windows=[('-infinity', 'infinity')]) %}
{% if incremental and not timeline_media_pending('not deleted') %}
    {% set lower = timeline_resume(this) %}
    {% set reach_sql %}
        with changed as (
            select captured_at from {{ ref('screen_frames') }}
            where _stamp > {{ timeline_literal(lower) }}
            union
            select unnest(previous_captured_at) from {{ ref('screen_frames') }}
            where _stamp > {{ timeline_literal(lower) }} and previous_captured_at is not null
        ),
        reach as (
            select
                coalesce((
                    select max(s.started_at) from {{ this }} s
                    where not s.deleted and s.started_at < c.captured_at
                ), '-infinity') as lo,
                coalesce((
                    select min(s.started_at) from {{ this }} s
                    where not s.deleted and s.started_at > coalesce((
                        select min(k.captured_at) from {{ ref('screen_frames') }} k
                        where k.bundle_id is not null and k.captured_at > c.captured_at
                    ), c.captured_at)
                ), 'infinity') as hi
            from changed c
        ),
        -- Overlapping or touching reaches become one window.
        marked as (
            select lo, hi, case when lo <= max(hi) over (
                order by lo, hi rows between unbounded preceding and 1 preceding
            ) then 0 else 1 end as opens
            from reach
        ),
        windows as (
            select min(lo) as lo, max(hi) as hi
            from (select lo, hi, sum(opens) over (order by lo, hi) as window_no from marked) m
            group by window_no
        )
        select string_agg(lo::text || '|' || hi::text, ';' order by lo) from windows
    {% endset %}
    {% set found = timeline_scalar(reach_sql) %}
    {% set ns.windows = [] %}
    {% for item in (found.split(';') if found else []) %}
        {% do ns.windows.append(item.split('|')) %}
    {% endfor %}
    {% if ns.windows | length > 50 %}
        {# Windows are disjoint and sorted; one span over them is still bounded by
           boundaries no change can move. #}
        {% set ns.windows = [(ns.windows[0][0], ns.windows[-1][1])] %}
    {% endif %}
{% endif %}
with windows as (
    {% for lo, hi in ns.windows %}
    select '{{ lo }}'::timestamptz as lo, '{{ hi }}'::timestamptz as hi
    {% if not loop.last %}union all{% endif %}
    {% else %}
    select null::timestamptz as lo, null::timestamptz as hi where false
    {% endfor %}
),
window_frames as (
    select image_path, captured_at, bundle_id
    from {{ ref('screen_frames') }}
    where bundle_id is not null and ({{ timeline_in_ranges('captured_at', ns.windows) }})
),
ordered as (
    select
        w.lo as window_lo,
        k.image_path,
        k.captured_at,
        k.bundle_id,
        lag(k.captured_at) over runs as previous_at,
        lag(k.bundle_id) over runs as previous_bundle
    from window_frames k
    join windows w on k.captured_at >= w.lo and k.captured_at < w.hi
    window runs as (partition by w.lo order by k.captured_at, k.image_path)
),
numbered as (
    select *, sum(case
        when previous_at is null
            or bundle_id <> previous_bundle
            or captured_at - previous_at > interval '{{ gap_seconds }} seconds'
        then 1 else 0 end) over (
            partition by window_lo order by captured_at, image_path
        ) as session_no
    from ordered
),
sessions as (
    select
        min(bundle_id) as bundle_id,
        min(captured_at) as started_at,
        max(captured_at) as ended_at,
        count(*) as frames,
        (array_agg(image_path order by captured_at, image_path))[1] as first_frame,
        (array_agg(image_path order by captured_at desc, image_path desc))[1] as last_frame
    from numbered
    group by window_lo, session_no
),
current_sessions as (
    select
        'screen'::text as source,
        {{ timeline_connection('screen') }} as connection,
        'screen:' || s.first_frame as event_id,
        s.started_at,
        s.ended_at,
        'app_session'::text as kind,
        {{ timeline_app_name('s.bundle_id', 'k.name') }} as title,
        null::text as body,
        'screenshots'::text as media_kind,
        s.first_frame as media_key,
        f.media_status,
        f.thumbnail_w360,
        f.thumbnail_w1280,
        f.media_withheld,
        s.bundle_id as group_key,
        jsonb_build_object(
            'bundle_id', s.bundle_id, 'frames', s.frames, 'last_frame', s.last_frame
        ) as payload,
        false as deleted,
        {% if incremental %}
        {{ timeline_previous('previous_started_at', 't.started_at', 's.started_at', consumed) }}
        {% else %}null::timestamptz[]{% endif %} as previous_started_at,
        {{ timeline_literal(timeline_input('screen')) }} as _input,
        {{ timeline_literal(upper) }} as _stamp
    from sessions s
    left join {{ ref('timeline_known_apps') }} k on k.bundle_id = s.bundle_id
    left join {{ ref('screen_frames') }} f on f.image_path = s.first_frame
    -- One index probe per session: a plain join was planned as a read of them all.
    {% if incremental %}left join lateral (
        select * from {{ this }} o where o.event_id = 'screen:' || s.first_frame limit 1
    ) t on true{% endif %}
)
select * from current_sessions
{% if incremental %}
union all
select
    t.source, t.connection, t.event_id, t.started_at, t.ended_at, t.kind, t.title,
    t.body, t.media_kind, t.media_key, t.media_status, t.thumbnail_w360,
    t.thumbnail_w1280, t.media_withheld, t.group_key, t.payload,
    true as deleted,
    {{ timeline_previous('previous_started_at', 't.started_at', 't.started_at', consumed) }} as previous_started_at,
    t._input,
    {{ timeline_literal(upper) }} as _stamp
from {{ this }} t
where not t.deleted
    and ({{ timeline_in_ranges('t.started_at', ns.windows) }})
    and not exists (select 1 from current_sessions c where c.event_id = t.event_id)
{% endif %}
