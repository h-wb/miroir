{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key=['source', 'day'],
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['source', 'day'], 'unique': True},
        {'columns': ['source', '_input']},
    ],
    grants=timeline_grants(),
    pre_hook="{{ timeline_remove_foreign(['screen', 'browser'], by_source=true) }}",
    post_hook=[
        "delete from {{ this }} where timezone <> '{{ var('timezone') | replace(\"'\", \"''\") }}'",
        "{{ timeline_exact_acl(true) }}",
    ],
) }}
-- depends_on: {{ ref('timeline_screen') }}, {{ ref('timeline_browser') }}
-- Per local day (in the analysis's timezone) and source: how many events started
-- that day, and for the screen how many frames were captured. A day with neither
-- has no row.
--
-- An incremental build recounts only the days of rows written since its last
-- build, removed sessions and visits included, and the days a moved frame,
-- session or visit left. A changed timezone or a source whose input was unlinked
-- or replaced recounts every day once: the pre-hook removes that source's days,
-- the post-hook the days counted in the old timezone.
--
-- Known limit: where the clocks go back at midnight, a day starts at the later
-- of the two midnights, so the hour before it counts on the previous day.
{% set tz = var('timezone') | replace("'", "''") %}
{% set ns = namespace(rebuild=not is_incremental()) %}
{% if is_incremental() and execute %}
    {% set other = run_query("select 1 from " ~ this ~ " where timezone <> '" ~ tz ~ "' limit 1") %}
    {% set ns.rebuild = other.rows | length > 0
        or timeline_replaced(['screen', 'browser'], by_source=true) | length > 0 %}
{% endif %}
{% set screen_upper = timeline_resume(ref('screen_frames')) %}
{% set browser_upper = timeline_resume(ref('timeline_browser')) %}
{% if not ns.rebuild %}
    {% set screen_lower = timeline_literal(timeline_resume(this, "source = 'screen'")) %}
    {% set browser_lower = timeline_literal(timeline_resume(this, "source = 'browser'")) %}
{% endif %}
with affected as (
    {% if ns.rebuild %}
    select distinct 'screen'::text as source, {{ timeline_local_day('captured_at') }} as day
    from {{ ref('screen_frames') }}
    union
    select distinct source, {{ timeline_local_day('started_at') }} from {{ ref('events') }}
    {% else %}
    select 'screen'::text as source, {{ timeline_local_day('moved.at') }} as day
    from {{ ref('screen_frames') }} f
    cross join lateral (select f.captured_at union all select unnest(f.previous_captured_at)) moved (at)
    where f._stamp > {{ screen_lower }} and moved.at is not null
    union
    select 'screen', {{ timeline_local_day('moved.at') }}
    from {{ ref('timeline_screen') }} s
    cross join lateral (select s.started_at union all select unnest(s.previous_started_at)) moved (at)
    where s._stamp > {{ screen_lower }} and moved.at is not null
    union
    select 'browser', {{ timeline_local_day('moved.at') }}
    from {{ ref('timeline_browser') }} v
    cross join lateral (select v.started_at union all select unnest(v.previous_started_at)) moved (at)
    where v._stamp > {{ browser_lower }} and moved.at is not null
    {% endif %}
),
spans as (
    select
        source,
        day,
        (day::timestamp at time zone '{{ tz }}') as day_start,
        ((day + 1)::timestamp at time zone '{{ tz }}') as day_end
    from affected
),
counted as (
    select
        d.source,
        d.day,
        (select count(*) from {{ ref('events') }} e
         where e.source = d.source and e.started_at >= d.day_start and e.started_at < d.day_end) as events,
        case when d.source = 'screen' then (
            select count(*) from {{ ref('screen_frames') }} f
            where f.captured_at >= d.day_start and f.captured_at < d.day_end
        ) end as frames,
        '{{ tz }}'::text as timezone,
        case d.source
            when 'screen' then {{ timeline_literal(timeline_input('screen')) }}
            else {{ timeline_literal(timeline_input('browser')) }}
        end as _input,
        case d.source
            when 'screen' then {{ timeline_literal(screen_upper) }}
            else {{ timeline_literal(browser_upper) }}
        end as _stamp
    from spans d
)
select *{% if is_incremental() %}, (events = 0 and coalesce(frames, 0) = 0) as _deleted{% endif %}
from counted
{% if not is_incremental() %}where events > 0 or coalesce(frames, 0) > 0{% endif %}
