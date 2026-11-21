{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key=['source', 'day'],
    incremental_strategy='timeline_upsert',
    indexes=[{'columns': ['source', 'day'], 'unique': True}],
    grants=timeline_grants(),
    post_hook="delete from {{ this }} where timezone <> '{{ var('timezone') | replace(\"'\", \"''\") }}'",
) }}
-- depends_on: {{ ref('timeline_screen') }}, {{ ref('timeline_browser') }}
-- Per local day (in the analysis's timezone) and source: how many events started
-- that day, and for the screen how many frames were captured. A day with neither
-- has no row.
--
-- An incremental build recounts only the days of rows written since its last
-- build, removed sessions and visits included. A changed timezone recounts every
-- day once; the post-hook removes the days counted in the old one.
{% set tz = var('timezone') | replace("'", "''") %}
{% set ns = namespace(rebuild=not is_incremental()) %}
{% if is_incremental() and execute %}
    {% set other = run_query("select 1 from " ~ this ~ " where timezone <> '" ~ tz ~ "' limit 1") %}
    {% set ns.rebuild = other.rows | length > 0 %}
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
    select 'screen'::text as source, {{ timeline_local_day('f.captured_at') }} as day
    from {{ ref('screen_frames') }} f
    where f._loaded_through > {{ screen_lower }}
    union
    select 'screen', {{ timeline_local_day('s.started_at') }}
    from {{ ref('timeline_screen') }} s
    where s._loaded_through > {{ screen_lower }}
    union
    select 'browser', {{ timeline_local_day('v.started_at') }}
    from {{ ref('timeline_browser') }} v
    where v._loaded_through > {{ browser_lower }}
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
            when 'screen' then {{ timeline_literal(screen_upper) }}
            else {{ timeline_literal(browser_upper) }}
        end as _loaded_through
    from spans d
)
select *{% if is_incremental() %}, (events = 0 and coalesce(frames, 0) = 0) as _deleted{% endif %}
from counted
{% if not is_incremental() %}where events > 0 or coalesce(frames, 0) > 0{% endif %}
