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
    pre_hook="{{ timeline_remove_foreign(timeline_lane_inputs(), by_source=true) }}",
    post_hook=[
        "delete from {{ this }} where timezone <> '{{ var('timezone') | replace(\"'\", \"''\") }}'",
        "{{ timeline_exact_acl(true) }}",
        "delete from {{ ref('messages') }} where deleted",
    ],
) }}
-- depends_on: {{ ref('screen_frames') }}, {{ ref('messages') }}
{%- for lane in timeline_lanes() %}, {{ ref('timeline_' ~ lane.source) }}{% endfor %}
-- Per local day (in the analysis's timezone) and source: how many events started
-- that day, and for the screen how many frames were captured. A day with neither
-- has no row. The sources are the lanes the build declares (timeline_lanes).
--
-- An incremental build recounts only the days of rows written since its last
-- build, removed rows included, and the days a moved row left: each source's
-- model (timeline_<source>) keeps its old times in previous_started_at until
-- this model has read them, and the screen's frames theirs in
-- previous_captured_at. A changed timezone or a source whose input was unlinked
-- or replaced recounts every day once: the pre-hook removes that source's days,
-- the post-hook the days counted in the old timezone.
--
-- Messages: events of source 'messages', recounted for the days of messages
-- written since (timeline_messages, previous_sent_at included). The last
-- post-hook removes the former messages that messages kept as deleted rows:
-- events and this model have both read them now, and a rebuild from scratch
-- would have none.
--
-- Known limit: where the clocks go back at midnight, a day starts at the later
-- of the two midnights, so the hour before it counts on the previous day.
{% set tz = var('timezone') | replace("'", "''") %}
{% set lanes = timeline_lanes() %}
{% set ns = namespace(rebuild=not is_incremental()) %}
{% if is_incremental() and execute %}
    {% set other = run_query("select 1 from " ~ this ~ " where timezone <> '" ~ tz ~ "' limit 1") %}
    {% set ns.rebuild = other.rows | length > 0
        or timeline_replaced(timeline_lane_inputs(), by_source=true) | length > 0 %}
{% endif %}
{# Where each source's days resume: the newest stamp of its model, and for the
   screen of its frames, which its sessions are built from. #}
{% set upper = {} %}
{% set lower = {} %}
{% for lane in lanes %}
    {% if lane.source == 'screen' %}
        {% do upper.update({lane.source: timeline_resume(ref('screen_frames'))}) %}
    {% else %}
        {% do upper.update({lane.source: timeline_resume(ref('timeline_' ~ lane.source))}) %}
    {% endif %}
    {% if not ns.rebuild %}
        {% do lower.update({lane.source: timeline_literal(timeline_resume(this, "source = '" ~ lane.source ~ "'"))}) %}
    {% endif %}
{% endfor %}
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
    where f._stamp > {{ lower.get('screen', timeline_literal('')) }} and moved.at is not null
    {% for lane in lanes %}
    union
    select '{{ lane.source }}', {{ timeline_local_day('moved.at') }}
    from {{ ref('timeline_' ~ lane.source) }} s
    cross join lateral (select s.started_at union all select unnest(s.previous_started_at)) moved (at)
    where s._stamp > {{ lower[lane.source] }} and moved.at is not null
    {% endfor %}
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
            {%- for lane in lanes %}
            when '{{ lane.source }}' then {{ timeline_literal(timeline_input(lane.role)) }}
            {%- endfor %}
        end as _input,
        case d.source
            {%- for lane in lanes %}
            when '{{ lane.source }}' then {{ timeline_literal(upper[lane.source]) }}
            {%- endfor %}
        end as _stamp
    from spans d
)
select *{% if is_incremental() %}, (events = 0 and coalesce(frames, 0) = 0) as _deleted{% endif %}
from counted
{% if not is_incremental() %}where events > 0 or coalesce(frames, 0) > 0{% endif %}
