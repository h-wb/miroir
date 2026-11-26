{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['event_id'], 'unique': True},
        {'columns': ['source', '_input']},
        {'columns': ['source', 'started_at']},
        {'columns': ['group_key', 'started_at']},
        {'columns': ['search'], 'type': 'gin'},
        {'columns': ['source', '_stamp']},
    ],
    grants=timeline_grants(),
    pre_hook=[
        "{{ timeline_remove_foreign(timeline_lane_inputs(), by_source=true) }}",
        "{{ timeline_add_media_columns() }}",
        "{{ timeline_media_pending_index('event_id', \"source = 'screen'\") }}",
    ],
    post_hook=[
        "{{ timeline_media_settle('false', \"source = 'screen'\") }}",
        "{{ timeline_exact_acl(true) }}",
    ],
) }}
-- Every source's events with the same columns; each source is a lane the build
-- declares (timeline_lanes, from src/analyses/views.py) and reaches it through
-- one model, timeline_<source>, with these columns and `deleted`,
-- `previous_started_at`, `_input` and `_stamp`. An incremental build reads the
-- rows each of those models wrote since this one last read it, merges them on
-- event_id and removes the ones they mark deleted. A source whose input was
-- unlinked or replaced loses its rows here first and is read again in full.
-- Media columns: what the worker recorded of the event's file
-- (timeline_media_columns), null when unknown. `search` weights the title over
-- the body, unless the model writes its own (messages: empty, message text is
-- searched in messages).
--
-- Only the screen's sessions carry media_withheld (true or false); other
-- sources' rows have none by design, so only screen rows count as pending.
{% set lanes = timeline_lanes() %}
{% set replaced = timeline_replaced(timeline_lane_inputs(), by_source=true) %}
{# A table holding screen events built before the media columns reads every
   source's rows once (browser and message events carry no media_withheld: null by
   design). #}
{% set upgrade = timeline_media_pending("source = 'screen'") %}
{% for lane in lanes %}
{% set model = ref('timeline_' ~ lane.source) %}
{% set own_search = execute and 'search' in (adapter.get_columns_in_relation(model) | map(attribute='name') | list) %}
select
    source,
    connection,
    event_id,
    started_at,
    ended_at,
    kind,
    title,
    body,
    media_kind,
    media_key,
    media_status,
    thumbnail_w360,
    thumbnail_w1280,
    media_withheld,
    group_key,
    payload,
    {% if own_search %}search{% else %}setweight(to_tsvector('simple', coalesce(title, '')), 'A')
        || setweight(to_tsvector('simple', coalesce(body, '')), 'B') as search{% endif %},
    _input,
    _stamp
    {% if is_incremental() %}, deleted as _deleted{% endif %}
from {{ model }}
{% if is_incremental() and lane.source not in replaced and not upgrade %}
where _stamp > {{ timeline_literal(timeline_resume(this, "source = '" ~ lane.source ~ "'")) }}
{% elif is_incremental() %}
where true
{% else %}
where not deleted
{% endif %}
{% if not loop.last %}union all{% endif %}
{% endfor %}
