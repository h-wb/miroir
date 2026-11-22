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
    pre_hook="{{ timeline_remove_foreign(['screen', 'browser'], by_source=true) }}",
    post_hook="{{ timeline_exact_acl(true) }}",
) }}
-- Every source's events with the same columns; each source reaches it through one
-- model, timeline_<source>. An incremental build reads the rows each of those
-- models wrote since this one last read it, merges them on event_id and removes
-- the ones they mark deleted. A source whose input was unlinked or replaced loses
-- its rows here first and is read again in full.
{% set sources = [('screen', 'timeline_screen'), ('browser', 'timeline_browser')] %}
{% set replaced = timeline_replaced(['screen', 'browser'], by_source=true) %}
{% for source, model in sources %}
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
    group_key,
    payload,
    setweight(to_tsvector('simple', coalesce(title, '')), 'A')
        || setweight(to_tsvector('simple', coalesce(body, '')), 'B') as search,
    _input,
    _stamp
    {% if is_incremental() %}, deleted as _deleted{% endif %}
from {{ ref(model) }}
{% if is_incremental() and source not in replaced %}
where _stamp > {{ timeline_literal(timeline_resume(this, "source = '" ~ source ~ "'")) }}
{% elif is_incremental() %}
where true
{% else %}
where not deleted
{% endif %}
{% if not loop.last %}union all{% endif %}
{% endfor %}
