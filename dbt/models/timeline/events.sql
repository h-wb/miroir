{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['event_id'], 'unique': True},
        {'columns': ['source', 'started_at']},
        {'columns': ['group_key', 'started_at']},
        {'columns': ['search'], 'type': 'gin'},
        {'columns': ['source', '_loaded_through']},
    ],
    grants=timeline_grants(),
) }}
-- Every source's events with the same columns; each source reaches it through one
-- model, timeline_<source>. An incremental build reads the rows each of those
-- models wrote since this one last read it, merges them on event_id and removes
-- the ones they mark deleted.
{% set sources = [('screen', 'timeline_screen'), ('browser', 'timeline_browser')] %}
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
    _loaded_through
    {% if is_incremental() %}, deleted as _deleted{% endif %}
from {{ ref(model) }}
{% if is_incremental() %}
where _loaded_through > {{ timeline_literal(timeline_resume(this, "source = '" ~ source ~ "'")) }}
{% else %}
where not deleted
{% endif %}
{% if not loop.last %}union all{% endif %}
{% endfor %}
