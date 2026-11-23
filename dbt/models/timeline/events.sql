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
        "{{ timeline_remove_foreign(['screen', 'browser', 'messages'], by_source=true) }}",
        "{{ timeline_add_media_columns() }}",
        "{{ timeline_media_pending_index('event_id', screen_only=true) }}",
    ],
    post_hook=[
        "{{ timeline_media_settle('false', screen_only=true) }}",
        "{{ timeline_exact_acl(true) }}",
    ],
) }}
-- Every source's events with the same columns; each source reaches it through one
-- model, timeline_<source>. An incremental build reads the rows each of those
-- models wrote since this one last read it, merges them on event_id and removes
-- the ones they mark deleted. A source whose input was unlinked or replaced loses
-- its rows here first and is read again in full. Media columns: what the worker
-- recorded of the event's file (timeline_media_columns), null when unknown.
--
-- A message (source 'messages', from the messages model) is one event at its
-- time: title is its text, body is null, group_key its conversation_id, media
-- its first attachment, and payload what the lane draws without a join (who,
-- counts, flags). Who sent it and in which conversation are not written as
-- names: those resolve through people and conversations, so a renamed contact
-- rewrites no event. Its search vector is empty: message text is searched in
-- messages, and the browser's search through this index must not wade through
-- every message. An undated message is not on the time axis and has no event.
{% set sources = [('screen', 'timeline_screen'), ('browser', 'timeline_browser')] %}
{% set replaced = timeline_replaced(['screen', 'browser', 'messages'], by_source=true) %}
{# A table holding screen events built before the media columns reads every
   source's rows once (browser and message events carry no media_withheld: null by
   design). #}
{% set upgrade = timeline_media_pending("source = 'screen'") %}
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
    media_status,
    thumbnail_w360,
    thumbnail_w1280,
    media_withheld,
    group_key,
    payload,
    setweight(to_tsvector('simple', coalesce(title, '')), 'A')
        || setweight(to_tsvector('simple', coalesce(body, '')), 'B') as search,
    _input,
    _stamp
    {% if is_incremental() %}, deleted as _deleted{% endif %}
from {{ ref(model) }}
{% if is_incremental() and source not in replaced and not upgrade %}
where _stamp > {{ timeline_literal(timeline_resume(this, "source = '" ~ source ~ "'")) }}
{% elif is_incremental() %}
where true
{% else %}
where not deleted
{% endif %}
union all
{% endfor %}
select
    'messages'::text as source,
    connection,
    'messages:' || guid as event_id,
    sent_at as started_at,
    sent_at as ended_at,
    'message'::text as kind,
    text as title,
    null::text as body,
    case when attachments->0->>'media_key' is not null then 'attachments' end as media_kind,
    attachments->0->>'media_key' as media_key,
    attachments->0->>'media_status' as media_status,
    (attachments->0->>'thumbnail_w360')::boolean as thumbnail_w360,
    (attachments->0->>'thumbnail_w1280')::boolean as thumbnail_w1280,
    null::boolean as media_withheld,
    conversation_id as group_key,
    jsonb_build_object(
        'guid', guid, 'conversation', conversation_id, 'from_me', is_from_me,
        'sender', sender_handle, 'service', service,
        'attachments', jsonb_array_length(attachments),
        'reactions', jsonb_array_length(reactions),
        'edited', is_edited, 'unsent', is_unsent, 'audio', is_audio,
        'reply_to', reply_to_guid
    ) as payload,
    ''::tsvector as search,
    _input,
    _stamp
    {% if is_incremental() %}, (deleted or sent_at is null) as _deleted{% endif %}
from {{ ref('messages') }}
{% if is_incremental() and 'messages' not in replaced and not upgrade %}
where _stamp > {{ timeline_literal(timeline_resume(this, "source = 'messages'")) }}
{% elif is_incremental() %}
where true
{% else %}
where not deleted and sent_at is not null
{% endif %}
