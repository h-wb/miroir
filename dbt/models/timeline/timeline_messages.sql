{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='view',
    grants={'select': []},
    post_hook="{{ timeline_exact_acl(false) }}",
) }}
-- Message events as every source's model gives them (package split, step 11):
-- the columns of events, `deleted`, `previous_started_at`, `_input` and
-- `_stamp`. A view over messages, so nothing is stored twice; internal, like
-- the other sources' models.
--
-- A message is one event at its time: title is its text, body is null,
-- group_key its conversation_id, media its first attachment, and payload what
-- the lane draws without a join (who, counts, flags). Who sent it and in which
-- conversation are not written as names: those resolve through people and
-- conversations, so a renamed contact rewrites no event. Its search vector is
-- empty (its `search` column): message text is searched in messages, and the
-- browser's search through events' index must not wade through every message.
-- An undated message is not on the time axis and has no event: it counts as
-- deleted here, as a removed message does.
{% set attachment = media_ref('messages', 'attachments', "attachments->0->>'media_key'") %}
select
    'messages'::text as source,
    connection,
    'messages:' || guid as event_id,
    sent_at as started_at,
    sent_at as ended_at,
    'message'::text as kind,
    text as title,
    null::text as body,
    case when attachments->0->>'media_key' is not null then {{ attachment.kind }} end as media_kind,
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
    (deleted or sent_at is null) as deleted,
    previous_sent_at as previous_started_at,
    _input,
    _stamp
from {{ ref('messages') }}
