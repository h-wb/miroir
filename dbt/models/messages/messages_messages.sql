{{ config(alias='messages', enabled=var('miroir_namespace', none) is not none) }}
-- One row per message: not reactions, not group events. No text, only its
-- length. Messages in no known chat are kept, with no conversation.
-- reply_minutes: in a direct conversation, for the first message after one
-- from the other side, the minutes since that message. Null beyond 24 hours,
-- when the conversation is taken to have started again.
with rows as (
    select r.*, c.is_group
    from {{ ref('int_apple_messages__rows') }} r
    left join {{ ref('messages_conversations') }} c using (conversation_key)
    where r.kind = 'message'
),
ordered as (
    select
        *,
        lag(is_from_me) over w as previous_from_me,
        lag(sent_at) over w as previous_sent_at
    from rows
    window w as (partition by conversation_key order by sent_at, guid)
)
select
    o.guid,
    o.conversation_key,
    o.is_group,
    o.sent_at,
    o.day,
    o.hour,
    o.weekday,
    o.is_from_me,
    o.sender_handle,
    case when o.sender_handle is not null
        then coalesce(n.display_name, o.sender_handle) end as sender_name,
    o.service,
    o.has_text,
    o.text_length,
    o.attachments,
    o.is_thread_reply,
    o.is_edited,
    o.is_unsent,
    o.is_audio,
    case when not o.is_group
        and o.previous_from_me <> o.is_from_me
        and o.sent_at - o.previous_sent_at <= interval '24 hours'
        then extract(epoch from o.sent_at - o.previous_sent_at)::double precision / 60
    end as reply_minutes
from ordered o
left join {{ ref('stg_apple_messages__contacts') }} n on n.handle = o.sender_handle
