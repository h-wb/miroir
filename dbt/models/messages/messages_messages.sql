{{ config(alias='messages', enabled=var('miroir_namespace', none) is not none) }}
-- One row per message: not reactions, not group events. No text, only its
-- length. Messages in no known chat are kept, with no conversation.
-- reply_minutes: in a direct conversation, a run is the messages one side sends
-- before the other answers. The first message of a run gets the minutes since
-- the first message of the run before it: how long the other side waited.
-- Null beyond 24 hours, when the conversation is taken to have started again,
-- and for every other message. Undated messages take no part.
with rows as (
    select r.*, c.is_group
    from {{ ref('int_apple_messages__rows') }} r
    left join {{ ref('messages_conversations') }} c using (conversation_key)
    where r.kind = 'message'
),
direct as (
    select
        guid, conversation_key, sent_at, is_from_me,
        lag(is_from_me) over w is distinct from is_from_me as starts_run,
        row_number() over w as position
    from rows
    where is_group = false and sent_at is not null
    window w as (partition by conversation_key order by sent_at, guid)
),
runs as (
    select
        *,
        sum(starts_run::int) over (partition by conversation_key order by position) as run
    from direct
),
run_starts as (
    select conversation_key, run, min(sent_at) as started_at
    from runs
    group by 1, 2
),
replies as (
    select r.guid, extract(epoch from r.sent_at - p.started_at)::double precision / 60 as minutes
    from runs r
    join run_starts p on p.conversation_key = r.conversation_key and p.run = r.run - 1
    where r.starts_run and r.sent_at - p.started_at <= interval '24 hours'
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
        then coalesce(n.display_name, o.sender_written) end as sender_name,
    o.service,
    o.has_text,
    o.text_length,
    o.attachments,
    o.is_thread_reply,
    o.is_edited,
    o.is_unsent,
    o.is_audio,
    r.minutes as reply_minutes
from rows o
left join replies r using (guid)
left join {{ ref('stg_apple_messages__contacts') }} n on n.address = o.sender_handle
