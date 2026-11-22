{{ config(alias='participants', enabled=var('miroir_namespace', none) is not none) }}
-- One row per conversation and person other than me: everyone listed in one
-- of its chats, and everyone who sent a message in it. A person is a number or
-- an address under messages_address, whatever the service or how it is
-- written; one person reached at a number and at an address is two rows.
with members as (
    select c.conversation_key, ch.address as handle, ch.handle as written
    from {{ ref('stg_apple_messages__chat_handles') }} ch
    join {{ ref('stg_apple_messages__chats') }} c on c.guid = ch.chat_guid
    where ch.handle <> ''
),
senders as (
    select
        conversation_key,
        sender_handle as handle,
        min(sender_written) as written,
        count(*) as messages_sent,
        min(sent_at) as first_message_at,
        max(sent_at) as last_message_at
    from {{ ref('int_apple_messages__rows') }}
    where kind = 'message' and sender_handle is not null and conversation_key is not null
    group by 1, 2
),
people as (
    select conversation_key, handle, min(written) as written
    from (
        select conversation_key, handle, written from members
        union all
        select conversation_key, handle, written from senders
    ) p
    group by 1, 2
)
select
    p.conversation_key,
    p.handle,
    coalesce(n.display_name, p.written) as display_name,
    n.display_name is not null as has_contact_name,
    coalesce(s.messages_sent, 0) as messages_sent,
    s.first_message_at,
    s.last_message_at
from people p
left join senders s using (conversation_key, handle)
left join {{ ref('stg_apple_messages__contacts') }} n on n.address = p.handle
