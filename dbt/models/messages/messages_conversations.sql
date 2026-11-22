{{ config(alias='conversations', enabled=var('miroir_namespace', none) is not none) }}
-- One row per conversation, keyed by the chats' own identifier: the chats of
-- one person over iMessage, SMS and RCS, and the same chat seen from Macs on
-- different macOS versions, are one conversation.
-- A group is style 43 (45 is a direct conversation); a chat without a style is
-- a group when it has a room name or more than one participant.
-- Name: a group's own title (from its chat with the latest message), else its
-- participants' names (four, then "+N"); a direct conversation's contact name
-- (of its identifier, else of its only participant), else the identifier.
with chats as (
    select * from {{ ref('stg_apple_messages__chats') }}
),
rows as (
    select * from {{ ref('int_apple_messages__rows') }}
),
chat_last as (
    select cm.chat_guid, max(r.sent_at) as last_at
    from {{ ref('stg_apple_messages__chat_messages') }} cm
    join rows r on r.guid = cm.message_guid
    group by 1
),
titles as (
    select distinct on (c.conversation_key) c.conversation_key, c.title
    from chats c
    left join chat_last l on l.chat_guid = c.guid
    where c.title is not null
    order by c.conversation_key, l.last_at desc nulls last, c.guid
),
shapes as (
    select
        conversation_key,
        bool_or(style = 43) as styled_group,
        bool_or(style = 45) as styled_direct,
        bool_or(room_name is not null) as has_room
    from chats
    group by 1
),
people as (
    select
        conversation_key,
        count(*) as participants,
        min(display_name) filter (where has_contact_name) as only_name,
        count(*) filter (where has_contact_name) as named,
        array_agg(display_name order by display_name, handle) as names
    from {{ ref('messages_participants') }}
    group by 1
),
counts as (
    select
        conversation_key,
        count(*) filter (where is_from_me) as messages_sent,
        count(*) filter (where not is_from_me) as messages_received,
        sum(attachments) as attachments,
        min(sent_at) as first_message_at,
        max(sent_at) as last_message_at
    from rows
    where kind = 'message' and conversation_key is not null
    group by 1
),
services as (
    select conversation_key, string_agg(distinct service, ', ' order by service) as services
    from (
        select conversation_key, service from rows
        where kind = 'message' and conversation_key is not null
        union all
        select conversation_key, service from chats
    ) s
    where service is not null
    group by 1
),
conversations as (
    select
        s.conversation_key,
        coalesce(s.styled_group, false)
            or (not coalesce(s.styled_direct, false)
                and (coalesce(s.has_room, false) or coalesce(p.participants, 0) > 1)) as is_group,
        t.title,
        p.participants,
        p.names,
        case when p.participants = 1 and p.named = 1 then p.only_name end as only_name
    from shapes s
    left join titles t using (conversation_key)
    left join people p using (conversation_key)
)
select
    c.conversation_key,
    case
        when c.is_group then coalesce(
            c.title,
            array_to_string(c.names[1:4], ', ')
                || case when c.participants > 4 then ' +' || (c.participants - 4) else '' end,
            c.conversation_key
        )
        else coalesce(n.display_name, c.only_name, c.conversation_key)
    end as display_name,
    c.is_group,
    coalesce(c.participants, 0) as participants,
    n2.first_message_at,
    n2.last_message_at,
    coalesce(n2.messages_sent, 0) as messages_sent,
    coalesce(n2.messages_received, 0) as messages_received,
    coalesce(n2.attachments, 0) as attachments,
    v.services
from conversations c
left join {{ ref('stg_apple_messages__contacts') }} n
    on not c.is_group and n.handle = c.conversation_key
left join counts n2 using (conversation_key)
left join services v using (conversation_key)
