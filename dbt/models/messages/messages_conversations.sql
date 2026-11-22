{{ config(alias='conversations', enabled=var('miroir_namespace', none) is not none) }}
-- One row per conversation (its key: see stg_apple_messages__chats). It is a
-- group when any of its chats is.
-- Name: a group's own title (from its chat with the latest message), else its
-- participants' names (four, then "+N"), else "Group"; a direct conversation's
-- contact name (of its key, else of its only participant), else its identifier
-- as written in its chat with the latest message.
-- An undated message counts in the totals but not in the first and last times.
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
latest as (
    select distinct on (c.conversation_key) c.conversation_key, c.identifier
    from chats c
    left join chat_last l on l.chat_guid = c.guid
    order by c.conversation_key, l.last_at desc nulls last, c.guid
),
titles as (
    select distinct on (c.conversation_key) c.conversation_key, c.title
    from chats c
    left join chat_last l on l.chat_guid = c.guid
    where c.title is not null
    order by c.conversation_key, l.last_at desc nulls last, c.guid
),
shapes as (
    select conversation_key, bool_or(is_group) as is_group
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
)
select
    s.conversation_key,
    case
        when s.is_group then coalesce(
            t.title,
            array_to_string(p.names[1:4], ', ')
                || case when p.participants > 4 then ' +' || (p.participants - 4) else '' end,
            'Group'
        )
        else coalesce(
            n.display_name,
            case when p.participants = 1 and p.named = 1 then p.only_name end,
            l.identifier
        )
    end as display_name,
    s.is_group,
    coalesce(p.participants, 0) as participants,
    c.first_message_at,
    c.last_message_at,
    coalesce(c.messages_sent, 0) as messages_sent,
    coalesce(c.messages_received, 0) as messages_received,
    coalesce(c.attachments, 0) as attachments,
    v.services
from shapes s
join latest l using (conversation_key)
left join titles t using (conversation_key)
left join people p using (conversation_key)
left join counts c using (conversation_key)
left join services v using (conversation_key)
left join {{ ref('stg_apple_messages__contacts') }} n
    on not s.is_group and n.address = s.conversation_key
