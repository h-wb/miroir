{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    incremental_strategy='timeline_replace',
    grants=timeline_grants(),
    indexes=[
        {'columns': ['conversation_id'], 'unique': True},
        {'columns': ['last_message_at']},
    ],
    post_hook="{{ timeline_exact_acl(true) }}",
) }}
-- One row per conversation: one person or one group, whatever Apple chats it
-- has (decision 041's key, through timeline_message_chats). conversation_id is
-- opaque (timeline_conversation_id): routes and logs use it, never the key,
-- which for a direct conversation is a phone number or an address and is not in
-- this table.
--   display_name: a group's own title (of its chat loaded last that has one),
--     else its people's names (four, then "+N"), else "Group"; a direct
--     conversation's person's name, else the handle as written. Names come from
--     `people`, so a renamed contact renames the conversation at the next build.
--   participants: the people (people.handle) listed in its chats or who sent in
--     it, never me, in order of handle.
--   message_count counts undated messages; first and last times do not.
-- Rebuilt every build: from the chats (hundreds of rows) and from index-only
-- scans of messages' (conversation_id, ...) indexes.
with chats as (
    select * from {{ ref('timeline_message_chats') }}
),
shapes as (
    select
        conversation_id,
        min(conversation_key) as conversation_key,
        bool_or(is_group) as is_group,
        (array_agg(title order by loaded desc, guid) filter (where title is not null))[1] as title,
        (array_agg(identifier order by loaded desc, guid))[1] as identifier
    from chats
    group by 1
),
listed as (
    select c.conversation_id, m.handle
    from chats c
    cross join unnest(c.members) m (handle)
),
senders as (
    select distinct conversation_id, sender_handle as handle
    from {{ ref('messages') }}
    where conversation_id is not null and sender_handle is not null
),
participants as (
    select conversation_id, handle from listed
    union
    select conversation_id, handle from senders
),
named as (
    select
        p.conversation_id,
        count(*) as participants,
        array_agg(p.handle order by p.handle) as handles,
        array_agg(coalesce(n.display_name, p.handle) order by coalesce(n.display_name, p.handle), p.handle) as names,
        count(*) filter (where n.name is not null) as named,
        min(n.name) as only_name
    from participants p
    left join {{ ref('people') }} n on n.handle = p.handle
    group by 1
),
counts as (
    select conversation_id, count(*) as messages, min(sent_at) as first_at, max(sent_at) as last_at
    from {{ ref('messages') }}
    where conversation_id is not null
    group by 1
)
select
    s.conversation_id,
    case
        when s.is_group then coalesce(
            s.title,
            array_to_string(p.names[1:4], ', ')
                || case when p.participants > 4 then ' +' || (p.participants - 4) else '' end,
            'Group'
        )
        else coalesce(
            k.name,
            case when p.participants = 1 and p.named = 1 then p.only_name end,
            k.written,
            s.identifier
        )
    end as display_name,
    s.is_group,
    s.title,
    coalesce(p.handles, '{}'::text[]) as participants,
    coalesce(p.participants, 0) as participant_count,
    c.first_at as first_message_at,
    c.last_at as last_message_at,
    coalesce(c.messages, 0) as message_count,
    {{ timeline_connection('messages') }} as connection
from shapes s
left join named p using (conversation_id)
left join counts c using (conversation_id)
left join {{ ref('people') }} k on not s.is_group and k.handle = s.conversation_key
