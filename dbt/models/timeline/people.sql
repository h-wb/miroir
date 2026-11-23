{{ config(
    enabled=var('miroir_namespace', none) is not none,
    grants=timeline_grants(),
    indexes=[{'columns': ['handle'], 'unique': True}],
    post_hook="{{ timeline_exact_acl(true) }}",
) }}
-- One row per person other than me who appears in Messages: listed in a chat,
-- sender of a message, or named in the address book. handle is the person
-- (messages_address), the key messages.sender_handle, conversations.participants
-- and a reaction's sender use. name is the address book's (decision 038's
-- matching; messages_contact_name), null without one; written is the handle as
-- Messages writes it (the first in order where it is written several ways);
-- display_name is the name, else the handle as written. A message's sender is
-- shown as the name, else as that message writes the handle
-- (messages.sender_written), as decision 041 does.
--
-- Names are resolved here and nowhere else: a contact renamed later shows on
-- every old message at the next build, and no message row is rewritten. Rebuilt
-- every build: the contacts and chat handles are small snapshots read whole
-- (completed loads only), and senders are walked along an index of messages, one
-- probe per distinct sender.
with completed as not materialized (
    {{ timeline_completed('apple_messages') }}
),
contacts as (
    select k.* from (
        {{ optional_input('apple_messages', 'contacts', {
            'handle': 'text', 'first_name': 'text', 'last_name': 'text',
            'nickname': 'text', 'organization': 'text', '_dlt_load_id': 'text'
        }, ['handle']) }}
    ) k
    where {{ timeline_completed_row('k') }}
),
names as (
    select {{ messages_address('handle') }} as handle, min(handle) as written, min(name) as name
    from (
        select handle, {{ messages_contact_name('first_name', 'last_name', 'nickname', 'organization') }} as name
        from contacts
    ) n
    group by 1
),
listed as (
    select {{ messages_address('h.handle') }} as handle, h.handle as written
    from (
        {{ optional_input('apple_messages', 'chat_handles', {
            'chat_guid': 'text', 'handle': 'text', '_dlt_load_id': 'text'
        }, ['chat_guid', 'handle']) }}
    ) h
    where h.handle <> '' and {{ timeline_completed_row('h') }}
),
-- Distinct (sender, as written) pairs, one index probe each.
senders as (
    with recursive walk (handle, written) as (
        (select sender_handle, sender_written from {{ ref('messages') }}
         where sender_handle is not null order by 1, 2 limit 1)
        union all
        select n.sender_handle, n.sender_written
        from walk w
        cross join lateral (
            select m.sender_handle, m.sender_written from {{ ref('messages') }} m
            where m.sender_handle is not null
                and (m.sender_handle, m.sender_written) > (w.handle, w.written)
            order by 1, 2 limit 1
        ) n
    )
    select handle, written from walk
),
seen as (
    select handle, min(written) as written
    from (select * from listed union all select * from senders) s
    group by 1
)
select
    coalesce(s.handle, n.handle) as handle,
    n.name,
    coalesce(s.written, n.written) as written,
    coalesce(n.name, s.written, n.written) as display_name
from seen s
full join names n on n.handle = s.handle
