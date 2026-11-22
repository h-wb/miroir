{{ config(alias='reactions', enabled=var('miroir_namespace', none) is not none) }}
-- One row per reaction that stands. Messages deletes a reaction's row when it
-- is removed, but rows already loaded stay, so a removal cancels the reaction
-- of the same kind by the same sender on the same part of the same message.
-- For each of those, the latest row decides; a removal at the same time wins.
-- A reaction replaced by another kind without a removal row counts as both.
with rows as (
    select * from {{ ref('int_apple_messages__rows') }}
    where kind in ('reaction', 'removal') and target_guid is not null
),
latest as (
    select distinct on (is_from_me, coalesce(sender_handle, ''), target_guid, target_part, reaction_kind) *
    from rows
    order by is_from_me, coalesce(sender_handle, ''), target_guid, target_part, reaction_kind,
        sent_at desc nulls last, kind = 'removal' desc, guid desc
)
select
    l.guid,
    coalesce(l.conversation_key, t.conversation_key) as conversation_key,
    l.sent_at,
    l.day,
    l.hour,
    l.reaction_kind as kind,
    l.is_from_me,
    l.sender_handle,
    case when l.sender_handle is not null
        then coalesce(n.display_name, l.sender_handle) end as sender_name,
    l.target_guid,
    l.target_part
from latest l
left join {{ ref('int_apple_messages__rows') }} t on t.guid = l.target_guid
left join {{ ref('stg_apple_messages__contacts') }} n on n.handle = l.sender_handle
where l.kind = 'reaction'
