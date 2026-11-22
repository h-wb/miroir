{{ config(alias='reactions', enabled=var('miroir_namespace', none) is not none) }}
-- One row per reaction that stands.
-- Tapbacks (love to emoji): Messages deletes a tapback's row when it is
-- removed, but rows already loaded stay, so a removal cancels the tapback of
-- the same kind by the same sender on the same part of the same message (the
-- latest row of that kind decides; a removal at the same time wins). A person
-- holds one tapback per part: of the standing ones, only the latest is kept.
-- Stickers are not tapbacks: each sticker row is one reaction, and a sticker
-- removal (3007) cannot say which sticker it removes, so it cancels none.
with rows as (
    select * from {{ ref('int_apple_messages__rows') }}
    where kind in ('reaction', 'removal') and target_guid is not null
),
tapbacks as (
    select distinct on (is_from_me, coalesce(sender_handle, ''), target_guid, target_part, reaction_kind) *
    from rows
    where reaction_kind <> 'sticker'
    order by is_from_me, coalesce(sender_handle, ''), target_guid, target_part, reaction_kind,
        sent_at desc nulls last, kind = 'removal' desc, guid desc
),
held as (
    select distinct on (is_from_me, coalesce(sender_handle, ''), target_guid, target_part) *
    from tapbacks
    where kind = 'reaction'
    order by is_from_me, coalesce(sender_handle, ''), target_guid, target_part,
        sent_at desc nulls last, guid desc
),
standing as (
    select * from held
    union all
    select * from rows where reaction_kind = 'sticker' and kind = 'reaction'
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
        then coalesce(n.display_name, l.sender_written) end as sender_name,
    l.target_guid,
    l.target_part
from standing l
left join {{ ref('int_apple_messages__rows') }} t on t.guid = l.target_guid
left join {{ ref('stg_apple_messages__contacts') }} n on n.address = l.sender_handle
