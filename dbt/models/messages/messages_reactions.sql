{{ config(alias='reactions', enabled=var('miroir_namespace', none) is not none) }}
-- One row per reaction that stands, by the rule both message packages share
-- (messages_standing_reactions).
with rows as (
    select * from {{ ref('int_apple_messages__rows') }}
    where kind in ('reaction', 'removal') and target_guid is not null
),
standing as (
    {{ messages_standing_reactions('rows') }}
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
