{{ config(enabled=var('miroir_namespace', none) is not none) }}
-- Every message row with its conversation, local time and attachment count.
-- A message linked to chats of two conversations takes the first key; one
-- linked to no known chat has none.
{% set tz = var('timezone') | replace("'", "''") %}
with conversations as (
    select cm.message_guid, min(c.conversation_key) as conversation_key
    from {{ ref('stg_apple_messages__chat_messages') }} cm
    join {{ ref('stg_apple_messages__chats') }} c on c.guid = cm.chat_guid
    group by 1
),
attachments as (
    select ma.message_guid, count(*) as attachments
    from {{ ref('stg_apple_messages__message_attachments') }} ma
    join {{ ref('stg_apple_messages__attachments') }} a on a.guid = ma.attachment_guid
    where not a.is_link_preview
    group by 1
)
select
    m.*,
    c.conversation_key,
    (m.sent_at at time zone '{{ tz }}')::date as day,
    extract(hour from m.sent_at at time zone '{{ tz }}')::int as hour,
    extract(isodow from m.sent_at at time zone '{{ tz }}')::int as weekday,
    coalesce(a.attachments, 0) as attachments
from {{ ref('stg_apple_messages__messages') }} m
left join conversations c on c.message_guid = m.guid
left join attachments a on a.message_guid = m.guid
