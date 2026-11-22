{{ config(enabled=var('miroir_namespace', none) is not none) }}
-- Chats with the conversation they belong to. A group's key is its identifier;
-- a direct chat's is its identifier under messages_address, so one person over
-- iMessage, SMS and RCS, on any Mac, written '+1 555…' or '555…', is one
-- conversation (decision 041). Never the chat guid, which differs between
-- services and macOS versions. Without an identifier, the last part of the guid
-- ('iMessage;-;<identifier>'), else the whole guid.
-- A chat is a group when its style is 43, direct when 45; without either, a
-- group when it has a room name or lists more than one handle.
with raw as (
    {{ optional_input('apple_messages', 'chats', {
        'guid': 'text', 'chat_identifier': 'text', 'service_name': 'text',
        'display_name': 'text', 'style': 'bigint', 'room_name': 'text'
    }, ['guid']) }}
),
listed as (
    select chat_guid, count(distinct handle) as handles
    from {{ ref('stg_apple_messages__chat_handles') }}
    where handle <> ''
    group by 1
),
shaped as (
    select
        r.*,
        coalesce(nullif(r.chat_identifier, ''), nullif(regexp_replace(r.guid, '^.*;', ''), ''), r.guid)
            as identifier,
        case
            when r.style = 43 then true
            when r.style = 45 then false
            else nullif(r.room_name, '') is not null or coalesce(l.handles, 0) > 1
        end as is_group
    from raw r
    left join listed l on l.chat_guid = r.guid
)
select
    guid,
    case when is_group then identifier else {{ messages_address('identifier') }} end as conversation_key,
    identifier,
    is_group,
    nullif(btrim(display_name), '') as title,
    nullif(service_name, '') as service
from shaped
