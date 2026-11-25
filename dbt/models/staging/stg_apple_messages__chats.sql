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
        {{ messages_chat_identifier('r.guid', 'r.chat_identifier') }} as identifier,
        {{ messages_chat_is_group('r.style', 'r.room_name', 'l.handles') }} as is_group
    from raw r
    left join listed l on l.chat_guid = r.guid
)
select
    guid,
    {{ messages_conversation_key('identifier', 'is_group') }} as conversation_key,
    identifier,
    is_group,
    nullif(btrim(display_name), '') as title,
    nullif(service_name, '') as service
from shaped
