{{ config(enabled=var('miroir_namespace', none) is not none) }}
-- Chats with the conversation they belong to: the chat's own identifier (a
-- number, an address or a group id), never its guid, which differs between
-- services and macOS versions for the same conversation (decision 038). A chat
-- without an identifier falls back to the last part of its guid
-- ('iMessage;-;<identifier>').
with raw as (
    {{ optional_input('apple_messages', 'chats', {
        'guid': 'text', 'chat_identifier': 'text', 'service_name': 'text',
        'display_name': 'text', 'style': 'bigint', 'room_name': 'text'
    }, ['guid']) }}
)
select
    guid,
    coalesce(nullif(chat_identifier, ''), regexp_replace(guid, '^.*;', '')) as conversation_key,
    nullif(btrim(display_name), '') as title,
    style,
    nullif(room_name, '') as room_name,
    nullif(service_name, '') as service
from raw
