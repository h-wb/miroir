{{ config(enabled=var('miroir_namespace', none) is not none) }}
with raw as (
    {{ optional_input('apple_messages', 'chat_handles', {
        'chat_guid': 'text', 'handle': 'text', 'handle_service': 'text'
    }, ['chat_guid', 'handle']) }}
)
select chat_guid, handle, {{ messages_address('handle') }} as address, handle_service
from raw
