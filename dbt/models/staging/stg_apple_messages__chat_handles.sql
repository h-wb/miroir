{{ config(enabled=var('miroir_namespace', none) is not none) }}
{{ optional_input('apple_messages', 'chat_handles', {
    'chat_guid': 'text', 'handle': 'text', 'handle_service': 'text'
}, ['chat_guid', 'handle']) }}
