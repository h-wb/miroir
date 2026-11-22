{{ config(enabled=var('miroir_namespace', none) is not none) }}
{{ optional_input('apple_messages', 'chat_messages', {
    'chat_guid': 'text', 'message_guid': 'text'
}, ['chat_guid', 'message_guid']) }}
