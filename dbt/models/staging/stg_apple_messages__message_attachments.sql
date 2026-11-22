{{ config(enabled=var('miroir_namespace', none) is not none) }}
{{ optional_input('apple_messages', 'message_attachments', {
    'message_guid': 'text', 'attachment_guid': 'text'
}, ['message_guid', 'attachment_guid']) }}
