{{ config(enabled=var('miroir_namespace', none) is not none) }}
-- Attachment metadata with a coarse type, from the mime type, else from the
-- extension of the stored name or of the transfer name. Link previews
-- (.pluginPayloadAttachment) are what Messages fetched for a link, not a file
-- anyone sent, and are marked so.
with raw as (
    {{ optional_input('apple_messages', 'attachments', {
        'guid': 'text', 'transfer_name': 'text', 'mime_type': 'text',
        'total_bytes': 'bigint', 'media_key': 'text', 'is_sticker': 'boolean'
    }, ['guid']) }}
),
named as (
    select *, {{ messages_attachment_extension('media_key', 'transfer_name') }} as extension
    from raw
)
select
    guid,
    mime_type,
    extension,
    {{ messages_attachment_kind('mime_type', 'extension') }} as kind,
    total_bytes as bytes,
    media_key is not null as has_media_key,
    {{ messages_is_link_preview('transfer_name') }} as is_link_preview
from named
