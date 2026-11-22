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
    select
        *,
        lower(mime_type) as mime,
        lower(coalesce(
            substring(media_key from '\.([A-Za-z0-9]{1,10})$'),
            substring(transfer_name from '\.([A-Za-z0-9]{1,10})$')
        )) as extension
    from raw
)
select
    guid,
    mime_type,
    extension,
    case
        when mime like 'image/%' then 'image'
        when mime like 'video/%' then 'video'
        when mime like 'audio/%' then 'audio'
        when mime like 'text/%' or mime in ('application/pdf', 'application/rtf', 'application/msword')
            or mime like 'application/vnd.openxmlformats-officedocument.%'
            or mime like 'application/vnd.ms-%'
            or mime like 'application/vnd.apple.%' then 'document'
        when extension in ('heic', 'heif', 'jpeg', 'jpg', 'png', 'gif', 'webp', 'tiff', 'tif', 'bmp') then 'image'
        when extension in ('mov', 'mp4', 'm4v', '3gp') then 'video'
        when extension in ('caf', 'm4a', 'mp3', 'aac', 'amr', 'wav') then 'audio'
        when extension in ('pdf', 'vcf', 'txt', 'rtf', 'csv', 'ics', 'doc', 'docx', 'xls', 'xlsx',
            'ppt', 'pptx', 'pages', 'numbers', 'key') then 'document'
        else 'other'
    end as kind,
    total_bytes as bytes,
    media_key is not null as has_media_key,
    coalesce(lower(transfer_name) like '%.pluginpayloadattachment', false) as is_link_preview
from named
