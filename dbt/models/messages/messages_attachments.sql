{{ config(alias='attachments', enabled=var('miroir_namespace', none) is not none) }}
-- One row per attachment of a message (not of a reaction or an event). Link
-- previews are left out. Only attachments whose metadata arrived are here.
select
    ma.attachment_guid,
    ma.message_guid,
    m.conversation_key,
    m.sent_at,
    m.day,
    m.is_from_me,
    a.kind,
    a.mime_type,
    a.extension,
    a.bytes,
    a.has_media_key
from {{ ref('stg_apple_messages__message_attachments') }} ma
join {{ ref('stg_apple_messages__attachments') }} a on a.guid = ma.attachment_guid
join {{ ref('int_apple_messages__rows') }} m on m.guid = ma.message_guid
where m.kind = 'message' and not a.is_link_preview
