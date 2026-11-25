{# The rules of Apple Messages data (decision 041), shared by the messages
   package's staging models and the timeline package's message models
   (decision 040), so the two cannot disagree on what a conversation, a message
   or a reaction is. Each macro is an SQL expression over the columns it is
   given. A test builds both packages on the same rows and compares them. #}

{# One person's number or address, the same rule the collector uses to match a
   handle to a contact: an address (it holds '@') in lower case; a number as its
   digits, a leading 1 dropped from eleven; anything else, such as a short code,
   as written. Groups never use it. #}
{% macro messages_address(value) -%}
    {%- set digits = "regexp_replace(" ~ value ~ ", '[^0-9]', '', 'g')" -%}
    case
        when {{ value }} like '%@%' then lower({{ value }})
        when length({{ digits }}) = 11 and {{ digits }} like '1%' then substr({{ digits }}, 2)
        when length({{ digits }}) >= 7 then {{ digits }}
        else {{ value }}
    end
{%- endmacro %}

{# A chat's own identifier: chat_identifier, else the last part of its guid
   ('iMessage;-;<identifier>'), else the whole guid, so it is never empty. #}
{% macro messages_chat_identifier(guid, chat_identifier) -%}
    coalesce(nullif({{ chat_identifier }}, ''), nullif(regexp_replace({{ guid }}, '^.*;', ''), ''), {{ guid }})
{%- endmacro %}

{# A chat is a group when its style is 43, direct when 45; without either, a
   group when it has a room name or lists more than one handle. #}
{% macro messages_chat_is_group(style, room_name, listed_handles) -%}
    case
        when {{ style }} = 43 then true
        when {{ style }} = 45 then false
        else nullif({{ room_name }}, '') is not null or coalesce({{ listed_handles }}, 0) > 1
    end
{%- endmacro %}

{# The conversation a chat belongs to: a group's identifier as it is, a direct
   chat's identifier as a person (messages_address). Never the chat guid, which
   differs between services and macOS versions. #}
{% macro messages_conversation_key(identifier, is_group) -%}
    case when {{ is_group }} then {{ identifier }} else {{ messages_address(identifier) }} end
{%- endmacro %}

{# A contact's name: first and last name, else the nickname, else the
   organization; null when it has none. #}
{% macro messages_contact_name(first_name, last_name, nickname, organization) -%}
    coalesce(
        nullif(btrim(concat_ws(' ', btrim({{ first_name }}), btrim({{ last_name }}))), ''),
        nullif(btrim({{ nickname }}), ''),
        nullif(btrim({{ organization }}), '')
    )
{%- endmacro %}

{# What a row of the messages table is:
   'message' (item_type 0, associated_message_type 0, 2 or 3, or null),
   'reaction' (2000-2007, and 1000: a sticker placed on a message), 'removal'
   (3000-3007), 'event' (any other item_type: group changes, FaceTime, shared
   location, kept audio), 'other' (any other associated type, such as 4000, a
   poll vote). #}
{% macro messages_row_kind(item_type, associated_message_type) -%}
    case
        when coalesce({{ item_type }}, 0) <> 0 then 'event'
        when {{ associated_message_type }} = 1000
            or {{ associated_message_type }} between 2000 and 2007 then 'reaction'
        when {{ associated_message_type }} between 3000 and 3007 then 'removal'
        when coalesce({{ associated_message_type }}, 0) in (0, 2, 3) then 'message'
        else 'other'
    end
{%- endmacro %}

{# The kind of a reaction or removal row (null for any other row). #}
{% macro messages_reaction_kind(associated_message_type) -%}
    case
        when {{ associated_message_type }} = 1000 then 'sticker'
        when {{ associated_message_type }} not between 2000 and 3007 then null
        when {{ associated_message_type }} between 2008 and 2999 then null
        when {{ associated_message_type }} % 1000 = 0 then 'love'
        when {{ associated_message_type }} % 1000 = 1 then 'like'
        when {{ associated_message_type }} % 1000 = 2 then 'dislike'
        when {{ associated_message_type }} % 1000 = 3 then 'laugh'
        when {{ associated_message_type }} % 1000 = 4 then 'emphasis'
        when {{ associated_message_type }} % 1000 = 5 then 'question'
        when {{ associated_message_type }} % 1000 = 6 then 'emoji'
        when {{ associated_message_type }} % 1000 = 7 then 'sticker'
    end
{%- endmacro %}

{# The message a reaction targets: 'p:<part>/<guid>' targets one part of a
   message, 'bp:<guid>' a link preview or app bubble (part 0); a bare guid is
   taken as it is. The timeline indexes the raw table on exactly this
   expression (messages_target_expression), so keep the two the same. #}
{% macro messages_target_expression(column) -%}
    regexp_replace({{ column }}, '^(p:[0-9]+/|bp:)', '')
{%- endmacro %}

{% macro messages_target_guid(column) -%}
    nullif({{ messages_target_expression(column) }}, '')
{%- endmacro %}

{% macro messages_target_part(column) -%}
    coalesce(substring({{ column }} from '^p:([0-9]+)/')::int, 0)
{%- endmacro %}

{# A time before 2001-01-02 is no time: Apple's clock starts at 2001-01-01, and
   a date of 0 or close to it is a missing date, not a message from 2001. #}
{% macro messages_dated(column) -%}
    case when {{ column }} >= timestamptz '2001-01-02 00:00:00+00' then {{ column }} end
{%- endmacro %}

{# An unsent message can carry an edit time as well; it counts as unsent only. #}
{% macro messages_is_unsent(retracted_at) -%}
    ({{ retracted_at }} is not null)
{%- endmacro %}

{% macro messages_is_edited(edited_at, retracted_at) -%}
    ({{ edited_at }} is not null and {{ retracted_at }} is null)
{%- endmacro %}

{# An attachment's extension: of its stored name, else of its transfer name. #}
{% macro messages_attachment_extension(media_key, transfer_name) -%}
    lower(coalesce(
        substring({{ media_key }} from '\.([A-Za-z0-9]{1,10})$'),
        substring({{ transfer_name }} from '\.([A-Za-z0-9]{1,10})$')
    ))
{%- endmacro %}

{# A coarse type, from the mime type, else from the extension. #}
{% macro messages_attachment_kind(mime_type, extension) -%}
    {%- set mime = 'lower(' ~ mime_type ~ ')' -%}
    case
        when {{ mime }} like 'image/%' then 'image'
        when {{ mime }} like 'video/%' then 'video'
        when {{ mime }} like 'audio/%' then 'audio'
        when {{ mime }} like 'text/%' or {{ mime }} in ('application/pdf', 'application/rtf', 'application/msword')
            or {{ mime }} like 'application/vnd.openxmlformats-officedocument.%'
            or {{ mime }} like 'application/vnd.ms-%'
            or {{ mime }} like 'application/vnd.apple.%' then 'document'
        when {{ extension }} in ('heic', 'heif', 'jpeg', 'jpg', 'png', 'gif', 'webp', 'tiff', 'tif', 'bmp') then 'image'
        when {{ extension }} in ('mov', 'mp4', 'm4v', '3gp') then 'video'
        when {{ extension }} in ('caf', 'm4a', 'mp3', 'aac', 'amr', 'wav') then 'audio'
        when {{ extension }} in ('pdf', 'vcf', 'txt', 'rtf', 'csv', 'ics', 'doc', 'docx', 'xls', 'xlsx',
            'ppt', 'pptx', 'pages', 'numbers', 'key') then 'document'
        else 'other'
    end
{%- endmacro %}

{# Link previews (.pluginPayloadAttachment) are what Messages fetched for a
   link, not a file anyone sent. #}
{% macro messages_is_link_preview(transfer_name) -%}
    coalesce(lower({{ transfer_name }}) like '%.pluginpayloadattachment', false)
{%- endmacro %}

{# The reactions that stand, from a relation of reaction and removal rows with
   guid, kind ('reaction' or 'removal'), is_from_me, sender_handle,
   target_guid, target_part, reaction_kind and sent_at.
   Tapbacks (love to emoji): Messages deletes a tapback's row when it is
   removed, but rows already loaded stay, so a removal cancels the tapback of the
   same kind by the same sender on the same part of the same message (the latest
   row of that kind decides; a removal at the same time wins). A person holds
   one tapback per part: of the standing ones, only the latest is kept.
   Stickers are not tapbacks: each sticker row is one reaction, and a sticker
   removal (3007) cannot say which sticker it removes, so it cancels none. #}
{% macro messages_standing_reactions(rows) -%}
    with tapbacks as (
        select distinct on (is_from_me, coalesce(sender_handle, ''), target_guid, target_part, reaction_kind) *
        from {{ rows }}
        where reaction_kind <> 'sticker'
        order by is_from_me, coalesce(sender_handle, ''), target_guid, target_part, reaction_kind,
            sent_at desc nulls last, kind = 'removal' desc, guid desc
    ),
    held as (
        select distinct on (is_from_me, coalesce(sender_handle, ''), target_guid, target_part) *
        from tapbacks
        where kind = 'reaction'
        order by is_from_me, coalesce(sender_handle, ''), target_guid, target_part,
            sent_at desc nulls last, guid desc
    )
    select * from held
    union all
    select * from {{ rows }} where reaction_kind = 'sticker' and kind = 'reaction'
{%- endmacro %}
