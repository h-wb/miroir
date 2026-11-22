{{ config(enabled=var('miroir_namespace', none) is not none) }}
-- Every message row with what it is. The text itself goes no further than this
-- view: only its length leaves it.
--   kind: 'message' (item_type 0, associated_message_type 0, 2 or 3, or null),
--   'reaction' (2000-2007, and 1000: a sticker placed on a message), 'removal'
--   (3000-3007), 'event' (any other item_type: group changes, FaceTime, shared
--   location, kept audio), 'other' (any other associated type, such as 4000,
--   a poll vote).
-- The source's handle is the other person in a direct chat for my own messages
-- too, so a sender is only ever set for messages from someone else. The sender
-- is the person (messages_address); sender_written keeps the handle as written.
-- A time before 2001-01-02 is no time: Apple's clock starts at 2001-01-01, and
-- a date of 0 or close to it is a missing date, not a message from 2001.
with raw as (
    {{ optional_input('apple_messages', 'messages', {
        'guid': 'text', 'text': 'text', 'text_source': 'text', 'service': 'text',
        'handle': 'text', 'is_from_me': 'boolean', 'sent_at': 'timestamptz',
        'edited_at': 'timestamptz', 'retracted_at': 'timestamptz',
        'item_type': 'bigint', 'associated_message_type': 'bigint',
        'associated_message_guid': 'text', 'thread_originator_guid': 'text',
        'is_audio_message': 'boolean'
    }, ['guid']) }}
),
typed as (
    select
        *,
        coalesce(is_from_me, false) as from_me,
        case when sent_at >= timestamptz '2001-01-02 00:00:00+00' then sent_at end as dated_at,
        case
            when coalesce(item_type, 0) <> 0 then 'event'
            when associated_message_type = 1000
                or associated_message_type between 2000 and 2007 then 'reaction'
            when associated_message_type between 3000 and 3007 then 'removal'
            when coalesce(associated_message_type, 0) in (0, 2, 3) then 'message'
            else 'other'
        end as kind,
        -- U+FFFC stands in the text for each attachment; it is not text.
        case when text_source <> 'unreadable'
            then btrim(replace(coalesce(text, ''), chr(65532), '')) end as visible_text
    from raw
)
select
    guid,
    kind,
    dated_at as sent_at,
    from_me as is_from_me,
    case when not from_me and handle <> '' then {{ messages_address('handle') }} end as sender_handle,
    case when not from_me then nullif(handle, '') end as sender_written,
    service,
    -- Unknown, not zero, when the collector could not read the text archive.
    visible_text <> '' as has_text,
    char_length(visible_text) as text_length,
    thread_originator_guid is not null as is_thread_reply,
    retracted_at is not null as is_unsent,
    -- An unsent message can carry an edit time as well; it counts as unsent only.
    edited_at is not null and retracted_at is null as is_edited,
    coalesce(is_audio_message, false) as is_audio,
    case when kind in ('reaction', 'removal') then
        case
            when associated_message_type = 1000 then 'sticker'
            when associated_message_type % 1000 = 0 then 'love'
            when associated_message_type % 1000 = 1 then 'like'
            when associated_message_type % 1000 = 2 then 'dislike'
            when associated_message_type % 1000 = 3 then 'laugh'
            when associated_message_type % 1000 = 4 then 'emphasis'
            when associated_message_type % 1000 = 5 then 'question'
            when associated_message_type % 1000 = 6 then 'emoji'
            when associated_message_type % 1000 = 7 then 'sticker'
        end
    end as reaction_kind,
    -- 'p:<part>/<guid>' targets one part of a message, 'bp:<guid>' a link
    -- preview or app bubble (part 0); a bare guid is taken as it is.
    case when kind in ('reaction', 'removal') then
        nullif(regexp_replace(associated_message_guid, '^(p:[0-9]+/|bp:)', ''), '')
    end as target_guid,
    case when kind in ('reaction', 'removal') then
        coalesce(substring(associated_message_guid from '^p:([0-9]+)/')::int, 0)
    end as target_part
from typed
