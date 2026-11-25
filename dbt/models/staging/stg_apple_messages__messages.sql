{{ config(enabled=var('miroir_namespace', none) is not none) }}
-- Every message row with what it is. The text itself goes no further than this
-- view: only its length leaves it.
--   kind: messages_row_kind, the rule both message packages share.
-- The source's handle is the other person in a direct chat for my own messages
-- too, so a sender is only ever set for messages from someone else. The sender
-- is the person (messages_address); sender_written keeps the handle as written.
-- A time before 2001-01-02 is no time (messages_dated).
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
        {{ messages_dated('sent_at') }} as dated_at,
        {{ messages_row_kind('item_type', 'associated_message_type') }} as kind,
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
    {{ messages_is_unsent('retracted_at') }} as is_unsent,
    {{ messages_is_edited('edited_at', 'retracted_at') }} as is_edited,
    coalesce(is_audio_message, false) as is_audio,
    case when kind in ('reaction', 'removal') then
        {{ messages_reaction_kind('associated_message_type') }}
    end as reaction_kind,
    case when kind in ('reaction', 'removal') then
        {{ messages_target_guid('associated_message_guid') }}
    end as target_guid,
    case when kind in ('reaction', 'removal') then
        {{ messages_target_part('associated_message_guid') }}
    end as target_part
from typed
