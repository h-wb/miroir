{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='guid',
    keep_cursor=true,
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['guid'], 'unique': True},
        {'columns': ['_input']},
        {'columns': ['_stamp']},
        {'columns': ['_cursor']},
        {'columns': ['timezone']},
        {'columns': ['deleted']},
        {'columns': ['conversation_id', 'sent_at', 'guid']},
        {'columns': ['conversation_id', 'sender_handle']},
        {'columns': ['sender_handle', 'sender_written']},
        {'columns': ['(' ~ timeline_search_vector('"text"') ~ ')'], 'type': 'gin'},
    ],
    grants=timeline_grants(),
    pre_hook=[
        {'sql': "{{ timeline_raw_indexes('apple_messages', {
            'messages': ['_dlt_load_id', 'guid', ('target', 'associated_message_guid', messages_target_expression('associated_message_guid'))],
            'chat_messages': ['_dlt_load_id', 'message_guid', 'chat_guid'],
            'message_attachments': ['_dlt_load_id', 'message_guid', 'attachment_guid'],
            'attachments': ['_dlt_load_id', 'guid', 'media_key'],
            'media_objects': ['_dlt_load_id', 'key'],
            '_dlt_loads': ['load_id', 'inserted_at']}) }}", 'transaction': false},
        "{{ timeline_messages_plan() }}",
        "{{ timeline_message_reactions_reset() }}",
        "{{ timeline_remove_foreign(['messages']) }}",
        "{{ timeline_search_index('text') }}",
    ],
    post_hook=[
        "{{ timeline_keep_cursor(this, this) if not is_incremental() else '' }}",
        "{{ timeline_message_reactions_map() }}",
        "{{ timeline_exact_acl(true) }}",
    ],
) }}
-- One row per Apple message (decision 041's rule: messages_row_kind; not
-- reactions, not group events), with its text as the collector delivered it,
-- masked on the device (nothing is masked or unmasked here). For a conversation
-- view, a message search and the Messages lane of events.
--   conversation_id: the opaque id of its conversation (timeline_message_chats),
--     the first conversation key in order when it is in chats of two; null when
--     it is in no known chat.
--   sender_handle: the person (messages_address) for a message from someone
--     else; null for mine. sender_written: the handle as this message writes it.
--     The sender's name is people.name, else sender_written: names resolve
--     through `people`, never stored here, so a renamed contact needs no message
--     rewritten.
--   day: the local day in the analysis's timezone (timeline_local_day).
--   attachments: in order (creation time, then transfer name, then guid: the
--     database keeps no other order the collector sends), link previews left out,
--     each with what the worker recorded of its file in media_objects; null
--     there means no record, which is unknown.
--   reactions: the ones that stand (messages_standing_reactions), oldest first.
--     _reactions_from lists every reaction and removal row aimed at it; the
--     post-hook keeps timeline_message_reaction_targets, the same turned around,
--     so a row sent again aimed elsewhere, or as something else, recomputes it.
--   deleted: a guid this table held as a message whose row is now something
--     else. It has no conversation, time or text, so no conversation or search
--     query meets it; events drops it and days' post-hook removes it.
-- Search queries must use the indexed expression, timeline_search_vector('text'):
-- the first 100,000 characters.
--
-- Only rows of loads dlt lists as complete are read (timeline_load_cursor). An
-- incremental build recomputes the messages that rows of the loads completed
-- since its cursor name or touch: a message sent again (an edit, an unsend, a
-- restore from another Mac), a new link to a chat, a reaction or removal (its
-- target), an attachment or its link, a media record (media_ref; its
-- attachment's message), and every message of a chat whose conversation changed
-- (timeline_message_chats' _stamp, read since _chats_seen). Each is looked up by
-- index on the raw tables. A message that moved keeps its previous times in
-- previous_sent_at until days has read the row (as timeline_previous).
{% set files = media_ref('messages', 'attachments', 'a.media_key') %}
{% set replaced = timeline_replaced(['messages']) | length > 0 %}
{% set cursor = timeline_load_cursor('apple_messages', replaced or timeline_messages_retimed()) %}
{% set stamp = timeline_new_stamp() %}
{% set previous = is_incremental() and not replaced %}
{% set consumed = timeline_consumed([('days', "source = 'messages'")]) %}
{% set chats_upper = timeline_resume(ref('timeline_message_chats')) %}
{% set chats_lower = timeline_scalar('select max(_chats_seen) from ' ~ this) if previous else '' %}
{% set target = messages_target_expression('x.associated_message_guid') %}
with completed as not materialized (
    {{ timeline_completed('apple_messages') }}
),
raw as not materialized (
    select r.*, {{ messages_row_kind('r.item_type', 'r.associated_message_type') }} as kind
    from (
        {{ optional_input('apple_messages', 'messages', {
            'guid': 'text', 'text': 'text', 'text_source': 'text', 'service': 'text',
            'handle': 'text', 'is_from_me': 'boolean', 'sent_at': 'timestamptz',
            'edited_at': 'timestamptz', 'retracted_at': 'timestamptz',
            'item_type': 'bigint', 'associated_message_type': 'bigint',
            'associated_message_guid': 'text', 'thread_originator_guid': 'text',
            'is_audio_message': 'boolean', '_dlt_load_id': 'text'
        }, ['guid']) }}
    ) r
),
links as not materialized (
    {{ optional_input('apple_messages', 'chat_messages', {
        'chat_guid': 'text', 'message_guid': 'text', '_dlt_load_id': 'text'
    }, ['chat_guid', 'message_guid']) }}
),
attachment_links as not materialized (
    {{ optional_input('apple_messages', 'message_attachments', {
        'message_guid': 'text', 'attachment_guid': 'text', '_dlt_load_id': 'text'
    }, ['message_guid', 'attachment_guid']) }}
),
attachments as not materialized (
    {{ optional_input('apple_messages', 'attachments', {
        'guid': 'text', 'transfer_name': 'text', 'mime_type': 'text',
        'total_bytes': 'bigint', 'media_key': 'text', 'created_at': 'timestamptz',
        '_dlt_load_id': 'text'
    }, ['guid']) }}
),
-- This connection's attachment files only; the dataset could hold another's.
media as not materialized (
    {{ files.records }}
),
chats as not materialized (
    select guid, conversation_key, conversation_id, _stamp from {{ ref('timeline_message_chats') }}
),
{% if not cursor.rebuild %}
-- Every guid whose row this build writes.
touched as (
    select guid from raw where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
    union
    select {{ messages_target_guid('associated_message_guid') }} from raw
    where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }} and kind in ('reaction', 'removal')
    union
    select message_guid from links where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
    union
    select l.message_guid from chats ch
    cross join lateral (
        select l.message_guid from links l where l.chat_guid = ch.guid and {{ timeline_completed_row('l') }}
    ) l
    where ch._stamp > {{ timeline_literal(chats_lower) }}
    union
    select message_guid from attachment_links where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
    union
    select al.message_guid from attachments a
    cross join lateral (
        select al.message_guid from attachment_links al
        where al.attachment_guid = a.guid and {{ timeline_completed_row('al') }}
    ) al
    where {{ timeline_in_loads('a._dlt_load_id', cursor.loads) }}
    union
    select al.message_guid from (
        select distinct key from media where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
    ) m
    cross join lateral (
        select a.guid from attachments a where a.media_key = m.key and {{ timeline_completed_row('a') }}
    ) a
    cross join lateral (
        select al.message_guid from attachment_links al
        where al.attachment_guid = a.guid and {{ timeline_completed_row('al') }}
    ) al
    union
    -- A reaction or removal sent again, aimed elsewhere or as something else:
    -- the message it was aimed at before (one probe per row of the new loads).
    select t.message_guid from {{ ref('timeline_message_reaction_targets') }} t
    where t.reaction_guid in (
        select guid from raw where {{ timeline_in_loads('_dlt_load_id', cursor.loads) }}
    )
),
picked as (
    select r.* from touched t
    cross join lateral (
        select * from raw r where r.guid = t.guid and {{ timeline_completed_row('r') }} limit 1
    ) r
),
messages_picked as (
    select * from picked where kind = 'message'
),
message_links as (
    select p.guid, ch.conversation_key, ch.conversation_id, ch.guid as chat_guid
    from messages_picked p
    cross join lateral (
        select l.chat_guid from links l where l.message_guid = p.guid and {{ timeline_completed_row('l') }}
    ) l
    join chats ch on ch.guid = l.chat_guid
),
message_attachments as (
    select p.guid as message_guid, a.*
    from messages_picked p
    cross join lateral (
        select al.attachment_guid from attachment_links al
        where al.message_guid = p.guid and {{ timeline_completed_row('al') }}
    ) al
    cross join lateral (
        select * from attachments a where a.guid = al.attachment_guid and {{ timeline_completed_row('a') }} limit 1
    ) a
),
reaction_raw as (
    select x.*
    from messages_picked p
    cross join lateral (
        select * from raw x
        where {{ target }} = p.guid and x.kind in ('reaction', 'removal') and {{ timeline_completed_row('x') }}
    ) x
),
{% else %}
picked as (
    select * from raw r where {{ timeline_completed_row('r') }}
),
messages_picked as (
    select * from picked where kind = 'message'
),
message_links as (
    select l.message_guid as guid, ch.conversation_key, ch.conversation_id, ch.guid as chat_guid
    from links l
    join chats ch on ch.guid = l.chat_guid
    where {{ timeline_completed_row('l') }}
),
message_attachments as (
    select al.message_guid, a.*
    from attachment_links al
    join attachments a on a.guid = al.attachment_guid
    where {{ timeline_completed_row('al') }} and {{ timeline_completed_row('a') }}
),
reaction_raw as (
    select * from picked where kind in ('reaction', 'removal')
),
{% endif %}
conversations_of as (
    select guid, (array_agg(conversation_id order by conversation_key, chat_guid))[1] as conversation_id
    from message_links
    group by 1
),
attachment_items as (
    select
        a.message_guid,
        jsonb_agg(jsonb_build_object(
            'guid', a.guid,
            'media_key', a.media_key,
            'kind', {{ messages_attachment_kind('a.mime_type', messages_attachment_extension('a.media_key', 'a.transfer_name')) }},
            'mime_type', a.mime_type,
            'bytes', a.total_bytes,
            'transfer_name', a.transfer_name,
            'media_status', m.thumbnail_status,
            'thumbnail_w360', m.thumbnail_w360,
            'thumbnail_w1280', m.thumbnail_w1280
        ) order by a.created_at nulls last, a.transfer_name nulls last, a.guid) as items
    from message_attachments a
    left join lateral {{ files.record }} m on true
    where not {{ messages_is_link_preview('a.transfer_name') }}
    group by 1
),
reaction_rows as (
    select
        x.guid,
        x.kind,
        coalesce(x.is_from_me, false) as is_from_me,
        case when not coalesce(x.is_from_me, false) and x.handle <> ''
            then {{ messages_address('x.handle') }} end as sender_handle,
        {{ messages_target_guid('x.associated_message_guid') }} as target_guid,
        {{ messages_target_part('x.associated_message_guid') }} as target_part,
        {{ messages_reaction_kind('x.associated_message_type') }} as reaction_kind,
        {{ messages_dated('x.sent_at') }} as sent_at
    from reaction_raw x
),
reaction_targets as (
    select * from reaction_rows where target_guid is not null
),
standing as (
    {{ messages_standing_reactions('reaction_targets') }}
),
reaction_items as (
    select
        target_guid as message_guid,
        jsonb_agg(jsonb_build_object(
            'kind', reaction_kind, 'from_me', is_from_me, 'sender', sender_handle,
            'part', target_part, 'at', {{ timeline_utc_text('sent_at') }}
        ) order by sent_at nulls last, guid) as items
    from standing
    group by 1
),
reaction_sources as (
    select target_guid as message_guid, array_agg(distinct guid order by guid) as guids
    from reaction_targets
    group by 1
),
current_messages as (
    select
        p.guid,
        co.conversation_id,
        {{ messages_dated('p.sent_at') }} as sent_at,
        coalesce(p.is_from_me, false) as is_from_me,
        case when not coalesce(p.is_from_me, false) and p.handle <> ''
            then {{ messages_address('p.handle') }} end as sender_handle,
        case when not coalesce(p.is_from_me, false) then nullif(p.handle, '') end as sender_written,
        p.service,
        p.text,
        coalesce(p.text_source = 'unreadable', false) as text_unreadable,
        {{ messages_is_edited('p.edited_at', 'p.retracted_at') }} as is_edited,
        {{ messages_is_unsent('p.retracted_at') }} as is_unsent,
        p.edited_at,
        coalesce(p.is_audio_message, false) as is_audio,
        p.thread_originator_guid as reply_to_guid,
        coalesce(ai.items, '[]'::jsonb) as attachments,
        coalesce(ri.items, '[]'::jsonb) as reactions,
        coalesce(rs.guids, '{}'::text[]) as reactions_from
    from messages_picked p
    left join conversations_of co on co.guid = p.guid
    left join attachment_items ai on ai.message_guid = p.guid
    left join reaction_items ri on ri.message_guid = p.guid
    left join reaction_sources rs on rs.message_guid = p.guid
)
select
    m.guid,
    m.conversation_id,
    m.sent_at,
    {{ timeline_local_day('m.sent_at') }} as day,
    {{ timeline_literal(var('timezone')) }} as timezone,
    m.is_from_me,
    m.sender_handle,
    m.sender_written,
    m.service,
    m.text,
    m.text_unreadable,
    m.is_edited,
    m.is_unsent,
    m.edited_at,
    m.is_audio,
    m.reply_to_guid,
    m.attachments,
    m.reactions,
    false as deleted,
    {{ timeline_connection('messages') }} as connection,
    {% if previous %}
    {{ timeline_previous_moved('previous_sent_at', 't.sent_at', 'm.sent_at', consumed) }}
    {% else %}null::timestamptz[]{% endif %} as previous_sent_at,
    {{ timeline_literal(timeline_input('messages')) }} as _input,
    nullif({{ timeline_literal(cursor.cursor) }}, '')::timestamptz as _cursor,
    {{ timeline_literal(stamp) }} as _stamp,
    {{ timeline_literal(chats_upper) }} as _chats_seen,
    m.reactions_from as _reactions_from
    {% if is_incremental() %}, false as _deleted{% endif %}
from current_messages m
{% if previous %}
-- One index probe per message: a plain join was planned as a read of them all.
left join lateral (select * from {{ this }} o where o.guid = m.guid limit 1) t on true
union all
select
    t.guid,
    null::text, null::timestamptz, null::date, {{ timeline_literal(var('timezone')) }},
    t.is_from_me, null::text, null::text, null::text, null::text, false, false, false,
    null::timestamptz, false, null::text, '[]'::jsonb, '[]'::jsonb,
    true as deleted,
    t.connection,
    {{ timeline_previous_moved('previous_sent_at', 't.sent_at', 'null::timestamptz', consumed) }},
    t._input,
    nullif({{ timeline_literal(cursor.cursor) }}, '')::timestamptz,
    {{ timeline_literal(stamp) }},
    {{ timeline_literal(chats_upper) }},
    '{}'::text[],
    false
from picked p
cross join lateral (
    select * from {{ this }} o where o.guid = p.guid and not o.deleted limit 1
) t
where p.kind <> 'message'
{% endif %}
{% if is_incremental() %}
union all
-- The cursor this build read, kept even when it writes no message
-- (keep_cursor); never inserted, and its null guid matches no row's.
select
    null::text, null::text, null::timestamptz, null::date, null::text, false, null::text,
    null::text, null::text, null::text, false, false, false, null::timestamptz,
    false, null::text, '[]'::jsonb, '[]'::jsonb, true, null::text,
    null::timestamptz[], null::text,
    nullif({{ timeline_literal(cursor.cursor) }}, '')::timestamptz,
    {{ timeline_literal(stamp) }}, {{ timeline_literal(chats_upper) }}, '{}'::text[],
    true
{% endif %}
