{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='guid',
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['guid'], 'unique': True},
        {'columns': ['_input']},
        {'columns': ['_stamp']},
    ],
    grants={'select': []},
    pre_hook="{{ timeline_remove_foreign(['messages']) }}",
    post_hook="{{ timeline_exact_acl(false) }}",
) }}
-- One row per Apple chat: the conversation it belongs to (decision 041's rule,
-- shared with the messages package: messages_chat_identifier,
-- messages_chat_is_group, messages_conversation_key), its opaque id, and what
-- the conversation's name is made of. Internal: the reader has no access, since
-- conversation_key can be a phone number.
--
-- Chats and their listed handles are snapshots the collector sends again when
-- they change, and they are small (hundreds of rows), so every build reads them
-- whole (rows of completed loads only) and writes the chats that differ. _stamp
-- moves only when a chat's conversation changes, or when it is new: that is what
-- makes messages read again the messages of that chat, and nothing else does. A
-- new title or a chat sent again unchanged reaches conversations, which is
-- rebuilt every build, and leaves the messages alone.
{% set stamp = timeline_new_stamp() %}
{% set incremental = is_incremental() and timeline_replaced(['messages']) | length == 0 %}
with completed as not materialized (
    {{ timeline_completed('apple_messages') }}
),
chats as (
    select ch.* from (
        {{ optional_input('apple_messages', 'chats', {
            'guid': 'text', 'chat_identifier': 'text', 'service_name': 'text',
            'display_name': 'text', 'style': 'bigint', 'room_name': 'text',
            '_dlt_load_id': 'text'
        }, ['guid']) }}
    ) ch
    where {{ timeline_completed_row('ch') }}
),
handles as (
    select h.* from (
        {{ optional_input('apple_messages', 'chat_handles', {
            'chat_guid': 'text', 'handle': 'text', '_dlt_load_id': 'text'
        }, ['chat_guid', 'handle']) }}
    ) h
    where h.handle <> '' and {{ timeline_completed_row('h') }}
),
listed as (
    select
        chat_guid,
        count(distinct handle) as handles,
        array_agg(distinct {{ messages_address('handle') }} order by {{ messages_address('handle') }}) as members
    from handles
    group by 1
),
shaped as (
    select
        c.guid,
        {{ messages_chat_identifier('c.guid', 'c.chat_identifier') }} as identifier,
        {{ messages_chat_is_group('c.style', 'c.room_name', 'l.handles') }} as is_group,
        nullif(btrim(c.display_name), '') as title,
        nullif(c.service_name, '') as service,
        coalesce(l.members, '{}'::text[]) as members,
        c._dlt_load_id as loaded
    from chats c
    left join listed l on l.chat_guid = c.guid
),
keyed as (
    select *, {{ messages_conversation_key('identifier', 'is_group') }} as conversation_key
    from shaped
),
derived as (
    select *, {{ timeline_conversation_id('conversation_key') }} as conversation_id
    from keyed
)
select
    d.guid,
    d.identifier,
    d.conversation_key,
    d.conversation_id,
    d.is_group,
    d.title,
    d.service,
    d.members,
    d.loaded,
    {{ timeline_connection('messages') }} as connection,
    {{ timeline_literal(timeline_input('messages')) }} as _input,
    {% if incremental %}
    case when t.conversation_id = d.conversation_id then t._stamp else {{ timeline_literal(stamp) }} end
    {% else %}{{ timeline_literal(stamp) }}{% endif %} as _stamp
from derived d
{% if incremental %}
left join {{ this }} t on t.guid = d.guid
where t.guid is null
    or (d.identifier, d.conversation_key, d.conversation_id, d.is_group, d.title, d.service, d.members, d.loaded)
        is distinct from (t.identifier, t.conversation_key, t.conversation_id, t.is_group, t.title, t.service, t.members, t.loaded)
{% endif %}
