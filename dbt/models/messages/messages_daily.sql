{{ config(alias='daily', enabled=var('miroir_namespace', none) is not none) }}
-- One row per local day from the first message or reaction to the last,
-- including days with none.
with messages as (
    select day, is_from_me, conversation_key from {{ ref('messages_messages') }}
),
reactions as (
    select day from {{ ref('messages_reactions') }}
),
days as (
    select generate_series(min(day), max(day), interval '1 day')::date as day
    from (select day from messages union all select day from reactions) d
),
sent as (
    select
        day,
        count(*) filter (where is_from_me) as messages_sent,
        count(*) filter (where not is_from_me) as messages_received,
        count(distinct conversation_key) as conversations_active
    from messages
    group by 1
),
attachments as (
    select day, count(*) as attachments from {{ ref('messages_attachments') }} group by 1
),
reacted as (
    select day, count(*) as reactions from reactions group by 1
)
select
    d.day,
    extract(isodow from d.day)::int as weekday,
    coalesce(s.messages_sent, 0) as messages_sent,
    coalesce(s.messages_received, 0) as messages_received,
    coalesce(s.conversations_active, 0) as conversations_active,
    coalesce(a.attachments, 0) as attachments,
    coalesce(r.reactions, 0) as reactions
from days d
left join sent s using (day)
left join attachments a using (day)
left join reacted r using (day)
