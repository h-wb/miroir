{{ config(
    enabled=var('miroir_namespace', none) is not none,
    grants=timeline_grants(),
    post_hook="{{ timeline_exact_acl(true) }}",
) }}
-- Bundle identifier to a readable name and a colour: the known list, and every app
-- seen in a screen session. An app the list lacks is named from its identifier.
with known as (
    select * from {{ ref('timeline_known_apps') }}
),
-- Every app of a session, one index probe per app rather than a read of every
-- session (index on deleted, group_key).
seen as (
    with recursive walk (bundle_id) as (
        (select group_key from {{ ref('timeline_screen') }}
         where not deleted and group_key is not null order by group_key limit 1)
        union all
        select (
            select s.group_key from {{ ref('timeline_screen') }} s
            where not s.deleted and s.group_key > w.bundle_id order by s.group_key limit 1
        )
        from walk w where w.bundle_id is not null
    )
    select bundle_id from walk where bundle_id is not null
),
ids as (
    select bundle_id from known
    union
    select bundle_id from seen
)
select
    i.bundle_id,
    {{ timeline_app_name('i.bundle_id', 'k.name') }} as name,
    {{ timeline_app_colour('i.bundle_id', 'k.colour') }} as colour,
    k.bundle_id is not null as known
from ids i
left join known k on k.bundle_id = i.bundle_id
