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
seen as (
    select distinct group_key as bundle_id
    from {{ ref('timeline_screen') }}
    where not deleted
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
