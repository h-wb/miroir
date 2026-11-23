{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key=['reaction_guid', 'message_guid'],
    incremental_strategy='timeline_upsert',
    indexes=[
        {'columns': ['reaction_guid', 'message_guid'], 'unique': True},
        {'columns': ['message_guid']},
    ],
    grants={'select': []},
    post_hook="{{ timeline_exact_acl(false) }}",
) }}
-- Which messages each reaction or removal row is aimed at: messages'
-- _reactions_from, turned around and indexed by the reaction's guid, so a row
-- sent again aimed elsewhere, or as something else, finds the message it left
-- by one index probe. messages writes it in its own transaction (post-hook
-- timeline_message_reactions_map); this model only creates it, empty, before
-- messages. Internal: the reader has no access.
select null::text as reaction_guid, null::text as message_guid
where false
