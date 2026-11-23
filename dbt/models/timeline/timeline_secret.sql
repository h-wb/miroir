{{ config(
    enabled=var('miroir_namespace', none) is not none,
    materialized='incremental',
    unique_key='id',
    full_refresh=false,
    grants={'select': []},
    post_hook="{{ timeline_exact_acl(false) }}",
) }}
-- One row: a random key, made at the first build and kept. Conversation ids are
-- a keyed hash of the conversation (timeline_conversation_id), so they cannot be
-- found by hashing every phone number. Only the owner may read it: the reader
-- role has no access (an empty grant list makes dbt revoke).
--
-- full_refresh=false: a rebuild from scratch (dbt --full-refresh) keeps it, and
-- with it every conversation id. Dropping this table, or the schema (a new or
-- renamed analysis), makes new ids. A key comes from two random UUIDs (244
-- random bits, Postgres's own strong random source); a build that finds the row
-- gone makes a new one.
select
    1 as id,
    sha256(convert_to(gen_random_uuid()::text || gen_random_uuid()::text, 'UTF8')) as secret
{% if is_incremental() %}
where not exists (select 1 from {{ this }})
{% endif %}
