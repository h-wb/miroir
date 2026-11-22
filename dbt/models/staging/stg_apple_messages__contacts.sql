{{ config(enabled=var('miroir_namespace', none) is not none) }}
-- Address book names matched to handles by the collector, when it sends them.
-- A name is the first and last name, else the nickname, else the organization.
with raw as (
    {{ optional_input('apple_messages', 'contacts', {
        'handle': 'text', 'first_name': 'text', 'last_name': 'text',
        'nickname': 'text', 'organization': 'text'
    }, ['handle']) }}
),
named as (
    select
        handle,
        coalesce(
            nullif(btrim(concat_ws(' ', btrim(first_name), btrim(last_name))), ''),
            nullif(btrim(nickname), ''),
            nullif(btrim(organization), '')
        ) as display_name
    from raw
)
select * from named where display_name is not null
