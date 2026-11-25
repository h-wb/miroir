{{ config(enabled=var('miroir_namespace', none) is not none) }}
-- Address book names matched to handles by the collector, when it sends them,
-- by person (messages_address). A name is the first and last name, else the
-- nickname, else the organization; handles of one person that were given
-- different names take the first in order.
with raw as (
    {{ optional_input('apple_messages', 'contacts', {
        'handle': 'text', 'first_name': 'text', 'last_name': 'text',
        'nickname': 'text', 'organization': 'text'
    }, ['handle']) }}
),
named as (
    select
        {{ messages_address('handle') }} as address,
        {{ messages_contact_name('first_name', 'last_name', 'nickname', 'organization') }} as display_name
    from raw
)
select address, min(display_name) as display_name
from named
where display_name is not null
group by 1
