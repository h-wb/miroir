{# The messages package's rule for one person's number or address, the same rule
   the collector uses to match a handle to a contact (decision 041): an address
   (it holds '@') in lower case; a number as its digits, a leading 1 dropped from
   eleven; anything else, such as a short code, as written. Groups never use it. #}
{% macro messages_address(value) -%}
    {%- set digits = "regexp_replace(" ~ value ~ ", '[^0-9]', '', 'g')" -%}
    case
        when {{ value }} like '%@%' then lower({{ value }})
        when length({{ digits }}) = 11 and {{ digits }} like '1%' then substr({{ digits }}, 2)
        when length({{ digits }}) >= 7 then {{ digits }}
        else {{ value }}
    end
{%- endmacro %}
