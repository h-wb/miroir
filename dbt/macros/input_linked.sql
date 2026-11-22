{# Whether an analysis input role is linked (decision 022). Packages whose inputs
   are all required pass no miroir_inputs: every role is linked. Measures of an
   unlinked input are unknown, so models render null instead of zero for them. #}
{% macro input_linked(role) -%}
    {{- return(var('miroir_inputs', none) is none or role in var('miroir_inputs')) -}}
{%- endmacro %}

{% macro if_linked(role, expression, type) -%}
    {%- if input_linked(role) -%}{{ expression }}{%- else -%}null::{{ type }}{%- endif -%}
{%- endmacro %}
