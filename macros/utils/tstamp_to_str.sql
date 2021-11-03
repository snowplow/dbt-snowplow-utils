{% macro tstamp_to_str(tstamp) -%}
  '{{ tstamp.strftime("%Y-%m-%d %H:%M:%S") }}'
{%- endmacro %}
