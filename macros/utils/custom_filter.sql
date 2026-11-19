{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Returns a caller-supplied SQL predicate wrapped in parentheses, after the same safety check as base_create_snowplow_events_this_run_t #}
{% macro custom_filter(custom_filter) %}
  {%- set disallowed = [';', '--', '/*', '*/', 'drop', 'delete', 'alter', 'insert', 'truncate'] -%}
  {%- for item in disallowed -%}
    {%- if item in custom_filter | lower -%}
      {{ exceptions.raise_compiler_error("Unsafe SQL detected in custom_filter: contains '" ~ item ~ "'") }}
    {%- endif -%}
  {%- endfor -%}
  ({{ custom_filter }})
{%- endmacro %}
