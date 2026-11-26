{#
Copyright (c) 2026-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro event_scan_filter(event_filter) %}
  {%- set names = event_filter.get('event_names', []) -%}
  {%- set vendor = event_filter.get('event_vendor') -%}
  {%- set vendor_names = event_filter.get('vendor_event_names') -%}
  {%- set case_sensitive = event_filter.get('case_sensitive', false) -%}
  {%- set name_column = 'event_name' if case_sensitive else 'lower(event_name)' -%}

  {%- if not names -%}
    true
  {%- else -%}
    (
      {%- if vendor and vendor_names is not none %}
      {{ name_column }} in ({{ snowplow_utils.event_filter_literals(names + vendor_names, case_sensitive) }}) and
      {%- endif %}
      ({{ name_column }} in ({{ snowplow_utils.event_filter_literals(names, case_sensitive) }})
       {%- if vendor %} or lower(event_vendor) = {{ snowplow_utils.event_filter_literals([vendor], false) }}{% endif %})
    )
  {%- endif -%}
{% endmacro %}

{% macro event_filter_literals(values, case_sensitive) %}
  {%- set literals = [] -%}
  {%- for value in values -%}
    {%- set value = value if case_sensitive else value | lower -%}
    {%- if target.type in ['bigquery', 'snowflake', 'spark', 'databricks'] -%}
      {%- set value = value.replace('\\', '\\\\') -%}
    {%- endif -%}
    {%- if target.type in ['spark', 'databricks'] -%}
      {%- set escaped = value.replace("'", "\\'") -%}
    {%- else -%}
      {%- set escaped = dbt.escape_single_quotes(value) -%}
    {%- endif -%}
    {%- do literals.append(dbt.string_literal(escaped)) -%}
  {%- endfor -%}
  {{- return(literals | join(', ')) -}}
{% endmacro %}
