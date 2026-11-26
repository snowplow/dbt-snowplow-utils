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

  {%- if not names -%}
    true
  {%- else -%}
    (
      {%- if vendor and vendor_names is not none %}
      lower(event_name) in ({{ snowplow_utils.event_filter_literals(names + vendor_names) }}) and
      {%- endif %}
      (lower(event_name) in ({{ snowplow_utils.event_filter_literals(names) }})
       {%- if vendor %} or lower(event_vendor) = {{ snowplow_utils.event_filter_literals([vendor]) }}{% endif %})
    )
  {%- endif -%}
{% endmacro %}

{% macro validate_event_filter(event_filter) %}
  {%- if event_filter is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- if event_filter is not mapping -%}
    {{ exceptions.raise_compiler_error("snowplow_utils: event_filter must be a mapping, for example {'event_names': ['page_view']}. Got: " ~ event_filter) }}
  {%- endif -%}
  {%- for key in ['event_names', 'vendor_event_names'] -%}
    {%- set values = event_filter.get(key) -%}
    {%- if values is not none -%}
      {%- if values is string or values is mapping or values is not iterable -%}
        {{ exceptions.raise_compiler_error("snowplow_utils: event_filter." ~ key ~ " must be a list of event names, for example ['page_view']. Got: " ~ values) }}
      {%- endif -%}
      {%- for value in values -%}
        {%- if value is not string -%}
          {{ exceptions.raise_compiler_error("snowplow_utils: event_filter." ~ key ~ " must contain only strings. Got: " ~ values) }}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endfor -%}
  {%- set vendor = event_filter.get('event_vendor') -%}
  {%- if vendor is not none and vendor is not string -%}
    {{ exceptions.raise_compiler_error("snowplow_utils: event_filter.event_vendor must be a single vendor string. Got: " ~ vendor) }}
  {%- endif -%}
{% endmacro %}

{% macro event_filter_literals(values) %}
  {%- set literals = [] -%}
  {%- for value in values -%}
    {%- set value = value | lower -%}
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
