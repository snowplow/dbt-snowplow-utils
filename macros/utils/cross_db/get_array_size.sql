{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
    Takes care of harmonising cross-db functions that create a string out of an array.
#}

{%- macro get_array_size(array_column) -%}
    {{ return(adapter.dispatch('get_array_size', 'snowplow_utils')(array_column)) }}
{%- endmacro -%}

{% macro default__get_array_size(array_column) %}
    array_size({{array_column}})
{% endmacro %}

{% macro bigquery__get_array_size(array_column) %}
  array_length({{array_column}})
{% endmacro %}

{% macro postgres__get_array_size(array_column) %}
    array_length({{array_column}})
{% endmacro %}

{% macro redshift__get_array_size(array_column) %}
  get_array_length({{array_column}})
{% endmacro %}
