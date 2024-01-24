{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
    Takes care of harmonising cross-db functions that create an array out of a string.
#}

{%- macro get_split_to_array(string_column, column_prefix, delimiter=',') -%}
    {{ return(adapter.dispatch('get_split_to_array', 'snowplow_utils')(string_column, column_prefix, delimiter)) }}
{%- endmacro -%}

{% macro default__get_split_to_array(string_column, column_prefix, delimiter=',') %}
    split({{column_prefix}}.{{string_column}}, '{{delimiter}}')
{% endmacro %}

{% macro redshift__get_split_to_array(string_column, column_prefix, delimiter=',') %}
    split_to_array({{column_prefix}}.{{string_column}}, '{{delimiter}}')
{% endmacro %}

{% macro postgres__get_split_to_array(string_column, column_prefix, delimiter=',') %}
    string_to_array({{column_prefix}}.{{string_column}}, '{{delimiter}}')
{% endmacro %}
