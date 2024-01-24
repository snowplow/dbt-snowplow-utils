{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
    Takes care of harmonising cross-db functions that create a string out of an array.
#}

{%- macro get_array_to_string(array_column, column_prefix, delimiter=',') -%}
    {{ return(adapter.dispatch('get_array_to_string', 'snowplow_utils')(array_column, column_prefix, delimiter)) }}
{%- endmacro -%}

{% macro default__get_array_to_string(array_column, column_prefix, delimiter=',') %}
    array_to_string({{column_prefix}}.{{array_column}},'{{delimiter}}')
{% endmacro %}

{% macro spark__get_array_to_string(array_column, column_prefix, delimiter=',') %}
    array_join({{column_prefix}}.{{array_column}},'{{delimiter}}')
{% endmacro %}
