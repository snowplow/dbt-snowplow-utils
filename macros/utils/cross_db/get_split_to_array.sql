{#
 Takes care of harmonising cross-db functions that create an array out of a string.
 #}

{%- macro get_split_to_array(string_column, column_prefix) -%}
    {{ return(adapter.dispatch('get_split_to_array', 'snowplow_utils')(string_column, column_prefix)) }}
{%- endmacro -%}

{% macro default__get_split_to_array(string_column, column_prefix) %}
   split({{column_prefix}}.{{string_column}}, ',')
{% endmacro %}

{% macro redshift__get_split_to_array(string_column, column_prefix) %}
    split_to_array({{column_prefix}}.{{string_column}})
{% endmacro %}

{% macro postgres__get_split_to_array(string_column, column_prefix) %}
    string_to_array({{column_prefix}}.{{string_column}}, ',')
{% endmacro %}
