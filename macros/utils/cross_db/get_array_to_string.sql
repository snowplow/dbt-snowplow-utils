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
