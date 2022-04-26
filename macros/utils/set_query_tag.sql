{%- macro set_query_tag(statement) -%}
  {{ return(adapter.dispatch('set_query_tag', 'snowplow_utils')(statement)) }}
{%- endmacro -%}

{% macro snowflake__set_query_tag(statement) %}
    alter session set query_tag = '{{ statement }}';
{% endmacro %}

{% macro default__set_query_tag(statement) %}
    
{% endmacro %}
