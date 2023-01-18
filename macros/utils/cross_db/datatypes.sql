{# string  -------------------------------------------------     #}

{%- macro type_string(max_characters) -%}
    {% if max_characters %}
        {{ return(adapter.dispatch('type_string', 'snowplow_utils')(max_characters)) }}
    {% else %}
        {{ return(adapter.dispatch('type_max_string', 'snowplow_utils')()) }}
    {% endif %}

{%- endmacro -%}

{% macro default__type_string(max_characters) %}
    varchar( {{max_characters }} )
{% endmacro %}

{% macro bigquery__type_string(max_characters) %}
    string
{% endmacro %}

{% macro spark__type_string(max_characters) %}
    string
{% endmacro %}

{%- macro type_max_string() -%}
  {{ return(adapter.dispatch('type_max_string', 'snowplow_utils')()) }}
{%- endmacro -%}

{% macro default__type_max_string() %}
    string
{% endmacro %}

{% macro snowflake__type_max_string() %}
    varchar
{% endmacro %}

{% macro redshift__type_max_string() %}
    varchar(max)
{% endmacro %}

{% macro postgres__type_max_string() %}
    text
{% endmacro %}
