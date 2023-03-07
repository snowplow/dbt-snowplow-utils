{# string  -------------------------------------------------     #}

{%- macro type_string(max_characters) -%}
    {% if max_characters %}
        {{ return(adapter.dispatch('type_string', 'snowplow_utils')(max_characters)) }}
    {% else %}
        {{ return(adapter.dispatch('type_max_string', 'snowplow_utils')()) }}
    {% endif %}

{%- endmacro -%}

{% macro default__type_string(max_characters) %}
    {%- set error_message = "Warning: the `snowplow_utils.type_string(size)` macro is deprecated and should be replaced with dbt's `api.Column.string_type(size)`. It will be removed completely in a future version of the package." -%}
    {%- do exceptions.warn(error_message) -%}

    {{ api.Column.string_type(max_characters) }}
{% endmacro %}

{%- macro type_max_string() -%}
    {{ return(adapter.dispatch('type_max_string', 'snowplow_utils')()) }}
{%- endmacro -%}

{% macro default__type_max_string() %}
    {%- set error_message = "Warning: the `snowplow_utils.type_string()` macro is deprecated and should be replaced with dbt's `type_string()`. It will be removed completely in a future version of the package." -%}
    {%- do exceptions.warn(error_message) -%}
    {{ type_string() }}
{% endmacro %}
