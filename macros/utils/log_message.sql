{# Logs to console, with option to disable. #}
{% macro log_message(message, is_printed=var('snowplow__has_log_enabled', true)) %}
    {{ return(adapter.dispatch('log_message', 'snowplow_utils')(message, is_printed)) }}
{% endmacro %}

{% macro default__log_message(message, is_printed) %}
    {{ log(dbt_utils.pretty_log_format(message), info=is_printed) }}
{% endmacro %}
