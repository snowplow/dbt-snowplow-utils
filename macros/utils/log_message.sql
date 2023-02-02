{% macro log_message(message, is_printed=var('snowplow__has_log_enabled', true)) %}
    {{ log(dbt_utils.pretty_log_format(message), info=is_printed) }}
{% endmacro %}
