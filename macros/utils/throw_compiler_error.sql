{# Allows for testing of compiler errors #}
{# By disabling errors, only error message is returned without throwing compiler errors #}
{# WARNING snowplow__disable_errors should not be set to true in normal use #}

{% macro throw_compiler_error(error_message, disable_error=var("snowplow__disable_errors", false)) %}

  {% if disable_error %}

    {{ return(error_message) }}

  {% else %}

    {{ exceptions.raise_compiler_error(error_message) }}

  {% endif %}

{% endmacro %}
