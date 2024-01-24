{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_level_limit(level, level_filter, required_field_names) %}

  {% set accepted_level_filters = ['equalto','lessthan','greaterthan'] %}

  {% if level_filter is not in accepted_level_filters %}
    {% set incompatible_level_filter_error_message -%}
      Error: Incompatible level filter arg. Accepted args: {{accepted_level_filters|join(', ')}}
    {%- endset %}
    {{ return(snowplow_utils.throw_compiler_error(incompatible_level_filter_error_message)) }}
  {% endif %}

  {% if level is not none and required_field_names|length %}
    {% set double_filter_error_message -%}
      Error: Cannot filter fields by both `required_fields` and `level` arg. Please use only one.
    {%- endset %}
    {{ return(snowplow_utils.throw_compiler_error(double_filter_error_message)) }}
  {% endif %}

  {% if required_field_names|length and level_filter != 'equalto' %}
    {% set required_fields_error_message -%}
      Error: To filter fields using `required_fields` arg, `level_filter` must be set to `equalto`
    {%- endset %}
    {{ return(snowplow_utils.throw_compiler_error(required_fields_error_message)) }}
  {% endif %}

  {# level_limit is inclusive #}

  {% if level is not none %}

    {% if level_filter == 'equalto' %}

      {% set level_limit = level %}

    {% elif level_filter == 'lessthan' %}

      {% set level_limit = level -1  %}

    {% elif level_filter == 'greaterthan' %}

      {% set level_limit = none %}

    {% endif %}

  {% elif required_field_names|length %}

    {% set field_depths = [] %}
    {% for field in required_field_names %}
      {% set field_depth = field.split('.')|length %}
      {% do field_depths.append(field_depth) %}
    {% endfor %}

    {% set level_limit = field_depths|max %}

  {% else %}

    {# Case when selecting all available fields #}

    {% set level_limit = none %}

  {% endif %}

  {{ return(level_limit) }}

{% endmacro %}
