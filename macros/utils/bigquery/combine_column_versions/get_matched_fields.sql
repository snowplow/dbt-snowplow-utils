{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_matched_fields(fields, required_field_names, nested_level, level_filter) %}

  {% if not required_field_names|length %}

    {% if nested_level is none %}

      {% set matched_fields = fields %}

    {% else %}

      {% set matched_fields = fields|selectattr('nested_level',level_filter, nested_level)|list %}

    {% endif %}

  {% else %}

    {% set matched_fields = fields|selectattr('field_name','in', required_field_names)|list %}

  {% endif %}

  {{ return(matched_fields) }}

{% endmacro %}
