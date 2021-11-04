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
