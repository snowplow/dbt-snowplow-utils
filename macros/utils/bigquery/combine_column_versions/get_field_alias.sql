{# returns tuple: (field_name, field_alias) #}
{% macro get_field_alias(field) %}
  
  {# Check if field is supplied as tuple e.g. (field_name, field_alias) #}
  {% if field is iterable and field is not string %}
    {{ return(field) }}
  {% else %}
    {{ return((field, field|replace('.', '_'))) }}
  {% endif %}
  
{% endmacro %}
