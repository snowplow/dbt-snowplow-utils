{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# returns tuple: (field_name, field_alias) #}
{% macro get_field_alias(field) %}

  {# Check if field is supplied as tuple e.g. (field_name, field_alias) #}
  {% if field is iterable and field is not string %}
    {{ return(field) }}
  {% else %}
    {{ return((field, field|replace('.', '_'))) }}
  {% endif %}

{% endmacro %}
