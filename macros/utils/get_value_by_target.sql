{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_value_by_target(dev_value, default_value, dev_target_name='dev') %}

  {% if target.name == dev_target_name %}
    {% set value = dev_value %}
  {% else %}
    {% set value = default_value %}
  {% endif %}

  {{ return(value) }}

{% endmacro %}
