{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro parse_agg_dict(agg_dict) %}
  {{ return(adapter.dispatch('parse_agg_dict', 'snowplow_utils')(agg_dict)) }}
{% endmacro %}

{% macro default__parse_agg_dict(agg_dict) %}
  {% set agg_type = agg_dict.get('type') %}
  {% set agg_field = agg_dict.get('field') %}
  {% set agg_alias = agg_dict.get('alias') %}

  {% if agg_type not in ['sum', 'avg', 'min', 'max', 'count', 'countd'] or not agg_type %}
    {{ exceptions.raise_compiler_error(
      "Snowplow Error: Unexpected aggregation provided, must be one of sum, avg, min, max, count, countd (count distinct), not "~agg_type~"."
    ) }}
  {% endif %}
  {% if not agg_alias %}
    {{ exceptions.raise_compiler_error(
      "Snowplow Error: Alias must be provided for all aggregations."
    ) }}
  {% endif %}
  {% if agg_type == 'countd' %}
    count(distinct {{ agg_field }}) as {{ agg_alias }}
  {% else %}
    {{ agg_type }}({{ agg_field }}) as {{ agg_alias }}
  {% endif %}

{% endmacro %}
