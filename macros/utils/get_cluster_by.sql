{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_cluster_by(bigquery_cols=none, snowflake_cols=none) %}

  {%- do exceptions.warn("Warning: the `get_cluster_by` macro is deprecated and will be removed in a future version of the package, please use `get_value_by_target_type` instead.") -%}


  {% if target.type == 'bigquery' %}
    {{ return(bigquery_cols) }}
  {% elif target.type == 'snowflake' %}
    {{ return(snowflake_cols) }}
  {% endif %}

{% endmacro %}
