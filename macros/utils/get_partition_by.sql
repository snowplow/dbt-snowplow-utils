{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{%- macro get_partition_by(bigquery_partition_by=none, databricks_partition_by=none) -%}

  {%- do exceptions.warn("Warning: the `get_partition_by` macro is deprecated and will be removed in a future version of the package, please use `get_value_by_target_type` instead.") -%}

  {% if target.type == 'bigquery' %}
    {{ return(bigquery_partition_by) }}
  {% elif target.type in ['databricks', 'spark'] %}
    {{ return(databricks_partition_by) }}
  {% endif %}

{%- endmacro -%}
