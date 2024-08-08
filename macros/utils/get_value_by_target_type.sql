{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{%- macro get_value_by_target_type(bigquery_val=none, snowflake_val=none, redshift_val=none, postgres_val=none, databricks_val=none, spark_val=none) -%}

  {% if target.type == 'bigquery' %}
    {{ return(bigquery_val) }}
  {% elif target.type == 'snowflake' %}
    {{ return(snowflake_val) }}
  {% elif target.type == 'redshift' %}
    {{ return(redshift_val) }}
  {% elif target.type == 'postgres' %}
    {{ return(postgres_val) }}
  {% elif target.type in ['databricks'] %}
    {{ return(databricks_val) }}
    {% elif target.type in ['spark'] %}
    {{ return(spark_val) }}
  {% else %}
    {{ exceptions.raise_compiler_error("Snowplow: Unexpected target type "~target.type) }}
  {% endif %}

{%- endmacro -%}
