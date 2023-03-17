{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro snowplow_validate_get_incremental_strategy(config) -%}
  {{ adapter.dispatch('snowplow_validate_get_incremental_strategy', 'snowplow_utils')(config) }}
{%- endmacro %}


{% macro default__snowplow_validate_get_incremental_strategy(config) %}

  {% if execute %}
    {%- set error_message = "Warning: the `snowplow_incremental` materialization is deprecated and should be replaced with dbt's `incremental` materialization, setting `snowplow_optimize=true` in your model config, and setting the appropriate dispatch search order in your project. See https://docs.snowplow.io//docs/modeling-your-data/modeling-your-data-with-dbt/dbt-advanced-usage/dbt-incremental-logic-pre-release for more details. The `snowplow_incremental` materialization will be removed completely in a future version of the package." -%}
    {%- do exceptions.warn(error_message) -%}
  {% endif %}

  {# Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}

  {# This shouldn't be required but due to some issue with dbt 1.3 this should resolve the default value not getting assigned #}
  {% if strategy is none %}
    {%- set strategy = 'merge' -%}
  {% endif %}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected 'merge'
  {%- endset %}
  {% if strategy not in ['merge'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}

{% endmacro %}


{% macro snowflake__snowplow_validate_get_incremental_strategy(config) %}

  {% if execute %}
    {%- set error_message = "Warning: the `snowplow_incremental` materialization is deprecated and should be replaced with dbt's `incremental` materialization, setting `snowplow_optimize=true` in your model config, and setting the appropriate dispatch search order in your project. See https://docs.snowplow.io//docs/modeling-your-data/modeling-your-data-with-dbt/dbt-advanced-usage/dbt-incremental-logic-pre-release for more details. The `snowplow_incremental` materialization will be removed completely in a future version of the package." -%}
    {%- do exceptions.warn(error_message) -%}
  {% endif %}

  {# Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}

  {# This shouldn't be required but due to some issue with dbt 1.3 this should resolve the default value not getting assigned #}
  {% if strategy is none %}
    {%- set strategy = 'merge' -%}
  {% endif %}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}

{% endmacro %}
