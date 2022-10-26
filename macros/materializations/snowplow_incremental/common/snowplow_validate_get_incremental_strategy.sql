{% macro snowplow_validate_get_incremental_strategy(config) -%}
  {{ adapter.dispatch('snowplow_validate_get_incremental_strategy', 'snowplow_utils')(config) }}
{%- endmacro %}


{% macro default__snowplow_validate_get_incremental_strategy(config) %}
  
  {# Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}
  {# {{ print("strategy in theory: " ~ config.get("incremental_strategy", default="merge")) }} #}
  {{ print("strategy after set: " ~ strategy) }}
  
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
  
  {# Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}

{% endmacro %}
