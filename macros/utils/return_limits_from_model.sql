{% macro return_limits_from_model(model, lower_limit_col, upper_limit_col) -%}

  {# In case of not execute just return empty strings to avoid hitting database #}
  {% if not execute %}
    {{ return(['','']) }}
  {% endif %}

  {% set limit_query %}
    select
      min({{lower_limit_col}}) as lower_limit,
      max({{upper_limit_col}}) as upper_limit
    from {{ model }}
    {% endset %}

  {% set results = run_query(limit_query) %}

  {% if execute %}

    {% set lower_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.cast_to_tstamp(results.columns[1].values()[0]) %}

  {{ return([lower_limit, upper_limit]) }}

  {% endif %}
{%- endmacro %}
