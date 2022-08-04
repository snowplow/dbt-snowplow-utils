{% macro return_limits_from_model(model, lower_limit_col, upper_limit_col) -%}

  {# In case of not execute just return empty strings to avoid hitting database #}
  {% if not execute %}
    {{ return(['','']) }}
  {% endif %}

  {% set target_relation = adapter.get_relation(
        database=model.database,
        schema=model.schema,
        identifier=model.name) %}

  {% if target_relation is not none %}

    {% set limit_query %}
      select
        min({{lower_limit_col}}) as lower_limit,
        max({{upper_limit_col}}) as upper_limit
      from {{ model }}
    {% endset %}

    {% set results = run_query(limit_query) %}

    {% if execute %}

      {# If there is no data within the limits, we should warn them otherwise they may be stuck here forever#}
      {%- if results.columns[0].values()[0] is none or results.columns[1].values()[0] is none -%}
      {# Currently warnings do not actually do anything other than text in logs, this makes it more visible https://github.com/dbt-labs/dbt-core/issues/6721 #}
        {{ snowplow_utils.log_message("Snowplow Warning: *************") }}
        {% do exceptions.warn("Snowplow Warning: No data in "~this~" for date range from variables, please modify your run variables to include data if this is not expected.") %}
        {{ snowplow_utils.log_message("Snowplow Warning: *************") }}
        {# This allows for bigquery to still run the same way the other warehouses do, but also ensures no data is processed #}
        {% set lower_limit = snowplow_utils.cast_to_tstamp('9999-01-01 00:00:00') %}
        {% set upper_limit = snowplow_utils.cast_to_tstamp('9999-01-02 00:00:00') %}
      {%- else -%}
        {% set lower_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}
        {% set upper_limit = snowplow_utils.cast_to_tstamp(results.columns[1].values()[0]) %}
      {%- endif -%}

      {{ return([lower_limit, upper_limit]) }}

    {% endif %}

  {% else %}

    {% do exceptions.warn("Snowplow Warning: " ~ model ~ " does not exist. This is expected if you are compiling a fresh installation of the dbt-snowplow-* packages.") %}
    {% set dummy_limit = snowplow_utils.cast_to_tstamp('9999-01-01 00:00:00') %}

    {{ return([dummy_limit, dummy_limit]) }}

  {% endif %}


{% endmacro %}
