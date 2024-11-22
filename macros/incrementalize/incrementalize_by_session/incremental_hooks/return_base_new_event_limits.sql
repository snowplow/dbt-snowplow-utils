{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro return_base_new_event_limits(base_events_this_run) -%}

  {# In case of not execute just return empty strings to avoid hitting database #}
  {% if not execute %}
    {{ return(['','',''])}}
  {% endif %}

  {% set target_relation = adapter.get_relation(
        database=base_events_this_run.database,
        schema=base_events_this_run.schema,
        identifier=base_events_this_run.name) %}

  {% if target_relation is not none %}

    {% set limit_query %}
      select
        lower_limit,
        upper_limit,
        {{ snowplow_utils.timestamp_add('day',
                                      -var("snowplow__max_session_days", 3),
                                      'lower_limit') }} as session_start_limit

      from {{ base_events_this_run }}
    {% endset %}

    {% set results = run_query(limit_query) %}

    {% if execute %}

      {% set lower_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}
      {% set upper_limit = snowplow_utils.cast_to_tstamp(results.columns[1].values()[0]) %}
      {% set session_start_limit = snowplow_utils.cast_to_tstamp(results.columns[2].values()[0]) %}

      {{ return([lower_limit, upper_limit, session_start_limit]) }}

    {% endif %}

  {% else %}

    {% do exceptions.warn("Snowplow Warning: " ~ base_events_this_run ~ " does not exist. This is expected if you are compiling a fresh installation of the dbt-snowplow-* packages.") %}

    {% set dummy_limit = snowplow_utils.cast_to_tstamp('9999-01-01 00:00:00') %}

    {{ return([dummy_limit, dummy_limit, dummy_limit]) }}

  {% endif %}

{%- endmacro %}
