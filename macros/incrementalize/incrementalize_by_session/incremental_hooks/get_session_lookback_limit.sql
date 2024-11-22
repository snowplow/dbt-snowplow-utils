{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_session_lookback_limit(lower_limit) %}

  {% if not execute %}
    {{ return('')}}
  {% endif %}

  {% set limit_query %}
    select
    {{ snowplow_utils.timestamp_add(
                'day',
                -var("snowplow__session_lookback_days", 365),
                lower_limit) }} as session_lookback_limit

  {% endset %}

  {% set results = run_query(limit_query) %}

  {% if execute %}

    {% set session_lookback_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}

  {{ return(session_lookback_limit) }}

  {% endif %}

{% endmacro %}
