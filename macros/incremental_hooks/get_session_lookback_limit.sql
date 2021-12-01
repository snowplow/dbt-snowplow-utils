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
