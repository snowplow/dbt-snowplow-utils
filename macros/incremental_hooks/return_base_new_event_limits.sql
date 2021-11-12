{% macro return_base_new_event_limits(base_events_this_run) -%}

  {% if not execute %}
    {{ return(['','',''])}}
  {% endif %}
  
  {% set limit_query %} 
    select 
      lower_limit, 
      upper_limit,
      {{ snowplow_utils.timestamp_add('day', 
                                     -var("snowplow__max_session_days", 3),
                                     'lower_limit') }} as session_lookback_limit

    from {{ base_events_this_run }} 
    {% endset %}

  {% set results = run_query(limit_query) %}
   
  {% if execute %}

    {% set lower_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.cast_to_tstamp(results.columns[1].values()[0]) %}
    {% set session_lookback_limit = snowplow_utils.cast_to_tstamp(results.columns[2].values()[0]) %}

  {{ return([lower_limit, upper_limit, session_lookback_limit]) }}

  {% endif %}
{%- endmacro %}
