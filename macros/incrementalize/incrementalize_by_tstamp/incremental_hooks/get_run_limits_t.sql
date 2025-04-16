{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{# Returns the sql to calculate the lower/upper limits of the run #}

{% macro get_run_limits_t(min_first_success,
                        max_first_success,
                        min_last_success,
                        max_last_success,
                        models_matched_from_manifest,
                        sync_count,
                        has_matched_all_models,  
                        start_date) -%}

  {% set start_tstamp = snowplow_utils.cast_to_tstamp(start_date) %}
  {% set min_last_success = snowplow_utils.cast_to_tstamp(min_last_success) %}
  {% set max_last_success = snowplow_utils.cast_to_tstamp(max_last_success) %}

  {% if not execute %}
    {{ return('') }}
  {% endif %}

  
  {# State 1: If no snowplow models are in the manifest, start from start_tstamp, 
  return start_tstamp as lower_limit and for the upper_limit, take the earliest of either the backfill_limit_days + 1 days - 1 second 
  (for a more unfiform return limit value) or the current timestamp in UTC #}
  
  {% if models_matched_from_manifest == 0 %}
  
    {% do snowplow_utils.log_message("Snowplow: No data in manifest. Processing data from start_date") %}
    {% set run_limits_query %}
      select {{ start_tstamp }} as lower_limit,
              least(
                {{ snowplow_utils.timestamp_add('second', -1, snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), dbt.date_trunc('day', start_tstamp))) }},
                {{ snowplow_utils.current_timestamp_in_utc() }}
              ) as upper_limit
    {% endset %}
    
  {# State 2: If all models in the run exists in the manifest but are out of sync, replay events from the min last success 
  to the max last success, the package should be omnipodent, just have to make sure that the skipped event counts are only updated after the daily agg model runs successfully #}

  {% elif sync_count == 2 %}
    {% do snowplow_utils.log_message("Snowplow: Snowplow incremental models out of sync. Syncing") %}

    {% set run_limits_query %}
      select {{ min_last_success }} as lower_limit,
              least(
                {{ max_last_success }},
                {{ snowplow_utils.timestamp_add('second', -1, snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30) + 1, dbt.date_trunc('day', min_last_success))) }}
              ) as upper_limit
    {% endset %}
    
  {# State 3: If all models in the run exists in the manifest, none are out of sync, it is a standard incremental run #}

  {% else %}
    {% do snowplow_utils.log_message("Snowplow: Standard incremental run") %}

      {% set run_limits_query %}
        select
          {{ min_last_success }} as lower_limit,
          least(
            {{ snowplow_utils.timestamp_add('second', -1, snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30) + 1, dbt.date_trunc('day', min_last_success))) }},
            {{ snowplow_utils.current_timestamp_in_utc() }}
          ) as upper_limit
      {% endset %}
  {% endif %}

  {{ return(run_limits_query) }}

{% endmacro %}
