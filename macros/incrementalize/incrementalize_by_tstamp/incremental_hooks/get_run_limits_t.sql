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

  
  {# State 1: If no snowplow models are in the manifest, start from start_tstamp #}
  
  {% if models_matched_from_manifest == 0 %}
    {% do snowplow_utils.log_message("Snowplow: No data in manifest. Processing data from start_date") %}
    {% set run_limits_query %}
      select {{start_tstamp}} as lower_limit,
              least({{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), start_tstamp) }},
              {{ snowplow_utils.current_timestamp_in_utc() }}) as upper_limit
    {% endset %}
    
  {# State 2: If a new Snowplow model is added which isnt already in the manifest, replay all events up to upper_limit unless syncing is disabled and the intention is to keep the most up-to-date business critical models running #}

  {% elif not has_matched_all_models %}
    {% if not var("snowplow__ignore_sync", false) %}  
      {% if sync_count > 1 %}
        {# Lets deal with this later, for now if multiple levels of unsynced models are present we tell the user to handle it manually #}
        {% if not var("snowplow__testing", false) %}
          {{ exceptions.raise_compiler_error("Snowplow Error: Multiple levels of unsynced models detected. Please handle this manually") }}
        {% else %}
          {% set run_limits_query %}
            select cast('1999-01-01 00:00:00+00:00' as {{ type_timestamp() }}) as lower_limit,
                  cast('1999-01-02 00:00:00+00:00' as {{ type_timestamp() }}) as upper_limit
          {% endset %}
        {% endif %}
      {% else %}
        {% do snowplow_utils.log_message("Snowplow: New Snowplow incremental model. Backfilling") %}
        {% set run_limits_query %}
          select {{ start_tstamp }} as lower_limit,
                  least({{ max_last_success }},
                  {{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), start_tstamp) }}) as upper_limit
        {% endset %}
      {% endif %}
    {% endif %}

  {# State 3: User wants to sync all models but there are more than one level of unsynced models. Lets deal with this later, for now we tell the user to handle it manually #}

  {% elif sync_count > 2 %}
    {% if not var("snowplow__testing", false) %}
      {{ exceptions.raise_compiler_error("Snowplow Error: Multiple levels of unsynced models detected. Please handle this manually") }}
    {% else %}
      {% set run_limits_query %}
        select cast('1999-01-01 00:00:00+00:00' as {{ type_timestamp() }}) as lower_limit,
               cast('1999-01-02 00:00:00+00:00' as {{ type_timestamp() }}) as upper_limit
      {% endset %}
    {% endif %}
    
  {# State 4: If all models in the run exists in the manifest but are out of sync and there are two levels only and the intention is to keep them synced, replay from the min last success to the max last success #}

  {% elif sync_count == 2 %}
    {% do snowplow_utils.log_message("Snowplow: Snowplow incremental models out of sync. Syncing") %}

    {% set run_limits_query %}
      select {{ min_last_success }} as lower_limit,
              least({{ max_last_success }},
              {{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), min_last_success) }}) as upper_limit
    {% endset %}
    
  {# State 5: If all models in the run exists in the manifest, none are out of sync, it is a standard incremental run #}

  {% else %}
    {% do snowplow_utils.log_message("Snowplow: Standard incremental run") %}

    {% if var("snowplow__backfill_limit_days", 30) > 0 %}
      {% set run_limits_query %}
        select
        
          {% if var("snowplow__run_type", "incremental") == 'incremental' %}
            {{ min_last_success }} as lower_limit,
          {% elif var("snowplow__run_type", "incremental") == 'current_day_incremental'%}
            least({{ snowplow_utils.deduct_days_from_current_tstamp_utc(0) }}, {{ min_last_success }}) as lower_limit,
          {% elif var("snowplow__run_type", "incremental") == 'last_n_days_incremental'%}
            least({{ snowplow_utils.deduct_days_from_current_tstamp_utc(var("snowplow__reprocess_days", 1)) }}, {{ min_last_success }}) as lower_limit,
          {% else %}
            {{ exceptions.raise_compiler_error("Snowplow Error: Input for variable snowplow__run_type not recognised. Input must be 'incremental', 'current_day_incremental' or 'last_n_days_incremental''. Input given: " ~ var("snowplow__run_type")) }}
          {% endif %}
          
          least({{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), min_last_success) }},
          {{ snowplow_utils.current_timestamp_in_utc() }}) as upper_limit
      {% endset %}
    {% endif %}

  {% endif %}

  {{ return(run_limits_query) }}

{% endmacro %}
