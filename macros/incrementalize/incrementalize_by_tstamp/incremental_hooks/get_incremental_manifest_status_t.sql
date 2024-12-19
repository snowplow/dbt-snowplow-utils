{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_incremental_manifest_status_t(incremental_manifest_table, models_in_run) -%}

{/* Returns an array of: 
  - 4 timestamp strings: min/max first/last processed timestamps from the manifest (all of these criteria are evaluated based on the models in the current run)
  - 2 integers: the number of models in the manifest, and the number of distinct last processed timestamps for syncing
  - a boolean: to show if the models tagged in the current run match with this number or not
  indicating syncing is required */} 

  {# In case of not execute just return empty strings to avoid hitting database #}
  {% if not execute %}
    {{ return(['', '', '', '', '', '', '']) }}
  {% endif %}

  {% set target_relation = adapter.get_relation(
        database=incremental_manifest_table.database,
        schema=incremental_manifest_table.schema,
        identifier=incremental_manifest_table.name) %}

  {% if target_relation is not none %}

    {% set status_query %}
      select
        min(first_success) as min_first_success,
        max(first_success) as max_first_success,
        min(last_success) as min_last_success,
        max(last_success) as max_last_success,
        coalesce(count(*), 0) as models,
        count(distinct last_success) as sync_count
      from {{ incremental_manifest_table }}
      where model in ({{ snowplow_utils.print_list(models_in_run) }})
    {% endset %}

    {% set results = run_query(status_query) %}

    {% if execute %}

      {% set min_first_success = results.columns[0].values()[0] %}
      {% set max_first_success = results.columns[1].values()[0] %}
      {% set min_last_success = results.columns[2].values()[0] %}
      {% set max_last_success = results.columns[3].values()[0] %}
      {% set models_matched_from_manifest = results.columns[4].values()[0] %}
      {% set sync_count = results.columns[5].values()[0] %}
      {% set has_matched_all_models = true if models_matched_from_manifest == models_in_run|length else false %}

      {{ return([min_first_success, 
                max_first_success, 
                min_last_success, 
                max_last_success, 
                models_matched_from_manifest, 
                sync_count,
                has_matched_all_models]) }}

    {% endif %}

  {% else %}

    {% do exceptions.warn("Snowplow Warning: " ~ incremental_manifest_table ~ " does not exist. This is expected if you are compiling a fresh installation of the dbt-snowplow-* packages.") %}

    {{ return(['9999-01-01 00:00:00', '9999-01-01 00:00:00', '9999-01-01 00:00:00', '9999-01-01 00:00:00', 0, 0, false]) }}

  {% endif %}


{%- endmacro %}

