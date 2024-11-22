{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# post-hook for incremental runs #}
{% macro snowplow_incremental_post_hook(package_name='snowplow', incremental_manifest_table_name=none, base_events_this_run_table_name=none, session_timestamp=var('snowplow__session_timestamp', 'load_tstamp')) %}

  {% set enabled_snowplow_models = snowplow_utils.get_enabled_snowplow_models(package_name) -%}

  {% set successful_snowplow_models = snowplow_utils.get_successful_models(models=enabled_snowplow_models) -%}

  {%- if incremental_manifest_table_name -%}
    {%- set incremental_manifest_table = ref(incremental_manifest_table_name) -%}
  {%- else -%}
    {% set incremental_manifest_table = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}
  {%- endif -%}

  {%- if base_events_this_run_table_name -%}
    {%- set base_events_this_run_table = ref(base_events_this_run_table_name) -%}
  {%- else -%}
    {% set base_events_this_run_table = ref(package_name~'_base_events_this_run') -%}
  {%- endif -%}

  {{ snowplow_utils.update_incremental_manifest_table(incremental_manifest_table, base_events_this_run_table, successful_snowplow_models, session_timestamp) }}

{% endmacro %}
