{% macro nowplow_incremental_post_hook(package_name='snowplow', incremental_manifest_table_name=none, base_events_this_run_table_name=none, session_timestamp=var('snowplow__session_timestamp', 'load_tstamp')) %}

  {% if not var('snowplow__enable_keyhole_backfill',false) %}
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

    {% endif %}
{% endmacro %}
