{# post-hook for incremental runs #}
{% macro snowplow_incremental_post_hook(package_name) %}
  
  {% set enabled_snowplow_models = snowplow_utils.get_enabled_snowplow_models(package_name) -%}

  {% set successful_snowplow_models = snowplow_utils.get_successful_models(models=enabled_snowplow_models) -%}

  {% set incremental_manifest_table = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}

  {% set base_events_this_run_table = ref(package_name~'_base_events_this_run') -%}
        
  {{ snowplow_utils.update_incremental_manifest_table(incremental_manifest_table, base_events_this_run_table, successful_snowplow_models) }}                  

{% endmacro %}
