{# When called by an incremental model, determines whether the model has previously consumed the data within the run
   If it has, returns false. This blocks the model from updating with old data #}
{% macro is_run_with_new_events(package_name) %}

  {%- set new_event_limits_relation = snowplow_utils.get_new_event_limits_table_relation(package_name) -%}
  {%- set incremental_manifest_relation = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}

  {% if snowplow_utils.snowplow_is_incremental() %}

    {%- set node_identifier = this.identifier -%}
    {%- set base_sessions_lifecycle_identifier = package_name+'_base_sessions_lifecycle_manifest' -%}

    {# base_sessions_lifecycle not included in manifest so query directly. Otherwise use the manifest for performance #}
    {%- if node_identifier == base_sessions_lifecycle_identifier -%}
      {#Technically should be max(end_tstsamp) but table is partitioned on start_tstamp so cheaper to use.
        Worst case we update the manifest during a backfill when we dont need to, which should be v rare. #}
      {% set has_been_processed_query %}
        select 
          case when 
            (select upper_limit from {{ new_event_limits_relation }}) <= (select max(start_tstamp) from {{this}}) 
          then false 
        else true end
      {% endset %}

    {%- else -%}

      {% set has_been_processed_query %}
        select 
          case when 
            (select upper_limit from {{ new_event_limits_relation }}) 
            <= (select last_success from {{ incremental_manifest_relation }} where model = '{{node_identifier}}') 
          then false 
        else true end
      {% endset %}

    {%- endif -%}

    {% set results = run_query(has_been_processed_query) %}

    {% if execute %}
      {% set has_new_events = results.columns[0].values()[0] | as_bool() %}
      {# Snowflake: dbt 0.18 returns bools as ints. Ints are not accepted as predicates in Snowflake. Cast to be safe. #}
      {% set has_new_events = 'cast('~has_new_events~' as boolean)' %}
    {% endif %}

  {% else %}

    {% set has_new_events = true %}

  {% endif %}

  {{ return(has_new_events) }}

{% endmacro %}
