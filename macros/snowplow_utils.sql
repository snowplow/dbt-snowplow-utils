{# Prints array as comma seperated, quoted items. #}
{% macro print_list(list) %}

  {%- for item in list %} '{{item}}' {%- if not loop.last %},{% endif %} {% endfor -%}

{% endmacro %}

{# Filters on app_id if provided #}
{% macro app_id_filter(app_ids) %}

  {%- if app_ids|length -%} 

    app_id in ('{{ app_ids|join("','") }}') --filter on app_id if provided

  {%- else -%}

    true

  {%- endif -%}

{% endmacro %}

{# Logs to console, with option to disable. #}
{% macro log_message(message, is_printed=var('snowplow__has_log_enabled', true)) %}
    {{ return(adapter.dispatch('log_message', ['snowplow_utils'])(message, is_printed)) }}
{% endmacro %}

{% macro default__log_message(message, is_printed) %}
    {{ log(dbt_utils.pretty_log_format(message), info=is_printed) }}
{% endmacro %}

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

{# Deletes specified models from the incremental_manifest table #}
{% macro snowplow_delete_from_manifest(package_name, models, incremental_manifest_table=none) %}
  
  /* Depends on: {{ snowplow_utils.get_incremental_manifest_table_relation(package_name) }} */

  {%- if models is string -%}
    {%- set models = [models] -%}
  {%- endif -%}

  {% if not models|length or not execute %}
    {{ return('') }}
  {% endif %}

  {# incremental_manifest_table karg allows for testing #}
  {% if incremental_manifest_table is none %}
    {%- set incremental_manifest_table = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}
  {% endif %}

  {%- set incremental_manifest_table_exists = adapter.get_relation(incremental_manifest_table.database,
                                                                  incremental_manifest_table.schema,
                                                                  incremental_manifest_table.name) -%}

  {%- if not incremental_manifest_table_exists -%}
    {{return(dbt_utils.log_info("Snowplow: "+incremental_manifest_table|string+" does not exist"))}}
  {%- endif -%}

  {%- set models_in_manifest = dbt_utils.get_column_values(table=incremental_manifest_table, column='model') -%}
  {%- set unmatched_models, matched_models = [], [] -%}

  {%- for model in models -%}

    {%- if model in models_in_manifest -%}
      {%- do matched_models.append(model) -%}
    {%- else -%}
      {%- do unmatched_models.append(model) -%}
    {%- endif -%}

  {%- endfor -%}

  {%- if not matched_models|length -%}
    {{return(dbt_utils.log_info("Snowplow: None of the supplied models exist in the manifest"))}}
  {%- endif -%}

  {% set delete_statement %}
    -- We don't need transaction but Redshift needs commit statement while BQ does not. By using transaction we cover both.
    begin;
    delete from {{ incremental_manifest_table }} where model in ({{ snowplow_utils.print_list(matched_models) }});
    commit;
  {% endset %}

  {%- if matched_models|length -%}
    {% do snowplow_utils.log_message("Snowplow: Deleted models "+snowplow_utils.print_list(matched_models)+" from the manifest") %}
  {%- endif -%}

  {%- if unmatched_models|length -%}
    {% do snowplow_utils.log_message("Snowplow: Models "+snowplow_utils.print_list(unmatched_models)+" do not exist in the manifest") %}
  {%- endif -%}

  {{ return(delete_statement) }}

{% endmacro %}


{% macro tstamp_to_str(tstamp) -%}
  '{{ tstamp.strftime("%Y-%m-%d %H:%M:%S") }}'
{%- endmacro %}


{% macro return_base_new_event_limits(base_events_this_run) -%}

  {% if not execute %}
    {{ return(['','','',''])}}
  {% endif %}
  
  {% set limit_query %} 
    select 
      lower_limit, 
      upper_limit,
      {{ snowplow_utils.timestamp_add('day', 
                                     -var("snowplow__session_lookback_days", 365),
                                     'lower_limit') }} as session_lookback_limit,
      {{ snowplow_utils.timestamp_add('day', 
                                     -var("snowplow__max_session_days", 3),
                                     'lower_limit') }} as lower_limit_minus_max_session_days

    from {{ base_events_this_run }} 
    {% endset %}

  {% set results = run_query(limit_query) %}
   
  {% if execute %}

    {% set lower_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.cast_to_tstamp(results.columns[1].values()[0]) %}
    {% set session_lookback_limit = snowplow_utils.cast_to_tstamp(results.columns[2].values()[0]) %}
    {% set lower_limit_minus_max_session_days = snowplow_utils.cast_to_tstamp(results.columns[3].values()[0]) %}

  {{ return([lower_limit, upper_limit, session_lookback_limit, lower_limit_minus_max_session_days]) }}

  {% endif %}
{%- endmacro %}


{% macro return_limits_from_model(model, lower_limit_col, upper_limit_col) -%}

  {% if not execute %}
    {{ return(['','']) }}
  {% endif %}
  
  {% set limit_query %} 
    select 
      min({{lower_limit_col}}) as lower_limit,
      max({{upper_limit_col}}) as upper_limit
    from {{ model }} 
    {% endset %}

  {% set results = run_query(limit_query) %}
   
  {% if execute %}

    {% set lower_limit = snowplow_utils.cast_to_tstamp(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.cast_to_tstamp(results.columns[1].values()[0]) %}

  {{ return([lower_limit, upper_limit]) }}

  {% endif %}
{%- endmacro %}
