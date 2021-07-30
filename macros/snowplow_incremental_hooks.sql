{% macro get_snowplow_manifest_schema() %}
  {# Derive full schema name from generate_schema_name macro in root project if exists #}
  {% if context.get(project_name, {}).get('generate_schema_name') %}
    {% set schema_name = context[project_name].generate_schema_name(var("snowplow__manifest_custom_schema","snowplow_manifest")) %}
    {{ return(schema_name) }}
  {% else %}
    {% set schema_name = generate_schema_name(var("snowplow__manifest_custom_schema","snowplow_manifest")) %}
    {{ return(schema_name) }}
  {% endif %}

{% endmacro %}


{# Creates snowplow manifest schema. This should be automatically handled by dbt but adding for peace of mind #}
{% macro create_snowplow_manifest_schema() %}
  
  {%- set manifest_schema=snowplow_utils.get_snowplow_manifest_schema() -%}

  {% do adapter.create_schema(api.Relation.create(database=target.database, schema=manifest_schema)) %}
  
{% endmacro %}


{# Returns the incremental manifest table reference. This table contains 1 row/model with the latest tstamp consumed #}
{% macro get_incremental_manifest_table_relation(package_name) %}

  {%- set manifest_schema=snowplow_utils.get_snowplow_manifest_schema() -%}

  {%- set incremental_manifest_table =
    api.Relation.create(
        database=target.database,
        schema=manifest_schema,
        identifier=package_name+'_incremental_manifest',
        type='table'
  ) -%}

  {{ return(incremental_manifest_table) }}

{% endmacro %}

{# Returns the current incremental table reference. This table contains lower and upper tstamp limits of the current run #}
{% macro get_current_incremental_tstamp_table_relation(package_name) %}

  {%- set manifest_schema=snowplow_utils.get_snowplow_manifest_schema() -%}

  {%- set current_incremental_tstamp_table =
    api.Relation.create(
        database=target.database,
        schema=manifest_schema,
        identifier=package_name+'_current_incremental_tstamp',
        type='table'
  ) -%}

  {{ return(current_incremental_tstamp_table) }}

{% endmacro %}

{# Creates or mutates incremental manifest table #}
{% macro create_incremental_manifest_table(package_name) -%}

  {{ return(adapter.dispatch('create_incremental_manifest_table', ['snowplow_utils'])(package_name)) }}

{% endmacro %}


{% macro default__create_incremental_manifest_table(package_name) -%}

  {% set required_columns = [
     ["model", dbt_utils.type_string()],
     ["last_success", dbt_utils.type_timestamp()],
  ] -%}

  {% set incremental_manifest_table = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}

  {% set incremental_manifest_table_exists = adapter.get_relation(incremental_manifest_table.database,
                                                                  incremental_manifest_table.schema,
                                                                  incremental_manifest_table.name) -%}

  {% if incremental_manifest_table_exists -%}

    {%- set columns_to_create = [] -%}

    {# map to lower to cater for snowflake returning column names as upper case #}
    {%- set existing_columns = adapter.get_columns_in_relation(incremental_manifest_table)|map(attribute='column')|map('lower')|list -%}

    {%- for required_column in required_columns -%}
      {%- if required_column[0] not in existing_columns -%}
        {%- do columns_to_create.append(required_column) -%}
      {%- endif -%}
    {%- endfor -%}

    {%- for column in columns_to_create -%}
      alter table {{ incremental_manifest_table }}
      add column {{ column[0] }} {{ column[1] }}
      default null;
    {% endfor -%}

    {%- if columns_to_create|length > 0 %}
      commit;
    {% endif -%}

  {%- else -%}

    create table if not exists {{ incremental_manifest_table }}
    (
    {% for column in required_columns %}
        {{ column[0] }} {{ column[1] }}{% if not loop.last %},{% endif %}
    {% endfor %}
    );
    commit;

  {%- endif -%}

{%- endmacro %}

{# Returns the sql to calculate the lower/upper limits of the run #}
{% macro get_run_limits(min_last_success, max_last_success, models_matched_from_manifest, has_matched_all_models, start_date) -%}

  {% set start_tstamp = snowplow_utils.cast_to_tstamp(start_date) %}
  {% set min_last_success = snowplow_utils.cast_to_tstamp(min_last_success) %}
  {% set max_last_success = snowplow_utils.cast_to_tstamp(max_last_success) %}

  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {% if models_matched_from_manifest == 0 %}
    {# If no snowplow models are in the manifest, start from start_tstamp #}
    {% do snowplow_utils.log_message("Snowplow: No data in manifest. Processing data from start_date") %}

    {% set run_limits_query %}
      select {{start_tstamp}} as lower_limit,
             least({{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), start_tstamp) }},
                   {{ dbt_utils.current_timestamp_in_utc() }}) as upper_limit
    {% endset %}

  {% elif not has_matched_all_models %}
    {# If a new Snowplow model is added which isn't already in the manifest, replay all events up to upper_limit #}
    {% do snowplow_utils.log_message("Snowplow: New Snowplow incremental model. Backfilling") %}

    {% set run_limits_query %}
      select {{ start_tstamp }} as lower_limit,
             least({{ max_last_success }},
                   {{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), start_tstamp) }}) as upper_limit
    {% endset %}

  {% elif min_last_success != max_last_success %}
    {# If all models in the run exists in the manifest but are out of sync, replay from the min last success to the max last success #}
    {% do snowplow_utils.log_message("Snowplow: Snowplow incremental models out of sync. Syncing") %}

    {% set run_limits_query %}
      select {{ snowplow_utils.timestamp_add('hour', -var("snowplow__lookback_window_hours", 6), min_last_success) }} as lower_limit,
             least({{ max_last_success }},
                  {{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), min_last_success) }}) as upper_limit
    {% endset %}

  {% else %}
    {# Else standard run of the model #}
    {% do snowplow_utils.log_message("Snowplow: Standard incremental run") %}

    {% set run_limits_query %}
      select 
        {{ snowplow_utils.timestamp_add('hour', -var("snowplow__lookback_window_hours", 6), min_last_success) }} as lower_limit,
        least({{ snowplow_utils.timestamp_add('day', var("snowplow__backfill_limit_days", 30), min_last_success) }}, 
              {{ dbt_utils.current_timestamp_in_utc() }}) as upper_limit
    {% endset %}

  {% endif %}

  {{ return(run_limits_query) }}
    
{% endmacro %}


{% macro get_incremental_manifest_status(incremental_manifest_table, models_in_run) -%}

  {% if not execute %}

    {{ return(['', '', '', '']) }}

  {% endif %}

  {% set incremental_manifest_table_exists = adapter.get_relation(incremental_manifest_table.database,
                                                                  incremental_manifest_table.schema,
                                                                  incremental_manifest_table.name) -%}

  {% if incremental_manifest_table_exists -%}

    {% set last_success_query %}
      select min(last_success) as min_last_success,
             max(last_success) as max_last_success,
             coalesce(count(*), 0) as models
      from {{ incremental_manifest_table }}
      where model in ({{ snowplow_utils.print_list(models_in_run) }})
    {% endset %}

  {% elif not incremental_manifest_table_exists %}
    
    {% set last_success_query %}
      select cast(null as {{ dbt_utils.type_timestamp() }}) as min_last_success,
             cast(null as {{ dbt_utils.type_timestamp() }}) as max_last_success,
             0 as models
    {% endset %} 

  {% endif %}

  {% set results = run_query(last_success_query) %}

  {% if execute %}

    {% set min_last_success = results.columns[0].values()[0] %}
    {% set max_last_success = results.columns[1].values()[0] %}
    {% set models_matched_from_manifest = results.columns[2].values()[0] %}
    {% set has_matched_all_models = true if models_matched_from_manifest == models_in_run|length else false %}

  {% endif %}

  {{ return([min_last_success, max_last_success, models_matched_from_manifest, has_matched_all_models]) }}

{%- endmacro %}

{# Prints the run limits for the run to the console #}
{% macro print_run_limits(run_limits_query) -%}

  {# Derive limits from manifest instead of selecting from limits table since run_query executes during 2nd parse the limits table is yet to be updated. #}
  {% set results = run_query(run_limits_query) %}

  {% if execute %}

    {% set lower_limit = snowplow_utils.tstamp_to_str(results.columns[0].values()[0]) %}
    {% set upper_limit = snowplow_utils.tstamp_to_str(results.columns[1].values()[0]) %}
    {% set run_limits_message = "Snowplow: Processing data between " + lower_limit + " and " + upper_limit %}

    {% do snowplow_utils.log_message(run_limits_message) %}

  {% endif %}

{%- endmacro %}


{# Returns an array of enabled models tagged with snowplow_web_incremental using dbts graph object. 
   Throws an error if untagged models are found that depend on the base_events_this_run model#}
{% macro get_enabled_snowplow_models(package_name, graph_object=none, models_to_run=var("models_to_run","")) -%}
  
  {# Override dbt graph object if graph_object is passed. Testing purposes #}
  {% if graph_object is not none %}
    {% set graph = graph_object %}
  {% endif %}
  
  {# models_to_run optionally passed using dbt ls command. This returns a string of models to be run. Split into list #}
  {% if models_to_run|length %}
    {% set selected_models = models_to_run.split(" ") %}
  {% else %}
    {% set selected_models = none %}
  {% endif %}

  {% set enabled_models = [] %}
  {% set untagged_snowplow_models = [] %}
  {% set snowplow_model_tag = package_name+'_incremental' %}
  {% set snowplow_events_this_run_path = 'model.'+package_name+'.'+package_name+'_base_events_this_run' %}

  {% if execute %}
    
    {% set nodes = graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
    
    {% for node in nodes %}
      {# If selected_models is specified, filter for these models #}
      {% if selected_models is none or node.name in selected_models %}

        {% if node.config.enabled and snowplow_model_tag not in node.tags and snowplow_events_this_run_path in node.depends_on.nodes %}

          {%- do untagged_snowplow_models.append(node.name) -%}

        {% endif %}

        {% if node.config.enabled and snowplow_model_tag in node.tags %}

          {%- do enabled_models.append(node.name) -%}

        {% endif %}

      {% endif %}
      
    {% endfor %}

    {% if untagged_snowplow_models|length %}
    {#
      Prints warning for models that reference snowplow_base_events_this_run but are untagged as 'snowplow_web_incremental'
      Without this tagging these models will not be inserted into the manifest, breaking the incremental logic.
      Only catches first degree dependencies rather than all downstream models
    #}
      {%- do exceptions.raise_compiler_error("Snowplow Warning: Untagged models referencing '"+package_name+"_base_events_this_run'. Please refer to the Snowplow docs on tagging. " 
      + "Models: "+ ', '.join(untagged_snowplow_models)) -%}
    
    {% endif %}

  {% endif %}

  {{ return(enabled_models) }}

{%- endmacro %}

{# Returns an array of successfully executed models tagged with snowplow_web_incremental #}
{% macro get_successful_snowplow_models(package_name) -%}

  {% set enabled_snowplow_models = snowplow_utils.get_enabled_snowplow_models(package_name) -%}

  {% set successful_snowplow_models = [] %}

  {% if execute %}

    {% for res in results -%}
    
      {% if res.status == 'success' and res.node.name in enabled_snowplow_models  %}
    
        {%- do successful_snowplow_models.append(res.node.name) -%}

      {% endif %}
      
    {% endfor %}

    {{ return(successful_snowplow_models) }}

  {% endif %}

{%- endmacro %}

{# Updates the incremental manifest table at the run end with the latest tstamp consumed per model #}
{% macro update_incremental_manifest_table(manifest_table, base_events_table, models) -%}

  {{ return(adapter.dispatch('update_incremental_manifest_table', ['snowplow_utils'])(manifest_table, base_events_table, models)) }}

{% endmacro %}

{% macro default__update_incremental_manifest_table(manifest_table, base_events_table, models) -%}

  {% if models %}

    {% set last_success_query %}
      select 
        model, 
        last_success 

      from 
        (select max(collector_tstamp) as last_success from {{ base_events_table }}),
        ({% for model in models %} select '{{model}}' as model {%- if not loop.last %} union all {% endif %} {% endfor %})

      where last_success is not null -- if run contains no data don't add to manifest
    {% endset %}

    merge {{ manifest_table }} m
    using ( {{ last_success_query }} ) s
    on m.model = s.model
    when matched then
        update set last_success = greatest(m.last_success, s.last_success)
    when not matched then
        insert (model, last_success) values(model, last_success)

  {% endif %}

{%- endmacro %}


{% macro redshift__update_incremental_manifest_table(manifest_table, base_events_table, models) -%}

  {% if models %}

    begin transaction;
      --temp table to find the greatest last_success per model.
      --this protects against partial backfills causing the last_success to move back in time.
      create temporary table snowplow_models_last_success as (
        select
          a.model,
          greatest(a.last_success, b.last_success) as last_success

        from (

          select 
            model, 
            last_success 

          from 
            (select max(collector_tstamp) as last_success from {{ base_events_table }}),
            ({% for model in models %} select '{{model}}' as model {%- if not loop.last %} union all {% endif %} {% endfor %})

          where last_success is not null -- if run contains no data don't add to manifest

        ) a
        left join {{ manifest_table }} b
        on a.model = b.model
        );

      delete from {{ manifest_table }} where model in (select model from snowplow_models_last_success);
      insert into {{ manifest_table }} (select * from snowplow_models_last_success);

    end transaction;

    drop table snowplow_models_last_success;
  {% endif %}

{%- endmacro %}

{# Calls functions to teardown all snowplow manifest tables or remove models from the manifest. Executes on-run-start  #}
{% macro snowplow_run_start_cleanup(package_name, teardown_all, models_to_remove) %}
  
  {%- if teardown_all -%}
    {%- do snowplow_utils.snowplow_teardown_all(package_name) -%}
  {%- endif -%}

  {%- if models_to_remove|length -%}
    {%- do snowplow_utils.snowplow_delete_from_manifest(package_name, models_to_remove) -%}
  {%- endif -%}

{% endmacro %}

{# pre-hook for incremental runs #}
{% macro snowplow_incremental_pre_hook(package_name) %}

  {{ snowplow_utils.create_snowplow_manifest_schema() }}

  {{ snowplow_utils.create_incremental_manifest_table(package_name) }}

  {%- set models_in_run = snowplow_utils.get_enabled_snowplow_models(package_name) -%}

  {% set incremental_tstamp_table = snowplow_utils.get_current_incremental_tstamp_table_relation(package_name) -%}

  {% set incremental_manifest_table = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}

  {% set min_last_success,
         max_last_success, 
         models_matched_from_manifest,
         has_matched_all_models = snowplow_utils.get_incremental_manifest_status(incremental_manifest_table, models_in_run) -%}

  {% set run_limits_query = snowplow_utils.get_run_limits(min_last_success, 
                                                          max_last_success,
                                                          models_matched_from_manifest,
                                                          has_matched_all_models,
                                                          var("snowplow__start_date","2020-01-01")) -%}

  {{ snowplow_utils.create_table_as_sql(incremental_tstamp_table, run_limits_query, replace=true) }}

  {{ snowplow_utils.print_run_limits(run_limits_query) }}

{% endmacro %}

{# post-hook for incremental runs #}
{% macro snowplow_incremental_post_hook(package_name) %}

  {% set successful_snowplow_models = snowplow_utils.get_successful_snowplow_models(package_name) -%}

  {% set incremental_manifest_table = snowplow_utils.get_incremental_manifest_table_relation(package_name) -%}

  {% set base_events_this_run_table = ref(package_name~'_base_events_this_run') -%}
        
  {{ snowplow_utils.update_incremental_manifest_table(incremental_manifest_table, base_events_this_run_table, successful_snowplow_models) }}                  

{% endmacro %}
