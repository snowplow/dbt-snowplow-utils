{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Deletes specified models from the incremental_manifest table #}
{% macro snowplow_delete_from_manifest(models, incremental_manifest_table) %}

  {# Ensure models is a list #}
  {%- if models is string -%}
    {%- set models = [models] -%}
  {%- endif -%}

  {# No models to delete or not in execute mode #}
  {% if not models|length or not execute %}
    {{ return('') }}
  {% endif %}

  {# Get the manifest table to ensure it exits #}
  {%- set incremental_manifest_table_exists = adapter.get_relation(incremental_manifest_table.database,
                                                                  incremental_manifest_table.schema,
                                                                  incremental_manifest_table.name) -%}

  {%- if not incremental_manifest_table_exists -%}
    {{return(dbt_utils.log_info("Snowplow: "+incremental_manifest_table|string+" does not exist"))}}
  {%- endif -%}

  {# Get all models in the manifest and compare to list of models to delete #}
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
    {%- if target.type in ['databricks'] -%}
      DELETE FROM {{ incremental_manifest_table }}
      WHERE model IN ({{ snowplow_utils.print_list(matched_models) }});
    {%- elif target.type in ['spark'] -%}
      DELETE FROM {{ incremental_manifest_table }}
      WHERE model IN ({{ snowplow_utils.print_list(matched_models) }});
    {%- else -%}
      -- We don't need transaction but Redshift needs commit statement while BQ does not. By using transaction we cover both.
      BEGIN;
      DELETE FROM {{ incremental_manifest_table }} 
      WHERE model IN ({{ snowplow_utils.print_list(matched_models) }});
      COMMIT;
    {%- endif -%}

  {% endset %}
  {%- do adapter.execute(delete_statement) -%}

  {%- if matched_models|length -%}
    {% do snowplow_utils.log_message("Snowplow: Deleted models "+snowplow_utils.print_list(matched_models)+" from the manifest") %}
  {%- endif -%}

  {%- if unmatched_models|length -%}
    {% do snowplow_utils.log_message("Snowplow: Models "+snowplow_utils.print_list(unmatched_models)+" do not exist in the manifest") %}
  {%- endif -%}

{% endmacro %}

{# Package specific macro. Makes the API less cumbersome for the user #}
{% macro snowplow_web_delete_from_manifest(models) %}

  {{ snowplow_utils.snowplow_delete_from_manifest(models, ref('snowplow_web_incremental_manifest')) }}

{% endmacro %}

{% macro snowplow_mobile_delete_from_manifest(models) %}

  {{ snowplow_utils.snowplow_delete_from_manifest(models, ref('snowplow_mobile_incremental_manifest')) }}

{% endmacro %}

{% macro snowplow_base_delete_from_manifest(models, incremental_manifest='snowplow_incremental_manifest') %}

  {{ snowplow_utils.snowplow_delete_from_manifest(models, ref(incremental_manifest)) }}

{% endmacro %}
