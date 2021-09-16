{% materialization snowplow_incremental, adapter='bigquery' -%}

  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {# Required keys. Throws error if not present #}
  {%- set unique_key = config.require('unique_key') -%}
  {%- set raw_partition_by = config.require('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}

  {# Raise error if dtype is int64. Unsupported. #}
  {% if partition_by.data_type == 'int64' %}
    {%- set wrong_dtype_message -%}
      Datatype int64 is not supported by 'snowplow_incremental'
      Please use one of the following: timestamp | date | datetime
    {%- endset -%}
    {% do exceptions.raise_compiler_error(wrong_dtype_message) %}
  {% endif %}

  {% set disable_upsert_lookback = config.get('disable_upsert_lookback') %}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}
  {%- set tmp_relation = make_temp_relation(this) %}

  {# Validate early so we don't run SQL if the strategy is invalid or missing keys #}
  {% set strategy = snowplow_utils.snowplow_validate_get_incremental_strategy(config) -%}

  {%- set cluster_by = config.get('cluster_by', none) -%}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
      {#-- There's no way to atomically replace a view with a table on BQ --#}
      {{ adapter.drop_relation(existing_relation) }}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {#-- If the partition/cluster config has changed, then we must drop and recreate --#}
      {% if not adapter.is_replaceable(existing_relation, partition_by, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}

      {% set build_sql = snowplow_utils.snowplow_merge(
          tmp_relation,
          target_relation,
          unique_key,
          partition_by,
          dest_columns,
          disable_upsert_lookback) %}

  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = this.incorporate(type='table') %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
