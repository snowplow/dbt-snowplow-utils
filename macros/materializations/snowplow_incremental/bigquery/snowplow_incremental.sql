
{% macro snowplow_bigquery_validate_get_incremental_strategy(config) %}
  {# Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="restricted_merge") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'restricted_merge'
  {%- endset %}
  {% if strategy not in ['merge', 'restricted_merge'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}


{% macro get_snowplow_merge_sql(target, source, unique_key, dest_columns, predicates, include_sql_header) -%}
  {{ adapter.dispatch('get_snowplow_merge_sql', ['snowplow_utils'])(target, source, unique_key, dest_columns, predicates, include_sql_header) }}
{%- endmacro %}

{% macro default__get_snowplow_merge_sql(target, source, unique_key, dest_columns, predicates, include_sql_header) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
    {% else %}
        {% set unique_key_match %}
            false
        {% endset %}
    {% endif %}

    {{ sql_header if sql_header is not none and include_sql_header }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ unique_key_match }}
        {% if predicates %} and {{ predicates | join(' and ') }} {% endif %}

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}


{% macro bq_restricted_merge(tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, disable_upsert_lookback) %}

    {% set predicate -%}
        DBT_INTERNAL_DEST.{{ partition_by.field }} between dbt_partition_lower_limit and dbt_partition_upper_limit
    {%- endset %}

    {%- set source_sql -%}
    (
      select * from {{ tmp_relation }}
    )
    {%- endset -%}

    -- generated script to merge partitions into {{ target_relation }}
    declare dbt_partition_lower_limit, dbt_partition_upper_limit {{ partition_by.data_type }};
    -- 1. create a temp table
    {{ create_table_as(True, tmp_relation, sql) }}
    -- 2. define partitions to update
    {% if disable_upsert_lookback %}
      set (dbt_partition_lower_limit, dbt_partition_upper_limit) = (
            select as struct
                   min({{ partition_by.field }}) as lower_limit,
                   max({{ partition_by.field }}) as upper_limit
            from {{ tmp_relation }}
      );
    {% else %}
      set (dbt_partition_lower_limit, dbt_partition_upper_limit) = (
            select as struct
                   cast({{ dbt_utils.dateadd('day', -var("snowplow__upsert_lookback_days", 30), 'min('~partition_by.field~')') }} 
                        as {{ partition_by.data_type }}) as lower_limit,
                   max({{ partition_by.field }}) as upper_limit
            from {{ tmp_relation }}
      );
    {% endif %}

    {#
      TODO: include_sql_header is a hack; consider a better approach that includes
            the sql_header at the materialization-level instead
    #}
    -- 3. run the merge statement
    {{ snowplow_utils.get_snowplow_merge_sql(target_relation, source_sql, unique_key, dest_columns, [predicate], include_sql_header=false) }};

    -- 4. clean up the temp table
    drop table if exists {{ tmp_relation }}


{% endmacro %}


{% materialization snowplow_incremental, adapter='bigquery' -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {% set disable_upsert_lookback = config.get('disable_upsert_lookback') %}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}
  {%- set tmp_relation = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = snowplow_utils.snowplow_bigquery_validate_get_incremental_strategy(config) -%}

  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
  {%- set partitions = config.get('partitions', none) -%}
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
     {#-- default restricted merge requires partition. Ignore if using standard merge strategy --#}
     {% if strategy == 'merge' %}

        {#-- wrap sql in parens to make it a subquery --#}
        {%- set source_sql -%}
          (
            {{sql}}
          )
        {%- endset -%}

        {% set build_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns) %}

     {% else %}

        {% set missing_partition_msg -%}
          The 'snowplow_incremental' materialization requires the `partition_by` config.
        {%- endset %}
        {% if partition_by is none %}
          {% do exceptions.raise_compiler_error(missing_partition_msg) %}
        {% endif %}

        {% set build_sql = snowplow_utils.bq_restricted_merge(
            tmp_relation,
            target_relation,
            sql,
            unique_key,
            partition_by,
            partitions,
            dest_columns,
            disable_upsert_lookback) %}
       
     {% endif %}

  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = this.incorporate(type='table') %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
