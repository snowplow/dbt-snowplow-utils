{% macro snowplow_merge(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) -%}
  {{ adapter.dispatch('snowplow_merge', 'snowplow_utils')(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) }}
{%- endmacro %}


{% macro default__snowplow_merge(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) %}

    {# partition_by supplied as upsert_date_key for BigQuery. Rename for clarity #}
    {%- set partition_by = upsert_date_key -%}

    {% set predicate -%}
        DBT_INTERNAL_DEST.{{ partition_by.field }} between dbt_partition_lower_limit and dbt_partition_upper_limit
    {%- endset %}

    {%- set source_sql -%}
    (
      select * from {{ tmp_relation }}
    )
    {%- endset -%}

    declare dbt_partition_lower_limit, dbt_partition_upper_limit {{ partition_by.data_type }};

    -- 1. create a temp table
    {{ create_table_as(True, tmp_relation, sql) }}

    -- 2. define partitions to update
    {{ snowplow_utils.get_snowplow_upsert_limits_sql(tmp_relation, partition_by, disable_upsert_lookback) }}

    {#
      TODO: include_sql_header is a hack; consider a better approach that includes
            the sql_header at the materialization-level instead
    #}
    -- 3. run the merge statement
    {{ snowplow_utils.get_snowplow_merge_sql(target_relation, source_sql, unique_key, dest_columns, [predicate], include_sql_header=false) }};

    -- 4. clean up the temp table
    drop table if exists {{ tmp_relation }}


{% endmacro %}


{% macro snowflake__snowplow_merge(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) %}

  {% set predicate -%}
      DBT_INTERNAL_DEST.{{ upsert_date_key }} between $dbt_partition_lower_limit and $dbt_partition_upper_limit
  {%- endset %}

  {%- set source_sql -%}
  (
    select * from {{ tmp_relation }}
  )
  {%- endset -%}

  -- define upsert limits
  {{ snowplow_utils.get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) }}

  {#
    TODO: include_sql_header is a hack; consider a better approach that includes
          the sql_header at the materialization-level instead
  #}
  -- run the merge statement
  {{ snowplow_utils.get_snowplow_merge_sql(target_relation, source_sql, unique_key, dest_columns, [predicate], include_sql_header=false) }};

  -- Unset variables
  unset (dbt_partition_lower_limit, dbt_partition_upper_limit);

{% endmacro %}

{% macro databricks__snowplow_merge(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) %}
    {%- set source_sql -%}
    (
      select * from {{ tmp_relation }}
    )
    {%- endset -%}

    -- 1. create a temp table
    {{ create_table_as(True, tmp_relation, sql) }}
    ;


    -- 2. define partitions to update
    {%- call statement('fetch_limits', fetch_result=True) -%}
      {{ snowplow_utils.get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) }}
    {%- endcall -%}

    {%- set lower_limit = load_result('fetch_limits')['data'][0][0] -%}
    {%- set upper_limit = load_result('fetch_limits')['data'][0][1] -%}

    {% set predicate -%}
        DBT_INTERNAL_DEST.{{ upsert_date_key }} between '{{lower_limit}}' and '{{upper_limit}}'
    {%- endset %}


    {#
      TODO: include_sql_header is a hack; consider a better approach that includes
            the sql_header at the materialization-level instead
    #}

    -- 3. run the merge statement
    {%- set merge_query -%}
    {{ snowplow_utils.get_snowplow_merge_sql(target_relation, source_sql, unique_key, dest_columns, [predicate], include_sql_header=false) }};
    {%- endset -%}


    -- 4. clean up the temp table
    {%- set drop_view -%}
    drop view if exists {{ tmp_relation }}
    {%- endset -%}

    {%- do run_query(merge_query) -%}
    {%- do run_query(drop_view) -%}

{% endmacro %}

{%- macro spark__snowplow_merge(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) -%}
    {{ return(snowplow_utils.databricks__snowplow_merge(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback)) }}
{%- endmacro %}
