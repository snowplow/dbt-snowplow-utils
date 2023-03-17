{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro snowplow_delete_insert(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) -%}
  {{ adapter.dispatch('snowplow_delete_insert', 'snowplow_utils')(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) }}
{%- endmacro %}


{% macro default__snowplow_delete_insert(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) %}

  {% set predicate -%}
    {{ upsert_date_key }} between (select lower_limit from vars) and (select upper_limit from vars)
  {%- endset %}

  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  -- define upsert limits
  {{ snowplow_utils.get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) }}

  -- run the delete+insert statement
  {{ snowplow_utils.get_snowplow_delete_insert_sql(target_relation, tmp_relation, unique_key, dest_cols_csv, [predicate]) }}

{%- endmacro %}


{% macro snowflake__snowplow_delete_insert(tmp_relation, target_relation, unique_key, upsert_date_key, dest_columns, disable_upsert_lookback) %}

  {% set predicate -%}
      {{ upsert_date_key }} between $dbt_partition_lower_limit and $dbt_partition_upper_limit
  {%- endset %}

  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

  {%- set source_sql -%}
  (
    select * from {{ tmp_relation }}
  )
  {%- endset -%}

  -- define upsert limits
  {{ snowplow_utils.get_snowplow_upsert_limits_sql(tmp_relation, upsert_date_key, disable_upsert_lookback) }}

  -- run the delete+insert statement
  {{ snowplow_utils.get_snowplow_delete_insert_sql(target_relation, source_sql, unique_key, dest_cols_csv, [predicate]) }}

  -- Unset variables
  unset (dbt_partition_lower_limit, dbt_partition_upper_limit);

{% endmacro %}
